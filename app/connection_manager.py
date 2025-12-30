# app/connection_manager.py
"""WebSocket connection manager for handling user connections and chat messaging."""

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from starlette.websockets import WebSocket

from app.config import settings

logger = logging.getLogger("realtime_service")

# Anonymous user ID as string (user_id in ConnectionManager is always str)
ANON_USER_ID = str(settings.ANONIM_USER)


class ConnectionManager:
    """
    Manages WebSocket connections for all users.

    Features:
    - Per-user connection lists with thread-safe locking
    - Semaphore-based rate limiting for anonymous users
    - Automatic cleanup of disconnected sockets
    - Chat messaging support (DM and group)
    - Presence tracking
    """

    def __init__(self) -> None:
        # user_id -> list of WebSocket connections
        self.active_connections: dict[str, list[WebSocket]] = defaultdict(list)
        # user_id -> asyncio.Lock for per-user operation serialization
        self.user_locks: dict[str, asyncio.Lock] = {}
        # Global lock for creating/removing user_locks entries
        self.global_lock = asyncio.Lock()
        # Semaphore for anonymous user to limit concurrent sends
        self.anon_semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_SENDS)
        # user_id -> last activity timestamp (for presence tracking)
        self.user_presence: dict[str, datetime] = {}

    async def _get_user_lock(self, user_id: str) -> asyncio.Lock:
        """
        Get or create a lock for a specific user.

        Creates the lock under global lock protection to prevent races.
        """
        async with self.global_lock:
            if user_id not in self.user_locks:
                self.user_locks[user_id] = asyncio.Lock()
            return self.user_locks[user_id]

    async def connect(self, user_id: str, websocket: WebSocket) -> None:
        """
        Add a new WebSocket connection for a user.

        Thread-safe operation using per-user locks.
        Updates presence timestamp on connect.
        """
        was_offline = not self.is_user_online(user_id)

        user_lock = await self._get_user_lock(user_id)
        async with user_lock:
            self.active_connections[user_id].append(websocket)

        # Update presence timestamp
        self.update_presence(user_id)

        logger.info(
            f"User {user_id} connected. "
            f"Total connections: {len(self.active_connections[user_id])}"
        )

        # Track if user just came online (for presence broadcast)
        if was_offline:
            logger.info(f"User {user_id} is now online")

    async def disconnect(self, user_id: str, websocket: WebSocket) -> None:
        """
        Remove a WebSocket connection for a user.

        Cleans up user lock when all connections are closed.
        Updates last_seen timestamp when user goes offline.
        """
        should_cleanup = False

        # Get user lock and perform disconnect under lock
        user_lock = await self._get_user_lock(user_id)
        async with user_lock:
            connections = self.active_connections.get(user_id, [])
            if websocket not in connections:
                logger.warning(f"WebSocket not found in connections for user {user_id}.")
                return

            connections.remove(websocket)
            logger.info(
                f"User {user_id} disconnected a connection. "
                f"Remaining connections: {len(connections)}"
            )

            # Mark for cleanup if no connections left
            if not connections:
                del self.active_connections[user_id]
                should_cleanup = True

        # Cleanup lock outside of user_lock to avoid holding lock while deleting it
        if should_cleanup:
            # Update last_seen timestamp when user goes offline
            self.update_presence(user_id)

            async with self.global_lock:
                # Double-check that user still has no connections before removing lock
                if user_id not in self.active_connections and user_id in self.user_locks:
                    del self.user_locks[user_id]

            logger.info(f"User {user_id} is now offline (all connections closed)")

    async def send_message_to_socket(
        self,
        message: dict[str, Any],
        user_id: str,
        websocket: WebSocket
    ) -> None:
        """
        Send a message to a specific WebSocket connection.

        Disconnects the socket on send failure.
        """
        try:
            await websocket.send_json(message)
            logger.debug(f"Sent message to user {user_id}; message details: {message.get('type')}")
        except Exception as e:
            logger.exception(f"Error sending message to user {user_id}: {e}")
            await self.disconnect(user_id, websocket)

    async def send_personal_message(self, message: dict[str, Any], user_id: str) -> None:
        """
        Send a message to all connections of a specific user.

        For anonymous users, uses semaphore to limit concurrent sends
        and prevent overwhelming the system with thousands of connections.
        """
        user_lock = await self._get_user_lock(user_id)
        # Copy connection list under lock to avoid conflicts
        async with user_lock:
            connections = list(self.active_connections.get(user_id, []))

        if not connections:
            logger.debug(f"No active connections for user {user_id} to send message.")
            return

        if user_id == ANON_USER_ID:
            async def sem_task(ws: WebSocket) -> None:
                async with self.anon_semaphore:
                    await self.send_message_to_socket(message, user_id, ws)

            await asyncio.gather(*(sem_task(ws) for ws in connections), return_exceptions=True)
        else:
            await asyncio.gather(
                *(self.send_message_to_socket(message, user_id, ws) for ws in connections),
                return_exceptions=True
            )

    async def send_message_to_subscribers(
        self,
        message: dict[str, Any],
        subscribers: set[str] | list[str]
    ) -> None:
        """
        Send a message to a group of users asynchronously.

        Spawns a background task to avoid blocking the caller.
        """
        asyncio.create_task(self._send_message_to_subscribers_impl(message, subscribers))

    async def _send_message_to_subscribers_impl(
        self,
        message: dict[str, Any],
        subscribers: set[str] | list[str]
    ) -> None:
        """
        Implementation of batch message sending to subscribers.
        """
        if not subscribers:
            return
        tasks = [
            asyncio.create_task(self.send_personal_message(message, user_id))
            for user_id in subscribers
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    # =========================================================================
    # Chat & Presence Methods
    # =========================================================================

    async def send_to_user(self, user_id: str, message: dict[str, Any]) -> bool:
        """
        Send a message to a specific user (all their connections).

        Returns True if user is online and message was sent, False otherwise.
        """
        user_lock = await self._get_user_lock(user_id)
        async with user_lock:
            connections = list(self.active_connections.get(user_id, []))

        if not connections:
            logger.debug(f"User {user_id} is offline, message not delivered")
            return False

        await asyncio.gather(
            *(self.send_message_to_socket(message, user_id, ws) for ws in connections),
            return_exceptions=True
        )
        return True

    async def send_to_users(
        self,
        user_ids: list[str],
        message: dict[str, Any],
        exclude_user: str | None = None
    ) -> dict[str, bool]:
        """
        Send a message to multiple users.

        Args:
            user_ids: List of user IDs to send to
            message: Message to send
            exclude_user: Optional user ID to exclude from sending

        Returns:
            Dict mapping user_id -> delivery success (True if online)
        """
        results: dict[str, bool] = {}
        tasks = []

        for user_id in user_ids:
            if exclude_user and user_id == exclude_user:
                continue
            tasks.append((user_id, self.send_to_user(user_id, message)))

        if tasks:
            send_results = await asyncio.gather(
                *(task for _, task in tasks),
                return_exceptions=True
            )
            for (user_id, _), result in zip(tasks, send_results):
                if isinstance(result, Exception):
                    logger.error(f"Error sending to user {user_id}: {result}")
                    results[user_id] = False
                else:
                    results[user_id] = result

        return results

    async def broadcast_to_group(
        self,
        member_ids: list[str],
        message: dict[str, Any],
        exclude_user: str | None = None
    ) -> dict[str, bool]:
        """
        Send a message to all members of a group.

        Args:
            member_ids: List of group member user IDs
            message: Message to send
            exclude_user: Optional user ID to exclude (e.g., sender)

        Returns:
            Dict mapping user_id -> delivery success
        """
        return await self.send_to_users(member_ids, message, exclude_user)

    def is_user_online(self, user_id: str) -> bool:
        """Check if a user has any active connections."""
        return user_id in self.active_connections and len(self.active_connections[user_id]) > 0

    def get_online_users(self, user_ids: list[str]) -> dict[str, bool]:
        """
        Check online status for multiple users.

        Returns:
            Dict mapping user_id -> online status
        """
        return {user_id: self.is_user_online(user_id) for user_id in user_ids}

    def update_presence(self, user_id: str) -> None:
        """Update last activity timestamp for a user."""
        self.user_presence[user_id] = datetime.now(timezone.utc)

    def get_presence(self, user_id: str) -> dict[str, Any]:
        """
        Get presence information for a user.

        Returns:
            Dict with online status and last_seen timestamp
        """
        online = self.is_user_online(user_id)
        last_seen = self.user_presence.get(user_id)

        return {
            "user_id": user_id,
            "online": online,
            "last_seen": last_seen.isoformat() if last_seen else None
        }

    def get_presence_batch(self, user_ids: list[str]) -> list[dict[str, Any]]:
        """Get presence information for multiple users."""
        return [self.get_presence(user_id) for user_id in user_ids]
