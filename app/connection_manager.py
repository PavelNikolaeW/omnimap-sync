# app/connection_manager.py
"""WebSocket connection manager for handling user connections."""

import asyncio
import logging
from collections import defaultdict
from typing import Any

from starlette.websockets import WebSocket

from app.config import settings
from app.redis_client import get_redis_pool

logger = logging.getLogger("realtime_service")

# Anonymous user ID as string (user_id in ConnectionManager is always str)
ANON_USER_ID = str(settings.ANONIM_USER)

# Redis key prefix for online status
ONLINE_STATUS_KEY_PREFIX = "user_online:"


class ConnectionManager:
    """
    Manages WebSocket connections for all users.

    Features:
    - Per-user connection lists with thread-safe locking
    - Semaphore-based rate limiting for anonymous users
    - Automatic cleanup of disconnected sockets
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
        Also tracks online status in Redis for non-anonymous users.
        """
        user_lock = await self._get_user_lock(user_id)
        async with user_lock:
            self.active_connections[user_id].append(websocket)
        logger.info(
            f"User {user_id} connected. "
            f"Total connections: {len(self.active_connections[user_id])}"
        )

        # Track online status in Redis (skip anonymous users)
        if user_id != ANON_USER_ID:
            await self._increment_online_counter(user_id)

    async def disconnect(self, user_id: str, websocket: WebSocket) -> None:
        """
        Remove a WebSocket connection for a user.

        Cleans up user lock when all connections are closed.
        Also decrements online status counter in Redis for non-anonymous users.
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

        # Decrement online counter in Redis (skip anonymous users)
        if user_id != ANON_USER_ID:
            await self._decrement_online_counter(user_id)

        # Cleanup lock outside of user_lock to avoid holding lock while deleting it
        if should_cleanup:
            async with self.global_lock:
                # Double-check that user still has no connections before removing lock
                if user_id not in self.active_connections and user_id in self.user_locks:
                    del self.user_locks[user_id]
            logger.info(f"All connections for user {user_id} have been removed.")

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

    async def send_to_user(self, user_id: str, message: dict[str, Any]) -> bool:
        """
        Send a message to a specific user.

        Returns True if user has active connections and message was sent,
        False if user is offline (no active connections).
        """
        user_lock = await self._get_user_lock(user_id)
        async with user_lock:
            connections = list(self.active_connections.get(user_id, []))

        if not connections:
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
            Dict mapping user_id to delivery status (True if delivered, False if offline)
        """
        results: dict[str, bool] = {}

        async def send_and_track(uid: str) -> tuple[str, bool]:
            delivered = await self.send_to_user(uid, message)
            return uid, delivered

        tasks = [
            send_and_track(uid)
            for uid in user_ids
            if uid != exclude_user
        ]

        if tasks:
            task_results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in task_results:
                if isinstance(result, tuple):
                    uid, delivered = result
                    results[uid] = delivered
                # Exceptions are silently ignored (user treated as offline)

        return results

    # --- Online Status Tracking Methods ---

    async def _increment_online_counter(self, user_id: str) -> None:
        """
        Increment the online counter for a user in Redis.

        Uses pipeline with transaction to ensure atomicity of incr + expire.
        Sets TTL to auto-expire if no activity (protection against crashes).
        Gracefully handles Redis errors to not block connections.
        """
        try:
            redis = await get_redis_pool()
            key = f"{ONLINE_STATUS_KEY_PREFIX}{user_id}"
            async with redis.pipeline(transaction=True) as pipe:
                pipe.incr(key)
                pipe.expire(key, settings.ONLINE_STATUS_TTL)
                await pipe.execute()
            logger.debug(f"Incremented online counter for user {user_id}")
        except Exception:
            logger.exception(f"Failed to increment online counter for user {user_id}")

    async def _decrement_online_counter(self, user_id: str) -> None:
        """
        Decrement the online counter for a user in Redis.

        If counter reaches 0 or below, deletes the key.
        Logs warning if counter goes negative (indicates missed increment).
        Gracefully handles Redis errors.
        """
        try:
            redis = await get_redis_pool()
            key = f"{ONLINE_STATUS_KEY_PREFIX}{user_id}"
            current = await redis.decr(key)
            if current <= 0:
                await redis.delete(key)
                if current < 0:
                    logger.warning(
                        f"Online counter went negative for user {user_id}, cleaned up. "
                        "This may indicate a missed increment (server crash or Redis flush)."
                    )
                else:
                    logger.debug(f"User {user_id} went offline (counter reached 0)")
            else:
                logger.debug(f"Decremented online counter for user {user_id}, remaining: {current}")
        except Exception:
            logger.exception(f"Failed to decrement online counter for user {user_id}")

    async def refresh_user_online_ttl(self, user_id: str) -> None:
        """
        Refresh the TTL on user's online status key.

        Called on heartbeat/ping to prevent key expiration for active users.
        Skips anonymous users.
        Gracefully handles Redis errors.
        """
        if user_id == ANON_USER_ID:
            return

        try:
            redis = await get_redis_pool()
            key = f"{ONLINE_STATUS_KEY_PREFIX}{user_id}"
            # expire() returns 1 if TTL was set, 0 if key doesn't exist
            result = await redis.expire(key, settings.ONLINE_STATUS_TTL)
            if result:
                logger.debug(f"Refreshed online TTL for user {user_id}")
        except Exception:
            logger.exception(f"Failed to refresh online TTL for user {user_id}")
