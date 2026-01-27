# app/connection_manager.py
"""WebSocket connection manager for handling user connections."""

import asyncio
import logging
from collections import defaultdict
from typing import Any

from starlette.websockets import WebSocket

from app.config import settings
from app.metrics import (
    ws_connections_active,
    ws_connections_total,
    ws_disconnections_total,
    ws_users_active,
    ws_messages_sent_total,
    ws_send_errors_total,
)
from app.redis_client import get_redis_pool

# Optional httpx for backend fallback requests
try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False

logger = logging.getLogger("realtime_service")

# Anonymous user ID as string (user_id in ConnectionManager is always str)
ANON_USER_ID = str(settings.ANONIM_USER)

# Redis key prefix for online status
ONLINE_STATUS_KEY_PREFIX = "user_online:"

# Redis key prefix for sandbox mode cache
SANDBOX_KEY_PREFIX = "sandbox:"

# TTL for sandbox cache keys (24 hours) - safety mechanism for stale entries
SANDBOX_CACHE_TTL = 86400


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

        # Prometheus metrics
        ws_connections_active.inc()
        ws_connections_total.inc()
        ws_users_active.set(len(self.active_connections))

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

        # Prometheus metrics
        ws_connections_active.dec()
        ws_disconnections_total.inc()
        ws_users_active.set(len(self.active_connections))

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
            ws_messages_sent_total.inc()
            logger.debug(f"Sent message to user {user_id}; message details: {message.get('type')}")
        except Exception as e:
            ws_send_errors_total.inc()
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

    # --- Sandbox Mode Caching Methods ---

    async def update_sandbox_cache(
        self,
        block_uuid: str,
        sandbox_mode: str | None,
        creator_id: str | int | None
    ) -> None:
        """
        Update sandbox mode cache for a block in Redis.

        Stores sandbox info if mode is 'open' or 'private' with TTL.
        Deletes the key if mode is 'none' or not set.
        """
        try:
            redis = await get_redis_pool()
            key = f"{SANDBOX_KEY_PREFIX}{block_uuid}"

            if sandbox_mode and sandbox_mode in ("open", "private"):
                async with redis.pipeline(transaction=True) as pipe:
                    pipe.hset(key, mapping={
                        "mode": sandbox_mode,
                        "creator_id": str(creator_id) if creator_id else ""
                    })
                    pipe.expire(key, SANDBOX_CACHE_TTL)
                    await pipe.execute()
                logger.debug(f"Updated sandbox cache for block {block_uuid}: mode={sandbox_mode}")
            else:
                await redis.delete(key)
                logger.debug(f"Deleted sandbox cache for block {block_uuid}")
        except Exception:
            logger.exception(f"Failed to update sandbox cache for block {block_uuid}")

    async def get_sandbox_info(self, block_uuid: str) -> dict[str, str] | None:
        """
        Get sandbox info for a block from Redis cache.

        Returns dict with 'mode' and 'creator_id' if block is a sandbox container.
        Returns None if block is not a sandbox or not cached.
        """
        if not block_uuid:
            return None

        try:
            redis = await get_redis_pool()
            key = f"{SANDBOX_KEY_PREFIX}{block_uuid}"
            data = await redis.hgetall(key)

            if data and data.get("mode") in ("open", "private"):
                return {
                    "mode": data.get("mode", ""),
                    "creator_id": data.get("creator_id", "")
                }
            return None
        except Exception:
            logger.exception(f"Failed to get sandbox info for block {block_uuid}")
            return None

    async def get_parent_sandbox_info(self, parent_id: str | None) -> dict[str, str] | None:
        """
        Get sandbox info for a parent block.

        First checks Redis cache. If not found and backend_url is configured,
        falls back to HTTP request to backend.

        Returns dict with 'mode' and 'creator_id' if parent is a sandbox container.
        Returns None if parent is not a sandbox or info not available.
        """
        if not parent_id:
            return None

        # Try Redis cache first
        cached = await self.get_sandbox_info(parent_id)
        if cached:
            return cached

        # Fallback: request from backend if configured and httpx is available
        if HTTPX_AVAILABLE and settings.backend_url and settings.service_token:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    resp = await client.get(
                        f"{settings.backend_url}/api/v1/blocks/{parent_id}/sandbox/",
                        headers={"Authorization": f"Bearer {settings.service_token}"}
                    )
                    if resp.status_code == 200:
                        info = resp.json()
                        mode = info.get("sandbox_mode", "none")
                        if mode in ("open", "private"):
                            creator_id = str(info.get("creator_id", ""))
                            # Cache the result
                            await self.update_sandbox_cache(parent_id, mode, creator_id)
                            return {"mode": mode, "creator_id": creator_id}
            except Exception:
                logger.exception(f"Failed to fetch sandbox info from backend for {parent_id}")

        return None

    def filter_subscribers_for_private_sandbox(
        self,
        subscribers: set[str],
        block_creator_id: str | int | None,
        container_owner_id: str | None
    ) -> set[str]:
        """
        Filter subscribers for a block in a private sandbox.

        Only allows:
        - Block creator (the user who created this specific block)
        - Container owner (the owner of the sandbox container)

        Args:
            subscribers: Set of user IDs subscribed to the block
            block_creator_id: Creator of the block being updated
            container_owner_id: Owner of the private sandbox container

        Returns:
            Filtered set of user IDs who should receive the update
        """
        block_creator_str = str(block_creator_id) if block_creator_id else ""
        container_owner_str = str(container_owner_id) if container_owner_id else ""

        filtered = set()
        for user_id in subscribers:
            if user_id == block_creator_str or user_id == container_owner_str:
                filtered.add(user_id)

        return filtered
