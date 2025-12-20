# app/redis_client.py
"""Redis client singleton with connection pooling."""

import asyncio
import logging

import redis.asyncio as redis
from redis.asyncio import Redis

from app.config import settings

logger = logging.getLogger("realtime_service")

# Global Redis client singleton
_redis_client: Redis | None = None
_redis_lock = asyncio.Lock()


async def get_redis_pool() -> Redis:
    """
    Get or create Redis connection pool (singleton).

    Uses double-checked locking pattern for thread safety.

    Returns:
        Redis client instance
    """
    global _redis_client

    if _redis_client is not None:
        return _redis_client

    async with _redis_lock:
        # Double-check after acquiring lock
        if _redis_client is not None:
            return _redis_client

        _redis_client = await redis.from_url(
            settings.redis_url,
            decode_responses=True,
            max_connections=20
        )
        logger.info("Redis connection pool created")
        return _redis_client


async def close_redis_pool() -> None:
    """
    Close Redis connection pool gracefully.

    Safe to call even if pool was never initialized.
    """
    global _redis_client

    async with _redis_lock:
        if _redis_client is not None:
            try:
                await _redis_client.close()
                logger.info("Redis connection pool closed")
            except Exception:
                logger.exception("Error closing Redis connection pool")
            finally:
                _redis_client = None
