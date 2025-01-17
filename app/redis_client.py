import redis.asyncio as redis
from app.config import settings

redis_client = None


async def get_redis_pool():
    global redis_client
    if not redis_client:
        redis_client = await redis.from_url(
            settings.redis_url, decode_responses=True, max_connections=20
        )
    return redis_client
