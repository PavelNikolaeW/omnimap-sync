# app/main.py
"""FastAPI application entry point with WebSocket and RabbitMQ consumer."""

import asyncio
import logging
from typing import Optional

from aio_pika import connect_robust
from fastapi import FastAPI, WebSocket
from fastapi.responses import JSONResponse

from app.config import settings
from app.rabbitmq_consumer import start_consumer
from app.redis_client import close_redis_pool, get_redis_pool
from app.websockets import websocket_endpoint

app = FastAPI()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("realtime_service")

# Global reference to consumer task for cleanup
rabbitmq_consumer_task: Optional[asyncio.Task] = None


@app.on_event("startup")
async def startup_event() -> None:
    """Start RabbitMQ consumer on application startup."""
    global rabbitmq_consumer_task
    rabbitmq_consumer_task = asyncio.create_task(start_consumer())
    logger.info("RabbitMQ consumer started")


@app.on_event("shutdown")
async def shutdown_event() -> None:
    """
    Graceful shutdown handler.

    Cancels RabbitMQ consumer and closes Redis connections.
    """
    global rabbitmq_consumer_task

    # Cancel RabbitMQ consumer task
    if rabbitmq_consumer_task:
        logger.info("Cancelling RabbitMQ consumer task...")
        rabbitmq_consumer_task.cancel()
        try:
            await rabbitmq_consumer_task
        except asyncio.CancelledError:
            logger.info("RabbitMQ consumer task cancelled successfully.")
        except Exception:
            logger.exception("Error while cancelling RabbitMQ consumer task")

    # Close Redis connection pool
    await close_redis_pool()


@app.on_event("startup")
async def check_services() -> None:
    """
    Verify Redis and RabbitMQ connectivity on startup.

    Raises exception if services are not available.
    """
    # Check Redis
    try:
        redis = await get_redis_pool()
        await redis.ping()
        logger.info("Redis connected")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise

    # Check RabbitMQ
    try:
        connection = await connect_robust(settings.rabbitmq_url, heartbeat=60)
        logger.info("RabbitMQ connected")
        await connection.close()
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        raise


@app.get("/health")
async def health_check() -> JSONResponse:
    """Health check endpoint with CORS headers for frontend monitoring."""
    return JSONResponse(
        content={"status": "healthy"},
        headers={"Access-Control-Allow-Origin": "*"}
    )


@app.websocket("/ws")
async def websocket_route(websocket: WebSocket) -> None:
    """WebSocket endpoint for real-time block updates."""
    await websocket_endpoint(websocket)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=True
    )
