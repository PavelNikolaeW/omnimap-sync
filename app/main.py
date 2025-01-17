# app/main.py

import logging
import asyncio
import signal

from aio_pika import connect_robust
from fastapi import FastAPI, WebSocket
from fastapi.responses import Response

from app.redis_client import get_redis_pool
from app.websockets import websocket_endpoint
from app.rabbitmq_consumer import start_consumer
from app.config import settings

app = FastAPI()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("realtime_service")
rabbitmq_consumer_task = None


@app.on_event("startup")
async def startup_event():
    global rabbitmq_consumer_task
    rabbitmq_consumer_task = asyncio.create_task(start_consumer())
    logger.info("RabbitMQ consumer started")


@app.on_event("shutdown")
async def shutdown_event():
    global rabbitmq_consumer_task
    if rabbitmq_consumer_task:
        logger.info("Cancelling RabbitMQ consumer task...")
        rabbitmq_consumer_task.cancel()
        try:
            await rabbitmq_consumer_task
        except asyncio.CancelledError:
            logger.info("RabbitMQ consumer task cancelled successfully.")


@app.on_event("startup")
async def check_services():
    """
    Проверяем доступность Redis и RabbitMQ при старте.
    """
    try:
        redis = await get_redis_pool()
        await redis.ping()
        logger.info("Redis connected")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise

    try:
        connection = await connect_robust(settings.rabbitmq_url, heartbeat=60)
        logger.info("RabbitMQ connected")
        await connection.close()
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        raise


@app.websocket("/ws")
async def websocket_route(websocket: WebSocket):
    await websocket_endpoint(websocket)


if __name__ == "__main__":
    import uvicorn

    # uvicorn app.main:app --host 0.0.0.0 --port 7999 --reload
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=True
    )
