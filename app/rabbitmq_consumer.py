# app/rabbitmq_consumer.py
import asyncio
import json
import logging

from aio_pika import connect_robust, IncomingMessage, RobustChannel, RobustConnection, exceptions
from app.config import settings
from app.redis_client import get_redis_pool
from app.websockets import connection_manager

logger = logging.getLogger("realtime_service")


async def action_update_block(message: dict):
    """
    Обрабатывает обновление блока.
    """
    try:
        block_uuid = message["block_uuid"]
        block_data = message["block_data"]
    except KeyError as e:
        logger.error(f"Missing required key in message: {e}")
        return
    redis = await get_redis_pool()
    key_data = f'blockdata:{block_uuid}'
    try:
        await redis.hset(key_data, mapping=block_data)
    except Exception:
        logger.exception(f"Failed to save block data for block_uuid={block_uuid}")
        return

    # Рассылаем обновление подписанным клиентам
    try:
        subscribers = await redis.smembers(f"block:{block_uuid}")
        if not subscribers:
            logger.debug(f"No subscribers found for block_uuid={block_uuid}")
            return

        await connection_manager.send_message_to_subscribers({
                    "type": "block_update",
                    "block_uuid": block_uuid,
                    "data": block_data
                }, subscribers)

        logger.info(f"Processed update_block for block_uuid={block_uuid}")
    except Exception:
        logger.exception(f"Failed to notify subscribers for block_uuid={block_uuid}")


async def send_message_update_access(start_block_ids, block_uuids, user_id, permission, redis):
    if permission == "deny":
        data_list = [{**settings.FORBIDDEN_BLOCK, 'id': block_id} for block_id in start_block_ids]
    else:
        keys = [f'blockdata:{block_id}' for block_id in start_block_ids]
        pipe = redis.pipeline()
        for key in keys:
            pipe.hgetall(key)
        results = await pipe.execute()
        data_list = [data for data in results if data]
    if data_list:
        message = {
            'type': 'block_update_access',
            'start_block_ids': data_list,
            'block_uuids': block_uuids,
            'permission': permission
        }
        await connection_manager.send_personal_message(message, str(user_id))
    else:
        logger.warning('No messages')


async def action_update_access(message: dict):
    """
    Если permission = deny, то удаляем пользователя из множеств:
      - block:{block_uuid}: удаляем user_id
      - subscriber:{user_id}:blocks: удаляем block_uuid
    И наоборот, при разрешении – добавляем.
    """
    try:
        user_id = message["user_id"]
        permission = message["permission"]
        start_block_ids = message["start_block_ids"]
        block_uuids = message["block_uuids"] + start_block_ids
    except KeyError as e:
        logger.error(f"Missing required key in message: {e}")
        return
    redis = await get_redis_pool()
    block_portion = settings.block_portion
    try:
        for i in range(0, len(block_uuids), block_portion):
            chunk = block_uuids[i:i + block_portion]
            async with redis.pipeline(transaction=True) as pipe:
                if permission == 'deny':
                    for block_uuid in chunk:
                        pipe.srem(f"block:{block_uuid}", user_id)
                        pipe.srem(f"subscriber:{user_id}:blocks", block_uuid)
                    logger.info(f"User {user_id} access revoked for {len(chunk)} blocks.")
                else:
                    for block_uuid in chunk:
                        pipe.sadd(f"block:{block_uuid}", user_id)
                        pipe.sadd(f"subscriber:{user_id}:blocks", block_uuid)
                    logger.info(f"User {user_id} access granted for {len(chunk)} blocks.")
                await pipe.execute()
        await send_message_update_access(start_block_ids, block_uuids, user_id, permission, redis)
    except Exception:
        logger.exception("Failed to update access permissions in Redis")


async def action_subscribe(message: dict):
    """
    Обрабатывает подписку клиента на список блоков.
    Обновляет два набора:
      - block:{block_uuid}: добавляет user_id в подписчики блока.
      - subscriber:{user_id}:blocks: добавляет block_uuid в набор блоков подписчика.
    """
    try:
        user_id = message["user_id"]
        block_uuids = message["block_uuids"]
    except KeyError as e:
        logger.error(f"Missing required key in message: {e}")
        return

    redis = await get_redis_pool()
    block_portion = settings.block_portion

    try:
        for i in range(0, len(block_uuids), block_portion):
            chunk = block_uuids[i:i + block_portion]
            async with redis.pipeline(transaction=True) as pipe:
                for block_uuid in chunk:
                    pipe.sadd(f"block:{block_uuid}", user_id)
                    pipe.sadd(f"subscriber:{user_id}:blocks", block_uuid)
                await pipe.execute()

        logger.info(f"Users {user_id} subscribed to {len(block_uuids)} blocks successfully.")
    except Exception:
        logger.exception("Failed to subscribe users to blocks in Redis")


async def handle_message(message: IncomingMessage):
    """
    Обрабатывает входящие сообщения RabbitMQ.
    """
    async with message.process():
        try:
            message_data = json.loads(message.body)
        except json.JSONDecodeError:
            logger.error("Failed to decode JSON message")
            return
        action = message_data.get('action')
        if action == 'update_block':
            await action_update_block(message_data)
        elif action == 'update_access':
            await action_update_access(message_data)
        elif action == 'subscribe':
            await action_subscribe(message_data)
        else:
            logger.warning(f"Unknown action received: {action}")


async def consume():
    """
    Настраивает соединение с RabbitMQ и начинает потребление сообщений.
    """
    try:
        connection: RobustConnection = await connect_robust(settings.rabbitmq_url, heartbeat=60)
        channel: RobustChannel = await connection.channel()

        # Устанавливаем очередь
        queue = await channel.declare_queue(settings.queue_name, durable=True)
        await queue.consume(handle_message, no_ack=False)

        logger.info(f"Waiting for messages from {settings.queue_name}")

        return connection
    except exceptions.AMQPConnectionError as e:
        logger.error(f"RabbitMQ connection error: {e}")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error while setting up consumer: {e}")
        raise


async def start_consumer():
    """
    Запускает цикл подключения и потребления сообщений.
    Если соединение с RabbitMQ теряется, пытаемся переподключиться через 5 секунд.
    """
    while True:
        connection = None
        try:
            connection = await consume()  # Устанавливаем соединение и начинаем потребление
            logger.info("Connection established. Waiting for messages...")

            # Ждем, пока соединение явно не закроется
            await asyncio.sleep(float('inf'))  # Бесконечное ожидание, пока соединение активно
        except asyncio.CancelledError:
            logger.info("Consumer task cancelled. Exiting...")
            if connection and not connection.is_closed:
                await connection.close()
            break
        except exceptions.AMQPConnectionError as e:
            logger.error(f"Lost connection to RabbitMQ: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.exception(f"Unexpected error in consumer loop: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)
