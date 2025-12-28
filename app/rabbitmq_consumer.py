# app/rabbitmq_consumer.py
"""RabbitMQ message consumer for real-time block updates."""

import asyncio
import json
import logging
from collections import defaultdict
from typing import Any

from aio_pika import connect_robust, IncomingMessage, RobustChannel, RobustConnection, ExchangeType, exceptions
from pydantic import ValidationError
from redis.asyncio import Redis

from app.config import settings
from app.models import (
    UpdateBlockMessage,
    UpdateBlocksMessage,
    UpdateAccessMessage,
    SubscribeMessage,
    UnsubscribeMessage,
    BlockUpdateResponse,
    BlockUpdatesBatchResponse,
    BlockUpdateAccessResponse,
    NotificationEventMessage,
    ReminderEventResponse,
    SubscriptionEventResponse,
)
from app.redis_client import get_redis_pool
from app.websockets import connection_manager

logger = logging.getLogger("realtime_service")


async def action_update_block(message_data: dict[str, Any]) -> None:
    """
    Process a single block update.

    Saves block data to Redis and notifies all subscribers.
    """
    try:
        msg = UpdateBlockMessage(**message_data)
    except ValidationError as e:
        logger.error(f"Invalid update_block message: {e}")
        return

    redis = await get_redis_pool()
    key_data = f'blockdata:{msg.block_uuid}'

    try:
        await redis.hset(key_data, mapping=msg.block_data)
    except Exception:
        logger.exception(f"Failed to save block data for block_uuid={msg.block_uuid}")
        return

    # Notify subscribed clients
    try:
        subscribers = await redis.smembers(f"block:{msg.block_uuid}")
        if not subscribers:
            logger.debug(f"No subscribers found for block_uuid={msg.block_uuid}")
            return

        response = BlockUpdateResponse(
            block_uuid=msg.block_uuid,
            data=msg.block_data
        )
        await connection_manager.send_message_to_subscribers(
            response.model_dump(),
            subscribers
        )
        logger.info(f"Processed update_block for block_uuid={msg.block_uuid}")
    except Exception:
        logger.exception(f"Failed to notify subscribers for block_uuid={msg.block_uuid}")


async def action_update_blocks(message_data: dict[str, Any]) -> None:
    """
    Process batch update of multiple blocks.

    Saves all block data to Redis using pipeline and notifies subscribers.
    Groups notifications by user to reduce N+1 send operations.
    """
    try:
        msg = UpdateBlocksMessage(**message_data)
    except ValidationError as e:
        logger.error(f"Invalid update_blocks message: {e}")
        return

    redis = await get_redis_pool()

    # Step 1: Save block data using pipeline
    try:
        pipe = redis.pipeline(transaction=True)
        for block_uuid, block_data in msg.blocks.items():
            if not isinstance(block_data, dict):
                logger.error(f"block_data for {block_uuid} is not a dict")
                continue
            pipe.hset(f"blockdata:{block_uuid}", mapping=block_data)
        await pipe.execute()
    except Exception:
        logger.exception("Failed to save block data for batch update")
        return

    # Step 2: Collect subscribers and batch notifications by user
    try:
        subscribers_by_block: dict[str, set[str]] = {}

        if msg.blocks:
            pipe = redis.pipeline(transaction=True)
            for block_uuid in msg.blocks.keys():
                pipe.smembers(f"block:{block_uuid}")
            subscribers_lists = await pipe.execute()

            for block_uuid, subs in zip(msg.blocks.keys(), subscribers_lists):
                if subs:
                    subscribers_by_block[block_uuid] = subs

        # Group updates by user to reduce number of send operations
        user_updates: dict[str, list[dict[str, Any]]] = {}
        for block_uuid, block_data in msg.blocks.items():
            subs = subscribers_by_block.get(block_uuid)
            if not subs:
                logger.debug(f"No subscribers for block_uuid={block_uuid}")
                continue

            update_msg = BlockUpdateResponse(
                block_uuid=block_uuid,
                data=block_data
            ).model_dump()

            for user_id in subs:
                if user_id not in user_updates:
                    user_updates[user_id] = []
                user_updates[user_id].append(update_msg)

        # Send batched updates to each user
        for user_id, updates in user_updates.items():
            if len(updates) == 1:
                await connection_manager.send_personal_message(updates[0], user_id)
            else:
                batch_response = BlockUpdatesBatchResponse(updates=updates)
                await connection_manager.send_personal_message(
                    batch_response.model_dump(),
                    user_id
                )

        logger.info(f"Processed update_blocks for {len(msg.blocks)} blocks, notified {len(user_updates)} users")
    except Exception:
        logger.exception("Failed to notify subscribers during batch update")


async def send_message_update_access(
    start_block_ids: list[str],
    block_uuids: list[str],
    user_id: str | int,
    permission: str,
    redis: Redis
) -> None:
    """
    Send access update notification to user.

    For 'deny' permission, sends forbidden block data.
    For other permissions, fetches actual block data from Redis.
    """
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
        response = BlockUpdateAccessResponse(
            start_block_ids=data_list,
            block_uuids=block_uuids,
            permission=permission
        )
        await connection_manager.send_personal_message(response.model_dump(), str(user_id))
    else:
        logger.warning('No messages to send for access update')


async def action_update_access(message_data: dict[str, Any]) -> None:
    """
    Process access permission update.

    For 'deny' permission: removes user from block subscribers.
    For 'grant' permission: adds user to block subscribers.
    """
    try:
        msg = UpdateAccessMessage(**message_data)
    except ValidationError as e:
        logger.error(f"Invalid update_access message: {e}")
        return

    user_id = str(msg.user_id)
    block_uuids = msg.block_uuids + msg.start_block_ids

    redis = await get_redis_pool()
    block_portion = settings.block_portion

    try:
        for i in range(0, len(block_uuids), block_portion):
            chunk = block_uuids[i:i + block_portion]
            async with redis.pipeline(transaction=True) as pipe:
                if msg.permission == 'deny':
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

        await send_message_update_access(
            msg.start_block_ids,
            block_uuids,
            user_id,
            msg.permission,
            redis
        )
    except Exception:
        logger.exception("Failed to update access permissions in Redis")


async def action_subscribe(message_data: dict[str, Any]) -> None:
    """
    Process user subscription to blocks.

    Updates two Redis sets:
      - block:{block_uuid}: adds user_id to block subscribers
      - subscriber:{user_id}:blocks: adds block_uuid to user's subscribed blocks
    """
    try:
        msg = SubscribeMessage(**message_data)
    except ValidationError as e:
        logger.error(f"Invalid subscribe message: {e}")
        return

    user_id = str(msg.user_id)
    redis = await get_redis_pool()
    block_portion = settings.block_portion

    try:
        for i in range(0, len(msg.block_uuids), block_portion):
            chunk = msg.block_uuids[i:i + block_portion]
            async with redis.pipeline(transaction=True) as pipe:
                for block_uuid in chunk:
                    pipe.sadd(f"block:{block_uuid}", user_id)
                    pipe.sadd(f"subscriber:{user_id}:blocks", block_uuid)
                await pipe.execute()

        logger.info(f"User {user_id} subscribed to {len(msg.block_uuids)} blocks successfully.")
    except Exception:
        logger.exception("Failed to subscribe user to blocks in Redis")


async def action_unsubscribe(message_data: dict[str, Any]) -> None:
    """
    Process block unsubscription (removes blocks entirely).

    Removes blocks from all user subscriptions and deletes block data from Redis.
    """
    try:
        msg = UnsubscribeMessage(**message_data)
    except ValidationError as e:
        logger.error(f"Invalid unsubscribe message: {e}")
        return

    redis = await get_redis_pool()
    block_portion = settings.block_portion

    try:
        for i in range(0, len(msg.block_uuids), block_portion):
            chunk = msg.block_uuids[i:i + block_portion]

            # Get all subscribers for blocks in one pipeline
            pipe = redis.pipeline()
            for block_uuid in chunk:
                pipe.smembers(f"block:{block_uuid}")
            results = await pipe.execute()

            # Build mapping: user_id -> set(block_uuid)
            user_blocks: dict[str, set[str]] = defaultdict(set)
            for block_uuid, user_ids in zip(chunk, results):
                for user_id in user_ids:
                    user_blocks[user_id].add(block_uuid)

            # Remove block_uuids from user subscriptions
            pipe = redis.pipeline(transaction=True)
            for user_id, blocks in user_blocks.items():
                pipe.srem(f"subscriber:{user_id}:blocks", *blocks)

            # Delete block and blockdata keys
            pipe.delete(*[f"block:{uuid}" for uuid in chunk])
            pipe.delete(*[f"blockdata:{uuid}" for uuid in chunk])

            await pipe.execute()

        logger.info(f"Successfully unsubscribed and cleaned up {len(msg.block_uuids)} blocks")
    except Exception:
        logger.exception("Failed to process unsubscribe action")


# =============================================================================
# Notification Event Types (Reminders & Subscriptions)
# =============================================================================

REMINDER_EVENT_TYPES = {
    'reminder_created',
    'reminder_updated',
    'reminder_deleted',
    'reminder_triggered',
    'reminder_snoozed',
}

SUBSCRIPTION_EVENT_TYPES = {
    'subscription_created',
    'subscription_updated',
    'subscription_deleted',
}

NOTIFICATION_EVENT_TYPES = REMINDER_EVENT_TYPES | SUBSCRIPTION_EVENT_TYPES


async def action_notification_event(message_data: dict[str, Any]) -> None:
    """
    Process notification events (reminders and subscriptions).

    Routes events to the specific user based on user_id.
    Events are forwarded directly to the user's WebSocket connections.
    """
    try:
        msg = NotificationEventMessage(**message_data)
    except ValidationError as e:
        logger.error(f"Invalid notification event message: {e}")
        return

    user_id = str(msg.user_id)
    event_type = msg.type

    # Create response based on event type
    if event_type in REMINDER_EVENT_TYPES:
        response = ReminderEventResponse(
            type=event_type,
            data=msg.data
        )
    elif event_type in SUBSCRIPTION_EVENT_TYPES:
        response = SubscriptionEventResponse(
            type=event_type,
            data=msg.data
        )
    else:
        logger.warning(f"Unknown notification event type: {event_type}")
        return

    # Send to user's WebSocket connections
    await connection_manager.send_personal_message(response.model_dump(), user_id)
    logger.info(f"Sent {event_type} event to user {user_id}")


async def handle_message(message: IncomingMessage) -> None:
    """
    Process incoming RabbitMQ messages.

    Invalid messages are rejected (not requeued) to prevent infinite retry loops.
    Configure a Dead Letter Queue (DLQ) in RabbitMQ to capture rejected messages.
    """
    try:
        message_data: dict[str, Any] = json.loads(message.body)
    except json.JSONDecodeError:
        logger.exception(f"Failed to decode JSON message: {message.body[:100]}")
        await message.reject(requeue=False)
        return

    action = message_data.get('action')
    event_type = message_data.get('type')

    try:
        # Handle action-based messages (legacy format)
        if action == 'update_block':
            await action_update_block(message_data)
        elif action == 'update_blocks':
            await action_update_blocks(message_data)
        elif action == 'update_access':
            await action_update_access(message_data)
        elif action == 'subscribe':
            await action_subscribe(message_data)
        elif action == 'unsubscribe':
            await action_unsubscribe(message_data)
        # Handle type-based messages (notification events)
        elif event_type in NOTIFICATION_EVENT_TYPES:
            await action_notification_event(message_data)
        else:
            logger.warning(f"Unknown message received: action={action}, type={event_type}")

        await message.ack()
    except Exception:
        logger.exception(f"Failed to process message with action '{action}'")
        await message.reject(requeue=False)


def _get_exchange_type(type_str: str) -> ExchangeType:
    """Convert string exchange type to ExchangeType enum."""
    mapping = {
        'direct': ExchangeType.DIRECT,
        'fanout': ExchangeType.FANOUT,
        'topic': ExchangeType.TOPIC,
        'headers': ExchangeType.HEADERS,
    }
    return mapping.get(type_str.lower(), ExchangeType.DIRECT)


async def consume() -> RobustConnection:
    """
    Set up RabbitMQ connection and start consuming messages.

    Automatically creates exchange, queue, and binding if they don't exist.
    Returns the connection for lifecycle management.
    """
    try:
        connection: RobustConnection = await connect_robust(settings.rabbitmq_url, heartbeat=60)
        channel: RobustChannel = await connection.channel()

        # Declare exchange (creates if not exists)
        exchange = await channel.declare_exchange(
            settings.exchange_name,
            type=_get_exchange_type(settings.exchange_type),
            durable=True,
        )
        logger.info(f"Exchange '{settings.exchange_name}' declared (type: {settings.exchange_type})")

        # Declare queue (creates if not exists)
        queue = await channel.declare_queue(settings.queue_name, durable=True)
        logger.info(f"Queue '{settings.queue_name}' declared")

        # Bind queue to exchange with routing key
        await queue.bind(exchange, routing_key=settings.routing_key)
        logger.info(f"Queue '{settings.queue_name}' bound to exchange '{settings.exchange_name}' with routing_key '{settings.routing_key}'")

        await queue.consume(handle_message, no_ack=False)

        logger.info(f"Waiting for messages from {settings.queue_name}")
        return connection
    except exceptions.AMQPConnectionError as e:
        logger.error(f"RabbitMQ connection error: {e}")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error while setting up consumer: {e}")
        raise


async def start_consumer() -> None:
    """
    Start the message consumption loop with automatic reconnection.

    If RabbitMQ connection is lost, retries after 5 seconds.
    """
    while True:
        connection: RobustConnection | None = None
        try:
            connection = await consume()
            logger.info("Connection established. Waiting for messages...")
            await asyncio.sleep(float('inf'))
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
