# app/rabbitmq_consumer.py
"""RabbitMQ message consumer for real-time block updates."""

import asyncio
import json
import logging
import time
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
    SandboxModeChangedMessage,
    BlockUpdateResponse,
    BlockUpdatesBatchResponse,
    BlockUpdateAccessResponse,
    SandboxModeChangedResponse,
    NotificationEventMessage,
    ReminderEventResponse,
    SubscriptionEventResponse,
    ChatEventMessage,
    ChatEventResponse,
    AccessRequestMessage,
    AccessRequestResponse,
)
from app.metrics import (
    rabbitmq_messages_total,
    rabbitmq_message_errors_total,
    rabbitmq_message_duration_seconds,
    rabbitmq_reconnects_total,
)
from app.redis_client import get_redis_pool
from app.utils import prepare_block_data_for_redis, parse_redis_block_data
from app.websockets import connection_manager

logger = logging.getLogger("realtime_service")


async def action_update_block(message_data: dict[str, Any]) -> None:
    """
    Process a single block update.

    Saves block data to Redis and notifies all subscribers.
    Filters recipients for blocks in private sandboxes.

    Uses sandbox_context from message if provided (preferred),
    falls back to Redis lookup for backwards compatibility.
    """
    try:
        msg = UpdateBlockMessage(**message_data)
    except ValidationError as e:
        logger.error(f"Invalid update_block message: {e}")
        return

    redis = await get_redis_pool()
    key_data = f'blockdata:{msg.block_uuid}'

    try:
        prepared_data = prepare_block_data_for_redis(msg.block_data)
        await redis.hset(key_data, mapping=prepared_data)
    except Exception:
        logger.exception(f"Failed to save block data for block_uuid={msg.block_uuid}")
        return

    # Update sandbox cache if this block is a sandbox container
    sandbox_mode = msg.block_data.get("sandbox_mode")
    if sandbox_mode:
        creator_id = msg.block_data.get("creator_id")
        await connection_manager.update_sandbox_cache(msg.block_uuid, sandbox_mode, creator_id)

    # Notify subscribed clients
    try:
        subscribers = await redis.smembers(f"block:{msg.block_uuid}")
        if not subscribers:
            logger.debug(f"No subscribers found for block_uuid={msg.block_uuid}")
            return

        # Check if parent is a private sandbox
        parent_id = msg.block_data.get("parent_id")
        if parent_id:
            parent_id_str = str(parent_id)
            parent_sandbox: dict[str, Any] | None = None

            # Use sandbox_context from message if provided (no Redis lookup needed)
            if msg.sandbox_context and parent_id_str in msg.sandbox_context:
                ctx = msg.sandbox_context[parent_id_str]
                parent_sandbox = {"mode": ctx.mode, "creator_id": str(ctx.creator_id) if ctx.creator_id else ""}
            else:
                # Fallback: lookup in Redis (backwards compatibility)
                parent_sandbox = await connection_manager.get_parent_sandbox_info(parent_id_str)

            if parent_sandbox and parent_sandbox.get("mode") == "private":
                # Filter subscribers for private sandbox
                block_creator_id = msg.block_data.get("creator_id")
                container_owner_id = parent_sandbox.get("creator_id")
                subscribers = connection_manager.filter_subscribers_for_private_sandbox(
                    subscribers,
                    block_creator_id,
                    container_owner_id
                )
                logger.debug(
                    f"Private sandbox filter: block={msg.block_uuid}, "
                    f"filtered to {len(subscribers)} subscribers"
                )

                if not subscribers:
                    logger.debug(f"No authorized subscribers for private sandbox block {msg.block_uuid}")
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
    Filters recipients for blocks in private sandboxes.

    Uses sandbox_context from message if provided (preferred),
    falls back to Redis lookup for backwards compatibility.
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
            prepared_data = prepare_block_data_for_redis(block_data)
            pipe.hset(f"blockdata:{block_uuid}", mapping=prepared_data)
        await pipe.execute()
    except Exception:
        logger.exception("Failed to save block data for batch update")
        return

    # Step 1.5: Update sandbox cache for blocks that are sandbox containers
    for block_uuid, block_data in msg.blocks.items():
        if isinstance(block_data, dict):
            sandbox_mode = block_data.get("sandbox_mode")
            if sandbox_mode:
                creator_id = block_data.get("creator_id")
                await connection_manager.update_sandbox_cache(block_uuid, sandbox_mode, creator_id)

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

        # Build parent sandbox cache from message context or Redis fallback
        parent_sandbox_cache: dict[str, dict[str, str] | None] = {}

        if msg.sandbox_context:
            # Use sandbox_context from message (preferred - no Redis lookups)
            for parent_id, ctx in msg.sandbox_context.items():
                parent_sandbox_cache[parent_id] = {
                    "mode": ctx.mode,
                    "creator_id": str(ctx.creator_id) if ctx.creator_id else ""
                }
            logger.debug(f"Using sandbox_context from message: {len(parent_sandbox_cache)} parents")
        else:
            # Fallback: collect parent IDs and fetch from Redis (backwards compatibility)
            parent_ids: set[str] = set()
            for block_data in msg.blocks.values():
                if isinstance(block_data, dict):
                    parent_id = block_data.get("parent_id")
                    if parent_id:
                        parent_ids.add(str(parent_id))

            # Fetch sandbox info using pipeline to avoid connection exhaustion
            if parent_ids:
                parent_ids_list = list(parent_ids)
                pipe = redis.pipeline(transaction=True)
                for pid in parent_ids_list:
                    pipe.hgetall(f"sandbox:{pid}")
                results = await pipe.execute()

                for pid, data in zip(parent_ids_list, results):
                    if data and data.get("mode") in ("open", "private"):
                        parent_sandbox_cache[pid] = {
                            "mode": data.get("mode", ""),
                            "creator_id": data.get("creator_id", "")
                        }

        # Group updates by user to reduce number of send operations
        user_updates: dict[str, list[dict[str, Any]]] = {}
        for block_uuid, block_data in msg.blocks.items():
            subs = subscribers_by_block.get(block_uuid)
            if not subs:
                logger.debug(f"No subscribers for block_uuid={block_uuid}")
                continue

            # Filter subscribers for private sandbox
            if isinstance(block_data, dict):
                parent_id = block_data.get("parent_id")
                if parent_id:
                    parent_sandbox = parent_sandbox_cache.get(str(parent_id))
                    if parent_sandbox and parent_sandbox.get("mode") == "private":
                        block_creator_id = block_data.get("creator_id")
                        container_owner_id = parent_sandbox.get("creator_id")
                        subs = connection_manager.filter_subscribers_for_private_sandbox(
                            subs,
                            block_creator_id,
                            container_owner_id
                        )
                        if not subs:
                            logger.debug(f"No authorized subscribers for private sandbox block {block_uuid}")
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
    redis: Redis,
    block_data: list[dict[str, Any]] | None = None
) -> None:
    """
    Send access update notification to user.

    For 'deny' permission, sends forbidden block data.
    For other permissions, uses block_data from message if available,
    otherwise fetches from Redis (fallback for backwards compatibility).
    """
    if permission == "deny":
        data_list = [{**settings.FORBIDDEN_BLOCK, 'id': block_id} for block_id in start_block_ids]
    elif block_data:
        # Use block data provided by backend (preferred path)
        data_list = block_data
        logger.debug(f"Using block_data from message for {len(data_list)} blocks")
    else:
        # Fallback: fetch from Redis (may be empty if block not cached)
        keys = [f'blockdata:{block_id}' for block_id in start_block_ids]
        pipe = redis.pipeline()
        for key in keys:
            pipe.hgetall(key)
        results = await pipe.execute()
        data_list = [parse_redis_block_data(data) for data in results if data]
        if not data_list:
            logger.warning(
                f"Access update ({permission}): block_data not in message and not in Redis. "
                f"User {user_id} won't receive block data for {start_block_ids}"
            )

    if data_list:
        response = BlockUpdateAccessResponse(
            start_block_ids=data_list,
            block_uuids=block_uuids,
            permission=permission
        )
        await connection_manager.send_personal_message(response.model_dump(), str(user_id))
        logger.info(f"Sent access update to user {user_id}: permission={permission}, blocks={len(data_list)}")
    else:
        logger.warning(f'No block data to send for access update to user {user_id}')


async def action_update_access(message_data: dict[str, Any]) -> None:
    """
    Process access permission update.

    For 'deny' permission: removes user from block subscribers.
    For other permissions (view, edit, edit_ac, delete): adds user to block subscribers.
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
                    logger.info(f"User {user_id} access '{msg.permission}' set for {len(chunk)} blocks.")
                await pipe.execute()

        await send_message_update_access(
            msg.start_block_ids,
            block_uuids,
            user_id,
            msg.permission,
            redis,
            block_data=msg.block_data
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

    Sends deletion notifications to all subscribers, then removes blocks
    from all user subscriptions and deletes block data from Redis.
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
            all_subscribers: set[str] = set()
            for block_uuid, user_ids in zip(chunk, results):
                all_subscribers.update(user_ids)
                for user_id in user_ids:
                    user_blocks[user_id].add(block_uuid)

            # Send deletion notifications to all subscribers BEFORE removing from Redis
            for block_uuid in chunk:
                response = BlockUpdateResponse(
                    block_uuid=block_uuid,
                    data={"id": block_uuid, "deleted": True}
                )
                await connection_manager.send_message_to_subscribers(
                    response.model_dump(),
                    all_subscribers
                )

            # Remove block_uuids from user subscriptions
            pipe = redis.pipeline(transaction=True)
            for user_id, blocks in user_blocks.items():
                pipe.srem(f"subscriber:{user_id}:blocks", *blocks)

            # Delete block, blockdata, and sandbox cache keys
            pipe.delete(*[f"block:{uuid}" for uuid in chunk])
            pipe.delete(*[f"blockdata:{uuid}" for uuid in chunk])
            pipe.delete(*[f"sandbox:{uuid}" for uuid in chunk])

            await pipe.execute()

        logger.info(f"Successfully unsubscribed and cleaned up {len(msg.block_uuids)} blocks")
    except Exception:
        logger.exception("Failed to process unsubscribe action")


async def action_sandbox_mode_changed(message_data: dict[str, Any]) -> None:
    """
    Process sandbox mode change notification.

    Updates the sandbox cache and notifies all subscribers about the mode change.
    This allows clients to react to sandbox mode transitions (none -> private, etc).
    """
    try:
        msg = SandboxModeChangedMessage(**message_data)
    except ValidationError as e:
        logger.error(f"Invalid sandbox_mode_changed message: {e}")
        return

    # Update sandbox cache
    await connection_manager.update_sandbox_cache(
        msg.block_uuid,
        msg.sandbox_mode,
        msg.creator_id
    )

    # Notify all subscribers about the mode change
    redis = await get_redis_pool()
    try:
        subscribers = await redis.smembers(f"block:{msg.block_uuid}")
        if not subscribers:
            logger.debug(f"No subscribers for sandbox_mode_changed block {msg.block_uuid}")
            return

        response = SandboxModeChangedResponse(
            block_uuid=msg.block_uuid,
            sandbox_mode=msg.sandbox_mode
        )
        await connection_manager.send_message_to_subscribers(
            response.model_dump(),
            subscribers
        )
        logger.info(
            f"Processed sandbox_mode_changed for block {msg.block_uuid}: "
            f"mode={msg.sandbox_mode}, notified {len(subscribers)} subscribers"
        )
    except Exception:
        logger.exception(f"Failed to notify subscribers for sandbox_mode_changed block {msg.block_uuid}")


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


# =============================================================================
# Chat Event Handler (from backend with action: 'chat_event')
# =============================================================================

async def action_chat_event(message_data: dict[str, Any]) -> None:
    """
    Process chat events from backend (action: 'chat_event').

    Handles three event types:
    - dm: Direct message to a single recipient
    - group_message: Message to all group members
    - group_update: Group membership/settings change notification
    """
    try:
        msg = ChatEventMessage(**message_data)
    except ValidationError as e:
        logger.error(f"Invalid chat_event message: {e}")
        return

    event_type = msg.type

    if event_type == "dm":
        # Direct message: send to recipient and sender (for multi-device sync)
        if not msg.recipient_id:
            logger.error("DM chat_event missing recipient_id")
            return

        recipient_id = str(msg.recipient_id)
        sender_id = str(msg.sender_id) if msg.sender_id else None

        # Add recipient_id to message object for frontend to identify chat context
        message_with_recipient = msg.message.copy() if msg.message else {}
        message_with_recipient["recipient_id"] = msg.recipient_id

        response = ChatEventResponse(
            event_type="dm",
            data={
                "sender_id": msg.sender_id,
                "recipient_id": msg.recipient_id,
                "message": message_with_recipient
            }
        )

        response_data = response.model_dump()

        # Send to recipient
        delivered_to_recipient = await connection_manager.send_to_user(recipient_id, response_data)

        # Send echo to sender (for multi-device sync)
        delivered_to_sender = False
        if sender_id and sender_id != recipient_id:
            delivered_to_sender = await connection_manager.send_to_user(sender_id, response_data)

        if delivered_to_recipient or delivered_to_sender:
            logger.info(f"Chat DM delivered: recipient={delivered_to_recipient}, sender_echo={delivered_to_sender}")
        else:
            logger.debug(f"Chat DM not delivered, both users offline")

    elif event_type == "group_message":
        # Group message: send to all members except sender
        if not msg.member_ids:
            logger.warning("group_message chat_event has no member_ids")
            return

        sender_id = str(msg.sender_id) if msg.sender_id else None
        member_ids = [str(uid) for uid in msg.member_ids]

        response = ChatEventResponse(
            event_type="group_message",
            data={
                "group_id": msg.group_id,
                "sender_id": msg.sender_id,
                "message": msg.message
            }
        )

        results = await connection_manager.send_to_users(
            member_ids,
            response.model_dump(),
            exclude_user=sender_id
        )

        delivered_count = sum(1 for delivered in results.values() if delivered)
        total_recipients = len(member_ids) - (1 if sender_id else 0)
        logger.info(
            f"Chat group_message for group {msg.group_id} delivered to "
            f"{delivered_count}/{total_recipients} members"
        )

    elif event_type == "group_update":
        # Group update: send to all members
        if not msg.member_ids:
            logger.warning("group_update chat_event has no member_ids")
            return

        member_ids = [str(uid) for uid in msg.member_ids]

        response = ChatEventResponse(
            event_type="group_update",
            data={
                "group_id": msg.group_id,
                "group_action": msg.group_action,
                "data": msg.data
            }
        )

        results = await connection_manager.send_to_users(member_ids, response.model_dump())

        delivered_count = sum(1 for delivered in results.values() if delivered)
        logger.info(
            f"Chat group_update '{msg.group_action}' for group {msg.group_id} "
            f"delivered to {delivered_count}/{len(member_ids)} members"
        )

    else:
        logger.warning(f"Unknown chat_event type: {event_type}")


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


# =============================================================================
# Access Request Event Handler (from backend with action: 'access_request')
# =============================================================================

async def action_access_request(message_data: dict[str, Any]) -> None:
    """
    Process access request events from backend (action: 'access_request').

    Handles two event types:
    - new_request: New access request created, notify block owner
    - response: Access request approved/denied, notify requester
    """
    try:
        msg = AccessRequestMessage(**message_data)
    except ValidationError as e:
        logger.error(f"Invalid access_request message: {e}")
        return

    request_type = msg.type

    if request_type == "new_request":
        # Send to block owner
        if not msg.owner_id:
            logger.error("new_request access_request missing owner_id")
            return

        target_user_id = str(msg.owner_id)

        response = AccessRequestResponse(
            type="new_request",
            request_id=msg.request_id,
            requester=msg.requester,
            block=msg.block,
            owner_id=msg.owner_id
        )

        delivered = await connection_manager.send_to_user(target_user_id, response.model_dump())
        if delivered:
            logger.info(f"Access request new_request delivered to owner {target_user_id}")
        else:
            logger.debug(f"Access request new_request not delivered, owner {target_user_id} offline")

    elif request_type == "response":
        # Send to requester
        if not msg.user_id:
            logger.error("response access_request missing user_id")
            return

        target_user_id = str(msg.user_id)

        response = AccessRequestResponse(
            type="response",
            request_id=msg.request_id,
            approved=msg.approved,
            permission=msg.permission,
            block=msg.block,
            user_id=msg.user_id
        )

        delivered = await connection_manager.send_to_user(target_user_id, response.model_dump())
        if delivered:
            logger.info(f"Access request response delivered to user {target_user_id}")
        else:
            logger.debug(f"Access request response not delivered, user {target_user_id} offline")


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
    metric_label = action or event_type or "unknown"

    start_time = time.monotonic()
    try:
        # Handle action-based messages
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
        elif action == 'sandbox_mode_changed':
            await action_sandbox_mode_changed(message_data)
        # Handle chat events from backend
        elif action == 'chat_event':
            await action_chat_event(message_data)
        # Handle access request events from backend
        elif action == 'access_request':
            await action_access_request(message_data)
        # Handle type-based messages (notification events)
        elif event_type in NOTIFICATION_EVENT_TYPES:
            await action_notification_event(message_data)
        else:
            logger.warning(f"Unknown message received: action={action}, type={event_type}")

        rabbitmq_messages_total.labels(action=metric_label).inc()
        await message.ack()
    except Exception:
        rabbitmq_message_errors_total.labels(action=metric_label).inc()
        logger.exception(f"Failed to process message with action '{action}'")
        await message.reject(requeue=False)
    finally:
        duration = time.monotonic() - start_time
        rabbitmq_message_duration_seconds.labels(action=metric_label).observe(duration)


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
            rabbitmq_reconnects_total.inc()
            logger.error(f"Lost connection to RabbitMQ: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            rabbitmq_reconnects_total.inc()
            logger.exception(f"Unexpected error in consumer loop: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)
