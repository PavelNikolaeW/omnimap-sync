# app/websockets.py
"""WebSocket endpoint for real-time block updates."""

import json
import logging
from typing import Any

import jwt
from fastapi import WebSocket, WebSocketDisconnect

from app.connection_manager import ConnectionManager
from app.config import settings
from app.models import (
    ErrorResponse,
    BlockUpdatesResponse,
    DMTypingResponse,
    GroupTypingResponse,
    PresenceBatchResponse,
)
from app.redis_client import get_redis_pool
from app.utils import parse_redis_block_data
from app.auth import verify_jwt

logger = logging.getLogger("realtime_service")

connection_manager = ConnectionManager()


async def websocket_endpoint(websocket: WebSocket) -> None:
    """
    Main WebSocket endpoint for client connections.

    Authentication flow:
    1. Verify JWT with external auth service
    2. Decode and validate JWT locally
    3. Validate user_id exists in token
    4. Connect user and handle message loop
    """
    await websocket.accept()
    token = websocket.query_params.get("token")

    if not token:
        await websocket.send_json(ErrorResponse(message="Token is required.").model_dump())
        await websocket.close(code=1008)
        logger.warning("Connection attempt without token")
        return

    # Step 1: Verify JWT with external auth service
    is_valid = await verify_jwt(token)
    if not is_valid:
        await websocket.send_json(ErrorResponse(message="Token verification failed.").model_dump())
        await websocket.close(code=1008)
        logger.warning("Connection attempt with invalid token (auth service rejected)")
        return

    # Step 2: Decode and validate JWT locally
    try:
        jwt_data = jwt.decode(
            token,
            key=settings.jwt_secret_key,
            algorithms=settings.jwt_algorithms.split(','),
            options={"verify_signature": True}
        )
        user_id = jwt_data.get('user_id')
    except jwt.ExpiredSignatureError:
        await websocket.send_json(ErrorResponse(message="Token has expired.").model_dump())
        await websocket.close(code=1008)
        logger.warning("Connection attempt with expired token")
        return
    except jwt.InvalidTokenError as e:
        await websocket.send_json(ErrorResponse(message="Invalid token.").model_dump())
        await websocket.close(code=1008)
        logger.warning(f"Connection attempt with invalid token: {e}")
        return

    # Step 3: Validate user_id exists in token
    if not user_id:
        await websocket.send_json(ErrorResponse(message="user_id is missing in token.").model_dump())
        await websocket.close(code=1008)
        logger.warning("Connection attempt with token missing user_id")
        return

    connection_id = str(user_id)
    await connection_manager.connect(connection_id, websocket)
    logger.info(f"User {connection_id} authenticated and connected")

    try:
        while True:
            message: str | None = None
            try:
                message = await websocket.receive_text()
            except WebSocketDisconnect:
                logger.info(f"Connection {connection_id} disconnected (WebSocketDisconnect)")
                break
            except Exception:
                logger.exception(f"Error receiving message from {connection_id}")
                break

            if message is None:
                continue

            # Parse incoming JSON message
            try:
                data: dict[str, Any] = json.loads(message)
            except json.JSONDecodeError:
                logger.warning(f"Received invalid JSON from {connection_id}")
                await websocket.send_json(ErrorResponse(message="Invalid JSON format.").model_dump())
                continue

            action = data.get("action")
            msg_type = data.get("type")
            blocks = data.get("blocks", [])

            # Handle action-based messages
            if action == "ping":
                await websocket.send_json({"type": "pong"})
                logger.debug(f"Pong sent to {connection_id}")
            elif action == "get_updates":
                await handle_get_updates(websocket, connection_id, blocks)
            # Handle type-based messages (chat)
            elif msg_type == "chat_subscribe":
                await handle_chat_subscribe(websocket, connection_id)
            elif msg_type == "dm_typing":
                await handle_dm_typing(connection_id, data)
            elif msg_type == "group_typing":
                await handle_group_typing(connection_id, data)
            elif msg_type == "presence_request":
                await handle_presence_request(websocket, data)
            elif action or msg_type:
                logger.warning(f"Connection {connection_id} sent unknown action/type: action={action}, type={msg_type}")
                await websocket.send_json(ErrorResponse(message="Unknown action or type.").model_dump())
            else:
                await websocket.send_json(ErrorResponse(message="Missing action or type field.").model_dump())

    finally:
        await connection_manager.disconnect(connection_id, websocket)


async def handle_get_updates(
    websocket: WebSocket,
    connection_id: str,
    blocks: list[dict[str, Any]]
) -> None:
    """
    Handle client request for block updates.

    Validates user subscription and returns blocks that have been
    updated since the client's last known timestamp.
    """
    logger.debug(f'Get updates request from {connection_id}: blocks count = {len(blocks)}')

    # Validate blocks is a list
    if not isinstance(blocks, list):
        await websocket.send_json(ErrorResponse(message="`blocks` must be a list.").model_dump())
        logger.warning(f"Invalid blocks format from {connection_id}")
        return

    redis = await get_redis_pool()

    # Get user's subscribed block IDs from Redis
    subscriber_key = f"subscriber:{connection_id}:blocks"
    subscribed_blocks: set[str] = await redis.smembers(subscriber_key)

    if not subscribed_blocks:
        response = BlockUpdatesResponse(updates=[])
        await websocket.send_json(response.model_dump())
        logger.info(f"No subscribed blocks found for user {connection_id}")
        return

    # Filter input blocks: keep only subscribed ones, track client's updated_at
    valid_blocks_dict: dict[str, int] = {}  # subscribed blocks
    unsubscribed_blocks: list[str] = []  # not subscribed

    for block in blocks:
        block_id = block.get('id')
        if block_id in subscribed_blocks:
            valid_blocks_dict[block_id] = block.get('updated_at', 0)
        elif block_id:
            unsubscribed_blocks.append(block_id)

    if not valid_blocks_dict:
        response = BlockUpdatesResponse(updates=[])
        await websocket.send_json(response.model_dump())
        logger.info(f"User {connection_id} requested updates for blocks not subscribed.")
        return

    # Batch load from Redis using chunks
    block_portion = settings.block_portion
    valid_block_ids = list(valid_blocks_dict.keys())
    total_ids = len(valid_block_ids)

    logger.debug(
        f"User {connection_id} is requesting updates for {total_ids} subscribed blocks. "
        f"Using chunk size = {block_portion}"
    )

    updated_blocks_data: list[dict[str, Any]] = []

    for start_idx in range(0, total_ids, block_portion):
        chunk_block_ids = valid_block_ids[start_idx: start_idx + block_portion]

        # Create pipeline for chunk
        async with redis.pipeline(transaction=False) as pipe:
            for block_id in chunk_block_ids:
                pipe.hgetall(f"blockdata:{block_id}")
            redis_results = await pipe.execute()

        # Process chunk results
        for block_id, redis_data in zip(chunk_block_ids, redis_results):
            if not redis_data:
                continue

            try:
                redis_time = int(redis_data.get("updated_at", 0))
                client_time = valid_blocks_dict[block_id]
                if redis_time > client_time:
                    parsed_data = parse_redis_block_data(redis_data)
                    updated_blocks_data.append(parsed_data)
            except ValueError as e:
                logger.exception(f"Error parsing updated_at for block {block_id}: {e}")

    # Mark unsubscribed blocks as deleted
    for block_id in unsubscribed_blocks:
        updated_blocks_data.append({"id": block_id, "deleted": True})

    # Send response
    response = BlockUpdatesResponse(updates=updated_blocks_data)
    await websocket.send_json(response.model_dump())

    logger.info(
        f"Sent block updates to {connection_id}: {len(updated_blocks_data)} total blocks "
        f"({len(valid_blocks_dict)} subscribed, {len(unsubscribed_blocks)} deleted)"
    )


# =============================================================================
# Chat Message Handlers
# =============================================================================

async def handle_chat_subscribe(websocket: WebSocket, connection_id: str) -> None:
    """
    Handle chat subscription request.

    Acknowledges the subscription and updates presence.
    """
    connection_manager.update_presence(connection_id)
    await websocket.send_json({"type": "chat_subscribed", "status": "ok"})
    logger.info(f"User {connection_id} subscribed to chat")


async def handle_dm_typing(sender_id: str, data: dict[str, Any]) -> None:
    """
    Handle DM typing indicator.

    Forwards the typing status to the recipient.
    """
    recipient_id = data.get("recipient_id")
    is_typing = data.get("is_typing", False)

    if not recipient_id:
        logger.warning(f"DM typing from {sender_id} missing recipient_id")
        return

    recipient_id = str(recipient_id)

    # Get sender username from Redis or use ID
    redis = await get_redis_pool()
    username = await redis.get(f"user:{sender_id}:username")

    response = DMTypingResponse(
        user_id=sender_id,
        username=username,
        is_typing=is_typing
    )

    delivered = await connection_manager.send_to_user(recipient_id, response.model_dump())

    if delivered:
        logger.debug(f"DM typing indicator from {sender_id} sent to {recipient_id}")
    else:
        logger.debug(f"DM typing indicator not delivered, user {recipient_id} is offline")


async def handle_group_typing(sender_id: str, data: dict[str, Any]) -> None:
    """
    Handle group typing indicator.

    Forwards the typing status to all group members.
    """
    group_id = data.get("group_id")
    is_typing = data.get("is_typing", False)

    if not group_id:
        logger.warning(f"Group typing from {sender_id} missing group_id")
        return

    # Get group members from Redis cache
    redis = await get_redis_pool()
    member_ids_raw = await redis.smembers(f"group:{group_id}:members")

    if not member_ids_raw:
        logger.debug(f"No cached members for group {group_id}")
        return

    member_ids = [str(uid) for uid in member_ids_raw]

    # Get sender username from Redis
    username = await redis.get(f"user:{sender_id}:username")

    response = GroupTypingResponse(
        group_id=group_id,
        user_id=sender_id,
        username=username,
        is_typing=is_typing
    )

    results = await connection_manager.broadcast_to_group(
        member_ids,
        response.model_dump(),
        exclude_user=sender_id
    )

    delivered_count = sum(1 for delivered in results.values() if delivered)
    logger.debug(
        f"Group typing indicator from {sender_id} for group {group_id} "
        f"sent to {delivered_count} members"
    )


async def handle_presence_request(websocket: WebSocket, data: dict[str, Any]) -> None:
    """
    Handle presence status request.

    Returns online status for the requested users.
    """
    user_ids = data.get("user_ids", [])

    if not isinstance(user_ids, list):
        await websocket.send_json(ErrorResponse(message="user_ids must be a list").model_dump())
        return

    # Convert to strings
    user_ids = [str(uid) for uid in user_ids]

    # Get presence info
    presence_data = connection_manager.get_presence_batch(user_ids)

    response = PresenceBatchResponse(users=presence_data)
    await websocket.send_json(response.model_dump())

    logger.debug(f"Sent presence info for {len(user_ids)} users")
