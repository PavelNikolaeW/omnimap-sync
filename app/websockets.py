# app/websockets.py
"""WebSocket endpoint for real-time block updates."""

import json
import logging
from typing import Any

import jwt
from fastapi import WebSocket, WebSocketDisconnect

from app.connection_manager import ConnectionManager
from app.config import settings
from app.metrics import ws_messages_received_total
from app.models import ErrorResponse, BlockUpdatesResponse
from app.redis_client import get_redis_pool
from app.utils import parse_redis_block_data
from app.auth import verify_jwt

logger = logging.getLogger("realtime_service")

# Known client actions for metric label sanitization (prevents cardinality explosion)
_KNOWN_WS_ACTIONS = {"ping", "get_updates"}

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
            blocks = data.get("blocks", [])
            ws_messages_received_total.labels(
                action=action if action in _KNOWN_WS_ACTIONS else "unknown"
            ).inc()

            if action == "ping":
                await websocket.send_json({"type": "pong"})
                # Refresh online status TTL on heartbeat
                await connection_manager.refresh_user_online_ttl(connection_id)
                logger.debug(f"Pong sent to {connection_id}")
            elif action == "get_updates":
                await handle_get_updates(websocket, connection_id, blocks)
            else:
                logger.warning(f"Connection {connection_id} sent unknown action: {action}")
                await websocket.send_json(ErrorResponse(message="Unknown action.").model_dump())

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
            raw_updated_at = block.get('updated_at', 0)
            try:
                valid_blocks_dict[block_id] = int(raw_updated_at)
            except (ValueError, TypeError):
                valid_blocks_dict[block_id] = 0
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

    logger.info(
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

                # Компенсируем safety margin клиента: клиент отправляет (timestamp - 1)
                # чтобы не пропустить апдейты в ту же секунду. Проверяем разницу > 1.
                # Если разница ровно 1, значит блок не изменился (safety margin).
                time_diff = redis_time - client_time

                if time_diff > 1:
                    parsed_data = parse_redis_block_data(redis_data)
                    if "updated_at" in parsed_data:
                        try:
                            parsed_data["updated_at"] = int(parsed_data["updated_at"])
                        except (ValueError, TypeError):
                            parsed_data["updated_at"] = 0
                    updated_blocks_data.append(parsed_data)
                    logger.debug(
                        f"Block {block_id} has update: redis_time={redis_time}, client_time={client_time}, diff={time_diff}"
                    )
                else:
                    logger.debug(
                        f"Block {block_id} is up to date: redis_time={redis_time}, client_time={client_time}, diff={time_diff}"
                    )
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"Error comparing updated_at for block {block_id}: {e}. "
                    f"redis_data.updated_at={redis_data.get('updated_at')!r}, "
                    f"client_time={valid_blocks_dict.get(block_id)!r}"
                )

    # Mark unsubscribed blocks as deleted
    for block_id in unsubscribed_blocks:
        updated_blocks_data.append({"id": block_id, "deleted": True})

    # Determine new blocks: subscribed but not in client's request
    client_block_ids = set(valid_blocks_dict.keys()) | set(unsubscribed_blocks)
    new_block_ids = list(subscribed_blocks - client_block_ids)

    # Load full data for new blocks from Redis
    new_blocks_data: list[dict[str, Any]] = []
    if new_block_ids:
        async with redis.pipeline(transaction=False) as pipe:
            for block_id in new_block_ids:
                pipe.hgetall(f"blockdata:{block_id}")
            new_blocks_redis = await pipe.execute()

        for block_id, redis_data in zip(new_block_ids, new_blocks_redis):
            if redis_data:
                try:
                    parsed_data = parse_redis_block_data(redis_data)
                    if "updated_at" in parsed_data:
                        try:
                            parsed_data["updated_at"] = int(parsed_data["updated_at"])
                        except (ValueError, TypeError):
                            parsed_data["updated_at"] = 0
                    new_blocks_data.append(parsed_data)
                except Exception as e:
                    logger.exception(f"Error parsing new block {block_id}: {e}")

    # Send response
    response = BlockUpdatesResponse(
        updates=updated_blocks_data,
        new_blocks=new_blocks_data
    )
    await websocket.send_json(response.model_dump())

    logger.info(
        f"Sent block updates to {connection_id}: {len(updated_blocks_data)} updated blocks, "
        f"{len(new_blocks_data)} new blocks, "
        f"({len(valid_blocks_dict)} subscribed, {len(unsubscribed_blocks)} deleted)"
    )
