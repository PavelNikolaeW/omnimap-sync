# app/websockets.py
"""WebSocket endpoint for real-time block updates."""

import asyncio
import json
import logging
import time
from typing import Any

import jwt
from fastapi import WebSocket, WebSocketDisconnect

from app.connection_manager import ConnectionManager
from app.config import settings
from app.metrics import (
    ws_messages_received_total,
    ws_get_updates_requests_total,
    ws_get_updates_requested_blocks,
    ws_get_updates_response_updates,
    ws_get_updates_response_new_blocks,
    ws_get_updates_redis_ops_total,
    ws_get_updates_duration_seconds,
)
from app.models import ErrorResponse, BlockUpdatesResponse, BlockUpdatesV2Response
from app.redis_client import get_redis_pool
from app.utils import parse_redis_block_data
from app.auth import verify_jwt

logger = logging.getLogger("realtime_service")

# Known client actions for metric label sanitization (prevents cardinality explosion)
_KNOWN_WS_ACTIONS = {"ping", "get_updates", "get_updates_v2"}

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
                message = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=settings.WS_IDLE_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
                logger.info(
                    "Connection %s idle timeout (%ss), closing socket",
                    connection_id,
                    settings.WS_IDLE_TIMEOUT_SECONDS,
                )
                await websocket.close(code=1001, reason="idle_timeout")
                break
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
            elif action == "get_updates_v2":
                if not settings.SYNC_V2_ENABLED:
                    await websocket.send_json(ErrorResponse(message="get_updates_v2 is disabled.").model_dump())
                    continue

                try:
                    cursor = int(data.get("cursor", 0) or 0)
                except (TypeError, ValueError):
                    cursor = 0
                try:
                    subscription_version = int(data.get("subscription_version", 0) or 0)
                except (TypeError, ValueError):
                    subscription_version = 0
                try:
                    limit = int(data.get("limit", settings.GET_UPDATES_V2_DEFAULT_LIMIT) or settings.GET_UPDATES_V2_DEFAULT_LIMIT)
                except (TypeError, ValueError):
                    limit = settings.GET_UPDATES_V2_DEFAULT_LIMIT

                await handle_get_updates_v2(
                    websocket=websocket,
                    connection_id=connection_id,
                    cursor=max(0, cursor),
                    subscription_version=max(0, subscription_version),
                    limit=limit,
                )
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
    version = "v1"
    started_at = time.monotonic()
    result = "ok"
    updates_count = 0
    new_blocks_count = 0
    requested_blocks_count = len(blocks) if isinstance(blocks, list) else 0

    ws_get_updates_requested_blocks.labels(version=version).observe(requested_blocks_count)
    logger.debug(f'Get updates request from {connection_id}: blocks count = {requested_blocks_count}')

    try:
        # Validate blocks is a list
        if not isinstance(blocks, list):
            result = "invalid_payload"
            await websocket.send_json(ErrorResponse(message="`blocks` must be a list.").model_dump())
            logger.warning(f"Invalid blocks format from {connection_id}")
            return

        redis = await get_redis_pool()

        # Get user's subscribed block IDs from Redis
        subscriber_key = f"subscriber:{connection_id}:blocks"
        ws_get_updates_redis_ops_total.labels(version=version, op="smembers").inc()
        subscribed_blocks: set[str] = await redis.smembers(subscriber_key)

        if not subscribed_blocks:
            result = "no_subscriptions"
            response = BlockUpdatesResponse(
                updates=[],
                full_resync_required=True,
                reason="no_subscriptions",
            )
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
            result = "no_valid_blocks"
            response = BlockUpdatesResponse(
                updates=[],
                full_resync_required=True,
                reason="no_valid_blocks",
            )
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
            ws_get_updates_redis_ops_total.labels(version=version, op="hgetall").inc(len(chunk_block_ids))
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
            ws_get_updates_redis_ops_total.labels(version=version, op="hgetall").inc(len(new_block_ids))
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

        updates_count = len(updated_blocks_data)
        new_blocks_count = len(new_blocks_data)

        # Send response
        response = BlockUpdatesResponse(
            updates=updated_blocks_data,
            new_blocks=new_blocks_data
        )
        await websocket.send_json(response.model_dump())

        logger.info(
            f"Sent block updates to {connection_id}: {updates_count} updated blocks, "
            f"{new_blocks_count} new blocks, "
            f"({len(valid_blocks_dict)} subscribed, {len(unsubscribed_blocks)} deleted)"
        )
    except Exception:
        result = "error"
        raise
    finally:
        elapsed = time.monotonic() - started_at
        ws_get_updates_requests_total.labels(version=version, result=result).inc()
        ws_get_updates_response_updates.labels(version=version).observe(updates_count)
        ws_get_updates_response_new_blocks.labels(version=version).observe(new_blocks_count)
        ws_get_updates_duration_seconds.labels(version=version, result=result).observe(elapsed)


def _decode_changelog_member(member: str) -> tuple[str, str] | None:
    """
    Parse changelog member in format '<seq>:<op>:<block_id>'.
    Returns (op, block_id).
    """
    try:
        _, op, block_id = member.split(":", 2)
    except ValueError:
        return None

    if op not in {"upsert", "delete"} or not block_id:
        return None
    return op, block_id


async def handle_get_updates_v2(
    websocket: WebSocket,
    connection_id: str,
    cursor: int,
    subscription_version: int,
    limit: int,
) -> None:
    """
    Cursor-based incremental sync.

    Reads global changelog and returns only relevant subscribed block updates.
    If subscription version changed or cursor is stale, requests full resync.
    """
    version = "v2"
    started_at = time.monotonic()
    result = "ok"
    updates_count = 0
    safe_limit = max(1, min(limit, settings.GET_UPDATES_V2_MAX_LIMIT))

    # v2 does not send list of blocks; observe zero to keep metric shape stable.
    ws_get_updates_requested_blocks.labels(version=version).observe(0)

    try:
        redis = await get_redis_pool()

        # Validate subscription version
        subver_key = f"{settings.SUBSCRIPTION_VERSION_KEY_PREFIX}{connection_id}"
        ws_get_updates_redis_ops_total.labels(version=version, op="get").inc()
        server_subver_raw = await redis.get(subver_key)
        try:
            server_subscription_version = int(server_subver_raw or 0)
        except (TypeError, ValueError):
            server_subscription_version = 0

        if subscription_version != server_subscription_version:
            result = "subver_mismatch"
            ws_get_updates_redis_ops_total.labels(version=version, op="get").inc()
            seq_raw = await redis.get(settings.CHANGELOG_SEQ_KEY)
            next_cursor = int(seq_raw or 0)
            response = BlockUpdatesV2Response(
                updates=[],
                next_cursor=next_cursor,
                has_more=False,
                subscription_version=server_subscription_version,
                full_resync_required=True,
                reason="subscription_version_mismatch",
            )
            await websocket.send_json(response.model_dump())
            return

        subscriber_key = f"subscriber:{connection_id}:blocks"
        ws_get_updates_redis_ops_total.labels(version=version, op="scard").inc()
        subscribed_count = await redis.scard(subscriber_key)
        if subscribed_count == 0:
            result = "no_subscriptions"
            ws_get_updates_redis_ops_total.labels(version=version, op="get").inc()
            seq_raw = await redis.get(settings.CHANGELOG_SEQ_KEY)
            next_cursor = int(seq_raw or 0)
            response = BlockUpdatesV2Response(
                updates=[],
                next_cursor=next_cursor,
                has_more=False,
                subscription_version=server_subscription_version,
                full_resync_required=True,
                reason="no_subscriptions",
            )
            await websocket.send_json(response.model_dump())
            return

        # Validate cursor against changelog retention
        ws_get_updates_redis_ops_total.labels(version=version, op="zrange").inc()
        min_entry = await redis.zrange(settings.CHANGELOG_KEY, 0, 0, withscores=True)
        if min_entry:
            min_seq = int(min_entry[0][1])
            if cursor < (min_seq - 1):
                result = "stale_cursor"
                ws_get_updates_redis_ops_total.labels(version=version, op="get").inc()
                seq_raw = await redis.get(settings.CHANGELOG_SEQ_KEY)
                next_cursor = int(seq_raw or 0)
                response = BlockUpdatesV2Response(
                    updates=[],
                    next_cursor=next_cursor,
                    has_more=False,
                    subscription_version=server_subscription_version,
                    full_resync_required=True,
                    reason="stale_cursor",
                )
                await websocket.send_json(response.model_dump())
                return

        # Read changelog page after cursor
        ws_get_updates_redis_ops_total.labels(version=version, op="zrangebyscore").inc()
        entries: list[tuple[str, float]] = await redis.zrangebyscore(
            settings.CHANGELOG_KEY,
            min=f"({cursor}",
            max="+inf",
            start=0,
            num=safe_limit,
            withscores=True,
        )

        if not entries:
            response = BlockUpdatesV2Response(
                updates=[],
                next_cursor=cursor,
                has_more=False,
                subscription_version=server_subscription_version,
                full_resync_required=False,
            )
            await websocket.send_json(response.model_dump())
            return

        # Last event per block wins within this page
        latest_by_block: dict[str, tuple[int, str]] = {}
        max_seq = cursor
        for member, score in entries:
            parsed = _decode_changelog_member(member)
            if not parsed:
                continue
            op, block_id = parsed
            seq = int(score)
            max_seq = max(max_seq, seq)
            prev = latest_by_block.get(block_id)
            if prev is None or seq > prev[0]:
                latest_by_block[block_id] = (seq, op)

        if not latest_by_block:
            response = BlockUpdatesV2Response(
                updates=[],
                next_cursor=max_seq,
                has_more=False,
                subscription_version=server_subscription_version,
                full_resync_required=False,
            )
            await websocket.send_json(response.model_dump())
            return

        # Keep only currently subscribed blocks
        block_ids = list(latest_by_block.keys())
        ws_get_updates_redis_ops_total.labels(version=version, op="sismember").inc(len(block_ids))
        async with redis.pipeline(transaction=False) as pipe:
            for block_id in block_ids:
                pipe.sismember(subscriber_key, block_id)
            subscribed_results = await pipe.execute()

        is_subscribed_by_block = {
            block_id: bool(is_subscribed)
            for block_id, is_subscribed in zip(block_ids, subscribed_results)
        }

        relevant_events = [
            (seq, block_id, op)
            for block_id, (seq, op) in latest_by_block.items()
            if is_subscribed_by_block.get(block_id, False)
        ]
        relevant_events.sort(key=lambda item: item[0])

        updates: list[dict[str, Any]] = []
        upsert_ids = [block_id for _, block_id, op in relevant_events if op == "upsert"]
        if upsert_ids:
            ws_get_updates_redis_ops_total.labels(version=version, op="hgetall").inc(len(upsert_ids))
            async with redis.pipeline(transaction=False) as pipe:
                for block_id in upsert_ids:
                    pipe.hgetall(f"blockdata:{block_id}")
                upsert_rows = await pipe.execute()

            for block_id, redis_data in zip(upsert_ids, upsert_rows):
                if not redis_data:
                    updates.append({"id": block_id, "deleted": True})
                    continue

                parsed_data = parse_redis_block_data(redis_data)
                if "updated_at" in parsed_data:
                    try:
                        parsed_data["updated_at"] = int(parsed_data["updated_at"])
                    except (ValueError, TypeError):
                        parsed_data["updated_at"] = 0
                updates.append(parsed_data)

        for _, block_id, op in relevant_events:
            if op == "delete":
                updates.append({"id": block_id, "deleted": True})

        updates_count = len(updates)

        ws_get_updates_redis_ops_total.labels(version=version, op="zcount").inc()
        remaining_count = await redis.zcount(settings.CHANGELOG_KEY, min=f"({max_seq}", max="+inf")
        has_more = remaining_count > 0

        response = BlockUpdatesV2Response(
            updates=updates,
            next_cursor=max_seq,
            has_more=has_more,
            subscription_version=server_subscription_version,
            full_resync_required=False,
        )
        await websocket.send_json(response.model_dump())
    except Exception:
        result = "error"
        raise
    finally:
        elapsed = time.monotonic() - started_at
        ws_get_updates_requests_total.labels(version=version, result=result).inc()
        ws_get_updates_response_updates.labels(version=version).observe(updates_count)
        ws_get_updates_response_new_blocks.labels(version=version).observe(0)
        ws_get_updates_duration_seconds.labels(version=version, result=result).observe(elapsed)
