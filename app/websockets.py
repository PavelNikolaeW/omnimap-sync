# app/websockets.py

import json
import logging
import jwt

from fastapi import WebSocket, WebSocketDisconnect

from app.conection_manager import ConnectionManager
from app.config import settings
from app.redis_client import get_redis_pool
from app.auth import verify_jwt

logger = logging.getLogger("realtime_service")

connection_manager = ConnectionManager()


async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    token = websocket.query_params.get("token")
    if not token:
        await websocket.send_json({"type": "error", "message": "Token is required."})
        await websocket.close(code=1008)
        logger.warning("Connection attempt without token")
        return

    # Проверяем JWT
    is_valid = await verify_jwt(token)

    try:
        # Если подпись не нужно проверять, options={"verify_signature": False}
        jwt_data = jwt.decode(
            token,
            key=settings.jwt_secret_key,
            algorithms=settings.jwt_algorithms.split(','),
            options={"verify_signature": False}
        )
        user_id = jwt_data.get('user_id')
    except Exception as e:
        await websocket.send_json({"type": "error", "message": "Invalid token data."})
        user_id = settings.ANONIM_USER

    if not user_id:
        await websocket.send_json({"type": "error", "message": "user_id is missing in token."})
        await websocket.close(code=1008)
        return

    connection_id = str(user_id)
    await connection_manager.connect(connection_id, websocket)

    try:
        while True:
            message = None
            try:
                message = await websocket.receive_text()
            except WebSocketDisconnect:
                logger.info(f"Connection {connection_id} disconnected (WebSocketDisconnect)")
                break
            except Exception as e:
                logger.exception(f"Error receiving message from {connection_id}")
                break

            if message is None:
                continue

            # Разбираем входящую строку как JSON
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                logger.warning(f"Received invalid JSON from {connection_id}")
                await websocket.send_json({"type": "error", "message": "Invalid JSON format."})
                continue

            action = data.get("action")
            blocks = data.get("blocks", [])
            if action == "get_updates":
                await handle_get_updates(websocket, connection_id, blocks)
            else:
                logger.warning(f"Connection {connection_id} sent unknown action: {action}")
                await websocket.send_json({"type": "error", "message": "Unknown action."})

    finally:
        # В любом случае (исключение или разрыв) отключаем
        await connection_manager.disconnect(connection_id, websocket)


async def handle_get_updates(websocket: WebSocket, connection_id: str, blocks: list):
    """
    Обрабатывает запрос клиента на получение обновлений для списка блоков.
    Использует ключ subscriber:{user_id}:blocks для валидации подписки.
    """
    logger.debug(f'Get updates request from {connection_id}: blocks count = {len(blocks)}')

    # 1. Проверяем, что blocks — это список
    if not isinstance(blocks, list):
        await websocket.send_json({
            "type": "error",
            "message": "`blocks` must be a list."
        })
        logger.warning(f"Invalid blocks format from {connection_id}")
        return

    redis = await get_redis_pool()

    # 2. Получаем из Redis идентификаторы блоков, на которые подписан пользователь
    subscriber_key = f"subscriber:{connection_id}:blocks"
    subscribed_blocks = await redis.smembers(subscriber_key)
    if not subscribed_blocks:
        await websocket.send_json({"type": "block_updates", "updates": []})
        logger.info(f"No subscribed blocks found for user {connection_id}")
        return

    # 3. Фильтруем входной список blocks, оставляем только те, на которые подписан пользователь
    #    и сразу сохраним клиентское updated_at для каждого блока
    valid_blocks_dict = {}
    for block in blocks:
        block_id = block.get('id')
        if block_id in subscribed_blocks:
            valid_blocks_dict[block_id] = block.get('updated_at', 0)

    if not valid_blocks_dict:
        await websocket.send_json({"type": "block_updates", "updates": []})
        logger.info(f"User {connection_id} requested updates for blocks not subscribed.")
        return

    # 4. Готовимся к пакетной (chunk) загрузке из Redis
    block_portion = settings.block_portion  # читаем размер чанка из настроек
    valid_block_ids = list(valid_blocks_dict.keys())
    total_ids = len(valid_block_ids)

    logger.debug(
        f"User {connection_id} is requesting updates for {total_ids} blocks. "
        f"Using chunk size = {block_portion}"
    )

    updated_blocks_data = []

    for start_idx in range(0, total_ids, block_portion):
        chunk_block_ids = valid_block_ids[start_idx: start_idx + block_portion]

        # Создаём pipeline для чанка
        async with redis.pipeline(transaction=False) as pipe:
            for block_id in chunk_block_ids:
                pipe.hgetall(f"blockdata:{block_id}")
            redis_results = await pipe.execute()

        # 6. Обрабатываем результаты этого чанка
        for block_id, redis_data in zip(chunk_block_ids, redis_results):
            # Если блок в Redis не найден, пропускаем
            if not redis_data:
                continue

            try:
                redis_time = int(redis_data.get("updated_at", 0))
                client_time = valid_blocks_dict[block_id]
                if redis_time > client_time:
                    decoded_data = {
                        k: v
                        for k, v in redis_data.items()
                    }
                    updated_blocks_data.append(decoded_data)
            except ValueError as e:
                logger.exception(f"Error parsing updated_at for block {block_id}: {e}")

    # 7. Формируем ответ
    response = {
        "type": "block_updates",
        "updates": updated_blocks_data
    }
    await websocket.send_json(response)
    logger.info(
        f"Sent block updates to {connection_id}: {len(updated_blocks_data)} updated blocks "
        f"out of {len(valid_blocks_dict)} requested."
    )
