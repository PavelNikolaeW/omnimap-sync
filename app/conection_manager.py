import asyncio
import logging
from collections import defaultdict

from starlette.websockets import WebSocket

from app.config import settings

logger = logging.getLogger("realtime_service")


class ConnectionManager:
    """
    Класс для управления подключениями пользователей.
    Учитывает, что у анонимного пользователя может быть очень много подключений,
    поэтому для него используем семафор при отправке сообщений.
    """

    def __init__(self):
        # Словарь: user_id -> список WebSocket-соединений
        self.active_connections: dict[str, list[WebSocket]] = defaultdict(list)
        # Словарь: user_id -> asyncio.Lock, чтобы разграничить операции у конкретного пользователя
        self.user_locks: dict[str, asyncio.Lock] = {}
        # Глобальный лок используется только для создания/удаления записей в user_locks
        self.global_lock = asyncio.Lock()
        # Семафор для анонимного пользователя, чтобы ограничивать число одновременных отправок
        self.anon_semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_SENDS)

    async def _get_user_lock(self, user_id: str) -> asyncio.Lock:
        """
        Возвращает лок для конкретного пользователя, создаёт его при необходимости
        под защитой глобального лока.
        """
        async with self.global_lock:
            if user_id not in self.user_locks:
                self.user_locks[user_id] = asyncio.Lock()
            return self.user_locks[user_id]

    async def connect(self, user_id: str, websocket: WebSocket):
        """
        Добавление нового соединения для пользователя.
        """
        user_lock = await self._get_user_lock(user_id)
        async with user_lock:
            self.active_connections[user_id].append(websocket)
        logger.info(
            f"User {user_id} connected. "
            f"Total connections: {len(self.active_connections[user_id])}"
        )

    async def disconnect(self, user_id: str, websocket: WebSocket):
        """
        Удаление соединения пользователя при отключении.
        """
        # Получаем блокировку конкретного пользователя
        user_lock = await self._get_user_lock(user_id)
        async with user_lock:
            connections = self.active_connections.get(user_id, [])
            if websocket not in connections:
                logger.warning(f"WebSocket not found in connections for user {user_id}.")
                return

            connections.remove(websocket)
            logger.info(
                f"User {user_id} disconnected a connection. "
                f"Remaining connections: {len(connections)}"
            )

            # Если соединений не осталось — удаляем запись
            if not connections:
                del self.active_connections[user_id]
                # Удаляем блокировку под защитой глобального лока, чтобы избежать состояния гонки
                async with self.global_lock:
                    if user_id in self.user_locks:
                        del self.user_locks[user_id]
                logger.info(f"All connections for user {user_id} have been removed.")

    async def send_message_to_socket(self, message: dict, user_id: str, websocket: WebSocket):
        """
        Отправка сообщения конкретному WebSocket-соединению.
        Если отправка не удалась, соединение удаляется.
        """
        try:
            await websocket.send_json(message)
            logger.debug(f"Sent message to user {user_id}; message details: {message.get('type')}")
        except Exception as e:
            logger.exception(f"Error sending message to user {user_id}: {e}")
            await self.disconnect(user_id, websocket)

    async def send_personal_message(self, message: dict, user_id: str):
        """
        Отправка сообщения конкретному пользователю (на все его соединения).
        Для анонимного пользователя используем семафор, чтобы не отправлять
        сразу на сотни/тысячи соединений одновременно.
        """
        user_lock = await self._get_user_lock(user_id)
        # Читаем копию списка соединений под блокировкой, чтобы избежать конфликтов
        async with user_lock:
            connections = list(self.active_connections.get(user_id, []))
        if not connections:
            logger.debug(f"No active connections for user {user_id} to send message.")
            return

        if user_id == settings.ANONIM_USER:
            async def sem_task(ws: WebSocket):
                async with self.anon_semaphore:
                    await self.send_message_to_socket(message, user_id, ws)

            await asyncio.gather(*(sem_task(ws) for ws in connections), return_exceptions=True)
        else:
            await asyncio.gather(
                *(self.send_message_to_socket(message, user_id, ws) for ws in connections),
                return_exceptions=True
            )

    async def send_message_to_subscribers(self, message: dict, subscribers: list[str]):
        """
        Асинхронная отправка сообщения группе пользователей.
        Запускает фоновую задачу, чтобы не блокировать вызывающий код.
        """
        asyncio.create_task(self._send_message_to_subscribers_impl(message, subscribers))

    async def _send_message_to_subscribers_impl(self, message: dict, subscribers: list[str]):
        """
        Фактическая реализация массовой отправки.
        """
        if not subscribers:
            return
        tasks = [
            asyncio.create_task(self.send_personal_message(message, user_id))
            for user_id in subscribers
        ]
        await asyncio.gather(*tasks, return_exceptions=True)