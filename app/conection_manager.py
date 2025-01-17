import asyncio
import logging

from starlette.websockets import WebSocket

logger = logging.getLogger("realtime_service")


class ConnectionManager:
    """
    Класс для управления подключениями пользователей.
    """

    def __init__(self):
        self.active_connections: dict[str, list[WebSocket]] = {}
        self.lock = asyncio.Lock()

    async def connect(self, user_id: str, websocket: WebSocket):
        async with self.lock:
            if user_id not in self.active_connections:
                self.active_connections[user_id] = []
            self.active_connections[user_id].append(websocket)
        logger.info(f"User {user_id} connected. "
                    f"Total connections: {len(self.active_connections[user_id])}")

    async def disconnect(self, user_id: str, websocket: WebSocket):
        async with self.lock:
            if user_id not in self.active_connections:
                return

            connections = self.active_connections[user_id]
            try:
                connections.remove(websocket)
                logger.info(
                    f"User {user_id} disconnected a connection. "
                    f"Remaining connections: {len(connections)}"
                )
            except ValueError:
                logger.warning(f"WebSocket not found in connections for user {user_id}.")
            if not connections:
                del self.active_connections[user_id]
                logger.info(f"All connections for user {user_id} have been removed.")

    async def send_personal_message(self, message: dict, user_id: str):
        """
        Отправить сообщение конкретному пользователю (на все его подключения).
        Если сокет «мертв», удаляем его из списка.
        """
        async with self.lock:
            connections = list(self.active_connections.get(user_id, []))

        if not connections:
            logger.debug(f"No active connections for user {user_id} to send message.")
            return
        for connection in connections:
            try:
                await connection.send_json(message)
                logger.info(f"Sent message to user {user_id}; message details: {message['type']}")
            except Exception as e:
                logger.exception(f"Error sending message to user {user_id}: {e}")
                # Если отправка не удалась, удаляем проблемное соединение
                await self.disconnect(user_id, connection)
