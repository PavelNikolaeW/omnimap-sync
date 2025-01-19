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
        logger.info(f"User {user_id} connected. Total connections: {len(self.active_connections[user_id])}")

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

    async def send_message_to_socket(self, message: dict, user_id: str, websocket: WebSocket):
        """
        Отправка сообщения конкретному соединению.
        """
        try:
            await websocket.send_json(message)
            logger.info(f"Sent message to user {user_id}; message details: {message.get('type')}")
        except Exception as e:
            logger.exception(f"Error sending message to user {user_id}: {e}")
            # Если отправка не удалась, удаляем проблемное соединение
            await self.disconnect(user_id, websocket)

    async def send_personal_message(self, message: dict, user_id: str):
        """
        Отправить сообщение конкретному пользователю (на все его подключения)
        последовательно.
        """
        async with self.lock:
            connections = list(self.active_connections.get(user_id, []))
        if not connections:
            logger.debug(f"No active connections for user {user_id} to send message.")
            return
        for websocket in connections:
            await self.send_message_to_socket(message, user_id, websocket)

    def get_tasks_send_message(self, message, subscribers) -> list[asyncio.Task]:
        """
        Подготовить задачи для отправки сообщений по каждому соединению пользователя.

        :param messages: Список кортежей, где каждый кортеж имеет вид (user_id, message)
        :return: Список asyncio.Task, готовых к выполнению.
        """
        tasks = []
        for connection_id in subscribers:
            # Получаем список подключений для конкретного пользователя
            # Важно: доступ к словарю активных подключений осуществляется под блокировкой.
            async def create_tasks_for_user(u_id: str, msg: dict):
                async with self.lock:
                    connections = list(self.active_connections.get(u_id, []))
                # Для каждого соединения создаём отдельную задачу
                inner_tasks = [
                    asyncio.create_task(self.send_message_to_socket(msg, u_id, websocket))
                    for websocket in connections
                ]
                return inner_tasks

            # Для каждого пользователя создаём вложенную корутину, которая вернёт список задач.
            # Здесь используем asyncio.create_task, чтобы запланировать выполнение.
            task = asyncio.create_task(create_tasks_for_user(connection_id, message))
            # После выполнения задачи (которая возвращает список задач для данного пользователя)
            # добавляем их в общий список.
            tasks.append(task)

        # Дополнительный шаг: преобразуем список задач, возвращающих списки, в единый список
        async def gather_all():
            results = await asyncio.gather(*tasks)
            # results – это список списков задач, объединяем их
            all_inner_tasks = [inner_task for sublist in results for inner_task in sublist]
            # Выполняем все созданные задачи по отправке сообщений
            if all_inner_tasks:
                await asyncio.gather(*all_inner_tasks)

        # Возвращаем задачу, которая сначала соберет задачи для каждого пользователя, а затем выполнит их
        return [asyncio.create_task(gather_all())]
