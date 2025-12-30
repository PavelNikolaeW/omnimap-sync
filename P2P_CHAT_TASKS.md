# Задачи для omnimap-sync: P2P и групповые чаты WebSocket

## Описание

Расширить WebSocket сервис для поддержки real-time личных и групповых сообщений.

**Правило доступа:** Сообщения пересылаются только между пользователями с общими блоками.

## Новые типы WS сообщений

### От клиента → серверу

```json
// Подписаться на DM и группы при подключении
{
    "type": "chat_subscribe"
}

// Индикатор набора текста (DM)
{
    "type": "dm_typing",
    "recipient_id": "uuid",
    "is_typing": true
}

// Индикатор набора текста (группа)
{
    "type": "group_typing",
    "group_id": "uuid",
    "is_typing": true
}

// Запрос онлайн статуса пользователей
{
    "type": "presence_request",
    "user_ids": ["uuid1", "uuid2"]
}
```

### От сервера → клиенту (через RabbitMQ)

```json
// Новое личное сообщение
{
    "type": "dm",
    "message": {
        "id": "uuid",
        "sender_id": "uuid",
        "sender_username": "john",
        "content": "Hello!",
        "created_at": "ISO8601"
    }
}

// Новое групповое сообщение
{
    "type": "group_message",
    "group_id": "uuid",
    "group_name": "Project Chat",
    "message": {
        "id": "uuid",
        "sender_id": "uuid",
        "sender_username": "john",
        "content": "Hello everyone!",
        "created_at": "ISO8601"
    }
}

// Typing indicator (DM)
{
    "type": "dm_typing",
    "user_id": "uuid",
    "username": "john",
    "is_typing": true
}

// Typing indicator (группа)
{
    "type": "group_typing",
    "group_id": "uuid",
    "user_id": "uuid",
    "username": "john",
    "is_typing": true
}

// Прочитано (DM)
{
    "type": "dm_read",
    "user_id": "uuid",
    "last_read_at": "ISO8601"
}

// Онлайн статус
{
    "type": "presence",
    "user_id": "uuid",
    "online": true,
    "last_seen": "ISO8601"
}

// Изменения в группе
{
    "type": "group_update",
    "group_id": "uuid",
    "action": "member_added|member_removed|renamed|deleted",
    "data": {...}
}
```

## Архитектурные изменения

### 1. Connection Manager расширение

```python
class ConnectionManager:
    # Существующее
    active_connections: dict[str, WebSocket]  # user_id -> ws

    # Новое для чатов
    user_groups: dict[str, set[str]]  # user_id -> set of group_ids
    user_presence: dict[str, datetime]  # user_id -> last_activity

    async def send_to_user(self, user_id: str, message: dict) -> bool:
        """Отправить сообщение конкретному пользователю"""
        if ws := self.active_connections.get(user_id):
            await ws.send_json(message)
            return True
        return False

    async def send_to_users(self, user_ids: list[str], message: dict):
        """Отправить сообщение нескольким пользователям"""
        for user_id in user_ids:
            await self.send_to_user(user_id, message)

    async def broadcast_to_group(self, group_id: str, message: dict, exclude_user: str = None):
        """Отправить всем участникам группы"""
        # Получить member_ids из сообщения или из кэша
        pass

    def is_user_online(self, user_id: str) -> bool:
        return user_id in self.active_connections
```

### 2. Redis структуры

```
# Онлайн статус
presence:{user_id} = {"online": true, "last_seen": "ISO8601"}  # TTL 5min

# Группы пользователя (кэш)
user_groups:{user_id} = ["group_id1", "group_id2"]  # TTL 10min

# Pub/Sub каналы
channel: dm:{user_id}     - личные сообщения
channel: group:{group_id} - групповые сообщения
channel: presence         - обновления онлайн статуса
```

### 3. RabbitMQ Queues

```python
# Очереди для прослушивания
QUEUES = [
    'chat_dm',           # Личные сообщения от backend
    'chat_group',        # Групповые сообщения от backend
    'chat_group_update', # Изменения в группах
]

async def handle_chat_dm(message: dict):
    """Обработка DM от backend"""
    recipient_id = message['recipient_id']
    await manager.send_to_user(recipient_id, {
        'type': 'dm',
        'message': message['message']
    })

async def handle_chat_group(message: dict):
    """Обработка группового сообщения"""
    member_ids = message['member_ids']
    sender_id = message['sender_id']

    await manager.send_to_users(
        [uid for uid in member_ids if uid != sender_id],
        {
            'type': 'group_message',
            'group_id': message['group_id'],
            'message': message['message']
        }
    )
```

## WebSocket Message Handlers

```python
async def handle_message(websocket: WebSocket, user_id: str, data: dict):
    msg_type = data.get('type')

    if msg_type == 'chat_subscribe':
        # Загрузить группы пользователя и подписаться
        await subscribe_to_chat(user_id)

    elif msg_type == 'dm_typing':
        # Переслать typing indicator получателю
        recipient_id = data['recipient_id']
        await manager.send_to_user(recipient_id, {
            'type': 'dm_typing',
            'user_id': user_id,
            'is_typing': data['is_typing']
        })

    elif msg_type == 'group_typing':
        # Переслать всем в группе
        group_id = data['group_id']
        # Получить members из кэша или API
        await broadcast_typing_to_group(group_id, user_id, data['is_typing'])

    elif msg_type == 'presence_request':
        # Вернуть онлайн статус запрошенных пользователей
        user_ids = data['user_ids']
        presence_data = await get_presence(user_ids)
        await websocket.send_json({
            'type': 'presence_response',
            'users': presence_data
        })
```

## Задачи

### Фаза 1 — Базовая маршрутизация
- [x] Добавить `send_to_user()` в ConnectionManager
- [x] Подключить RabbitMQ очередь `chat_dm`
- [x] Обработчик для пересылки DM

### Фаза 2 — Групповые чаты
- [x] Подключить очередь `chat_group`
- [x] `send_to_users()` для групповых сообщений
- [x] Обработчик `chat_group_update`

### Фаза 3 — Typing & Presence
- [x] Обработчики `dm_typing`, `group_typing`
- [x] Redis presence tracking
- [x] Broadcast presence updates при connect/disconnect

### Фаза 4 — Масштабирование
- [ ] Redis pub/sub для multi-instance sync
- [ ] Кэширование group members в Redis
- [ ] Health check для chat connections

## Примечания

- Проверка разрешений (общие блоки) происходит на стороне omnimap-back
- omnimap-sync только маршрутизирует сообщения по user_id/group_id
- При получении сообщения из RabbitMQ, member_ids уже проверены на backend
