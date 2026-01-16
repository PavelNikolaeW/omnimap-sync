# Задачи для omnimap-front: Access Request WebSocket Events

## Описание

omnimap-sync теперь проксирует события Access Request через WebSocket.
Фронтенду нужно добавить обработку нового типа событий для real-time UX.

## WebSocket формат события

```json
{
    "type": "access_request",
    "request_type": "new_request" | "response",
    "data": { ... }
}
```

### Новый запрос (request_type: 'new_request')

Отправляется **владельцу блока** при создании нового запроса на доступ.

```json
{
    "type": "access_request",
    "request_type": "new_request",
    "data": {
        "request_id": "uuid-string",
        "requester": {
            "id": 123,
            "username": "john_doe"
        },
        "block": {
            "id": "uuid-string",
            "title": "Block Title"
        },
        "owner_id": 456
    }
}
```

**Действие:** Показать уведомление владельцу о новом запросе на доступ.

### Ответ на запрос (request_type: 'response')

Отправляется **запросившему пользователю** при одобрении/отклонении.

```json
{
    "type": "access_request",
    "request_type": "response",
    "data": {
        "request_id": "uuid-string",
        "approved": true,
        "permission": "view",
        "block": {
            "id": "uuid-string",
            "title": "Block Title"
        },
        "user_id": 123
    }
}
```

**Действие:**
- Если `approved: true` - показать успешное уведомление, обновить доступ к блоку
- Если `approved: false` - показать уведомление об отказе

## Реализация

### 1. Добавить обработчик в WebSocket message handler

```javascript
// В обработчике WebSocket сообщений
case 'access_request':
    handleAccessRequestEvent(message.request_type, message.data);
    break;
```

### 2. Реализовать handleAccessRequestEvent

```javascript
function handleAccessRequestEvent(requestType, data) {
    if (requestType === 'new_request') {
        // Показать уведомление владельцу
        showNotification({
            title: 'Запрос на доступ',
            message: `${data.requester.username} запрашивает доступ к "${data.block.title}"`,
            actions: ['Просмотреть']
        });
    } else if (requestType === 'response') {
        if (data.approved) {
            showNotification({
                title: 'Доступ получен',
                message: `Вам выдан доступ "${data.permission}" к "${data.block.title}"`
            });
            // Обновить список доступных блоков если нужно
        } else {
            showNotification({
                title: 'Запрос отклонен',
                message: `Запрос на доступ к "${data.block.title}" был отклонен`
            });
        }
    }
}
```

## Приоритет

Medium - Функционал работает без этих изменений (Telegram уведомления есть), но WebSocket обеспечит лучший real-time UX.
