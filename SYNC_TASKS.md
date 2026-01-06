# Задачи для omnimap-sync

## Проблема синхронизации удаления блоков

### Контекст

Backend (omnimap-back) при удалении блока теперь корректно:
1. Отправляет `send_message_block_update` для родительского блока (с обновлённым `children`/`childOrder`)
2. Отправляет `send_message_unsubscribe_user` с ID **всех удалённых блоков** (включая потомков)

### Проблема в sync-service

**Текущее поведение `action_unsubscribe()`:**
1. Получает список `block_uuids` удалённых блоков
2. Удаляет данные из Redis
3. **НЕ отправляет уведомление клиентам** о том, что блоки удалены

**Ожидаемое поведение:**
- sync-service должен отправить всем подписчикам уведомление `{type: 'block_update', data: {id: blockId, deleted: true}}`

### Задача

- [x] В `action_unsubscribe()` добавить отправку уведомлений всем подписчикам **ДО удаления из Redis**:

```python
async def action_unsubscribe(message_data: dict[str, Any]) -> None:
    msg = UnsubscribeMessage(**message_data)
    redis = await get_redis_pool()

    # 1. Собираем всех подписчиков ДО удаления
    all_subscribers: set[str] = set()
    for block_uuid in msg.block_uuids:
        subs = await redis.smembers(f"block:{block_uuid}")
        all_subscribers.update(subs)

    # 2. Отправляем уведомления о удалении
    for block_uuid in msg.block_uuids:
        response = BlockUpdateResponse(
            block_uuid=block_uuid,
            data={"id": block_uuid, "deleted": True}
        )
        await connection_manager.send_message_to_subscribers(
            response.model_dump(),
            all_subscribers
        )

    # 3. Теперь удаляем из Redis (существующий код)
    # ...
```

### Файл для изменения

`omnimap-sync/app/rabbitmq_consumer.py` → функция `action_unsubscribe()`

### Связанные изменения в backend

Исправлен файл `api/view_delete_tree.py`:
- Строка 110: `send_message_unsubscribe_user` теперь вызывается с `ids_to_delete` (удалённые блоки), а не с `updated_targets`

### Тестирование

После внесения изменений проверить:
1. Клиент A удаляет блок
2. Клиент B (подписанный на этот блок) должен получить `{type: 'block_update', data: {id: blockId, deleted: true}}`
3. Клиент B должен также получить обновление родительского блока (это уже работает через `send_message_block_update`)
