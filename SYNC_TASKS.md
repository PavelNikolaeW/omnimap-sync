# Задачи для omnimap-sync

## ✅ Использовать block_data из RabbitMQ сообщения при grant

**Статус: ВЫПОЛНЕНО**
**PR:** #11 (fix/grant-permission-block-data)
**Приоритет:** Critical

### Что сделано

1. Добавлено поле `block_data: list[dict[str, Any]] | None` в `UpdateAccessMessage` (app/models.py)
2. Функция `send_message_update_access` использует `block_data` из сообщения если есть, иначе fallback на Redis
3. Добавлены тесты:
   - `test_send_message_update_access_grant_with_block_data_param`
   - `test_send_message_update_access_grant_redis_empty_no_block_data`
   - `test_send_message_update_access_grant_fallback_to_redis`
   - `test_action_update_access_grant_passes_block_data`

### Тестирование

- 169 тестов проходят
- Backwards compatible (fallback на Redis сохранён)

---

## ✅ Redis online-статус пользователя

**Статус: ВЫПОЛНЕНО**
**PR:** #10 (merged)

### Что реализовано

**Redis ключ:** `user_online:{user_id}`
**Тип:** Integer (счётчик подключений)
**TTL:** Настраивается через `settings.ONLINE_STATUS_TTL`

### Реализация в connection_manager.py

1. **При connect:** `_increment_online_counter()` — INCR + EXPIRE
2. **При disconnect:** `_decrement_online_counter()` — DECR, удаление при counter <= 0
3. **При heartbeat:** `refresh_user_online_ttl()` — обновление TTL

### Особенности

- Атомарные операции через pipeline с transaction
- Graceful handling ошибок Redis (не блокирует connect/disconnect)
- Защита от негативных счётчиков (logging + cleanup)
- Пропускает anonymous users

---

Все задачи выполнены. Файл можно удалить после merge PR #11.
