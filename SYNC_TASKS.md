# Задачи для omnimap-sync

## Redis online-статус пользователя

**Статус: РЕАЛИЗОВАНО ✅**

**Цель:** Отслеживать online-статус пользователей для оптимизации Telegram-уведомлений (не отправлять если пользователь уже онлайн в браузере).

### Спецификация

**Redis ключ:** `user_online:{user_id}`
**Тип:** Integer (счётчик подключений)
**TTL:** 5 минут (автообновление при активности)

### Логика

1. **При connect пользователя:**
   ```python
   redis.incr(f"user_online:{user_id}")
   redis.expire(f"user_online:{user_id}", 300)  # 5 минут TTL
   ```

2. **При disconnect пользователя:**
   ```python
   key = f"user_online:{user_id}"
   current = redis.decr(key)
   if current <= 0:
       redis.delete(key)
   ```

3. **При любой активности (heartbeat/ping):**
   ```python
   redis.expire(f"user_online:{user_id}", 300)  # обновить TTL
   ```

### Почему счётчик, а не флаг

Пользователь может быть подключён с нескольких устройств одновременно (телефон + компьютер). Счётчик корректно отслеживает все подключения:
- connect с устройства 1 → counter = 1
- connect с устройства 2 → counter = 2
- disconnect с устройства 1 → counter = 1 (всё ещё онлайн!)
- disconnect с устройства 2 → counter = 0 → ключ удаляется

### Где реализовать

В файле `app/connection_manager.py`:
- Метод `connect()` — инкремент счётчика
- Метод `disconnect()` — декремент счётчика
- Добавить heartbeat handler для обновления TTL

### Использование в omnimap-back

omnimap-back будет проверять онлайн-статус перед отправкой Telegram-уведомлений:
```python
def is_user_online(user_id: int) -> bool:
    redis_client = get_redis_client()
    count = redis_client.get(f"user_online:{user_id}")
    return count is not None and int(count) > 0
```

### Важно

- Оба сервиса используют один Redis (проверить конфигурацию)
- TTL защищает от "зависших" ключей при краше сервера
- Не блокирует: если Redis недоступен, omnimap-back просто отправляет уведомление
