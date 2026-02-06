# Задачи для Frontend (omnimap-front)

## Оптимизация incremental sync

### Проблема
При обновлении страницы приходит **1271 блок**, хотя контент не менялся. Причина: **safety margin -1 секунда** вызывает ложные срабатывания.

### Текущая логика (некорректная)
```javascript
// Frontend отправляет
updated_at: unixSeconds - 1  // Safety margin

// Sync проверяет
if redis_time > client_time:  // 1738846234 > 1738846233 → ALWAYS TRUE
    return block
```

### Временное решение (применено в sync)
Изменена логика сравнения в `omnimap-sync/app/websockets.py:210`:
```python
time_diff = redis_time - client_time
if time_diff > 1:  # Игнорируем safety margin
    return block
```

### Правильное решение (требует изменений во frontend)
**Убрать safety margin при reconnect**, отправлять точные timestamps:

```javascript
// src/js/sincManager/sincManager.js:180
return {
    id: block.id,
    // Убрать safety margin при incremental sync reconnect
    updated_at: unixSeconds  // Было: unixSeconds - 1
};
```

### Обоснование
- Safety margin нужен для **realtime race conditions** (апдейты в ту же секунду)
- При **reconnect** локальные timestamps из IndexedDB уже стабильны
- Точные timestamps позволят sync корректно определить изменённые блоки

### Альтернативное решение
Если safety margin всё-таки нужен при reconnect, изменить сравнение на sync:
```python
if redis_time >= client_time + 2:  # Реальное изменение минимум на 1 секунду
```

### Тестирование
После изменений проверить:
1. Обновить страницу → должно вернуться ~0-10 блоков (только реально изменённые)
2. Изменить блок → должен вернуться при reconnect
3. Несколько устройств → синхронизация работает корректно
