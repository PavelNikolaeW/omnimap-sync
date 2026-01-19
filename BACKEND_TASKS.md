# Backend Tasks for Sandbox Context Optimization

## Context

При массовом обновлении блоков (657+) sync-сервис делал параллельные Redis-запросы для проверки sandbox mode каждого parent блока. Это приводило к ошибке `Too many connections`.

**Решение:** Backend отправляет sandbox context в RabbitMQ сообщении, избегая N Redis-запросов на стороне sync.

## Required Changes

### 1. Update `update_block` RabbitMQ message

При отправке сообщения `action: 'update_block'` добавить опциональное поле `sandbox_context`:

```python
# Если блок находится в private sandbox - добавить контекст
message = {
    "action": "update_block",
    "block_uuid": block.uuid,
    "block_data": {...},
    # NEW: добавить если parent - sandbox контейнер
    "sandbox_context": {
        "parent_uuid": {
            "mode": "private",  # или "open"
            "creator_id": 123
        }
    }
}
```

### 2. Update `update_blocks` RabbitMQ message

При отправке batch сообщения `action: 'update_blocks'` добавить `sandbox_context` со всеми sandbox parents:

```python
# Собрать все уникальные parent_id из blocks
# Для каждого parent проверить sandbox_mode
# Если sandbox_mode in ("open", "private") - добавить в контекст

message = {
    "action": "update_blocks",
    "blocks": {
        "uuid1": {...},
        "uuid2": {...},
    },
    # NEW: sandbox info для всех parent'ов которые являются sandbox контейнерами
    "sandbox_context": {
        "parent_uuid_1": {"mode": "private", "creator_id": 123},
        "parent_uuid_2": {"mode": "open", "creator_id": 456}
    }
}
```

### Implementation Notes

1. **sandbox_context is optional** - sync делает fallback на Redis если не предоставлен (backwards compatible)

2. **Include only sandbox parents** - не нужно включать parents с `sandbox_mode=None`

3. **Performance**: Fetch sandbox info в одном запросе для всех parent_ids:
   ```python
   parent_ids = {block.parent_id for block in blocks if block.parent_id}
   sandbox_parents = Block.objects.filter(
       id__in=parent_ids,
       sandbox_mode__in=['open', 'private']
   ).values('id', 'sandbox_mode', 'creator_id')

   sandbox_context = {
       str(p['id']): {
           "mode": p['sandbox_mode'],
           "creator_id": p['creator_id']
       }
       for p in sandbox_parents
   }
   ```

## Sync Models (for reference)

```python
class SandboxContextItem(BaseModel):
    mode: Literal["open", "private"]
    creator_id: int | str | None = None

class UpdateBlockMessage(BaseModel):
    block_uuid: str
    block_data: dict[str, Any]
    sandbox_context: dict[str, SandboxContextItem] | None = None

class UpdateBlocksMessage(BaseModel):
    blocks: dict[str, dict[str, Any]]
    sandbox_context: dict[str, SandboxContextItem] | None = None
```

## Testing

After implementation, create 500+ blocks at once and verify:
1. No "Too many connections" errors in sync logs
2. Private sandbox filtering still works correctly
