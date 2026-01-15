# Backend Tasks for Sandbox Mode Support

## Context

This PR implements sandbox mode filtering in omnimap-sync. For the feature to work correctly, the backend (omnimap-back) needs to send a new `sandbox_mode_changed` message when a block's sandbox mode is modified.

## Required Changes in omnimap-back

### 1. New Celery Task: `send_sandbox_mode_changed`

**File:** `api/tasks.py`

Add a new task to send sandbox mode change notifications:

```python
@shared_task
def send_sandbox_mode_changed(block_uuid: str, sandbox_mode: str, creator_id: int):
    """
    Send sandbox_mode_changed notification to omnimap-sync.

    Args:
        block_uuid: UUID of the block whose sandbox mode changed
        sandbox_mode: New sandbox mode ('none', 'open', 'private')
        creator_id: ID of the block creator/owner
    """
    message = {
        'action': 'sandbox_mode_changed',
        'block_uuid': block_uuid,
        'sandbox_mode': sandbox_mode,
        'creator_id': creator_id,
    }
    publish_message(message)
```

### 2. Call Task When Sandbox Mode Changes

**File:** `api/views.py` (or wherever `block_sandbox_view` is defined)

After successfully changing a block's sandbox mode:

```python
@api_view(['GET', 'POST'])
def block_sandbox_view(request, block_id):
    # ... existing code for changing sandbox mode ...

    if request.method == 'POST':
        # After successful sandbox mode change:
        from api.tasks import send_sandbox_mode_changed
        send_sandbox_mode_changed.delay(
            str(block.id),
            block.sandbox_mode,
            block.creator_id
        )
```

### 3. (Optional) Sandbox Info API Endpoint

For the fallback mechanism when sync's Redis cache is empty after restart, consider adding an endpoint:

**File:** `api/views.py`

```python
@api_view(['GET'])
@permission_classes([IsAuthenticated])  # Or service token auth
def block_sandbox_info_view(request, block_id):
    """
    Get sandbox info for a block.
    Used by omnimap-sync for cache fallback.
    """
    block = get_object_or_404(Block, id=block_id)
    return Response({
        'sandbox_mode': block.sandbox_mode,
        'creator_id': block.creator_id
    })
```

**URL:** `/api/v1/blocks/<block_id>/sandbox/`

## Message Format

The `sandbox_mode_changed` message has this structure:

```json
{
    "action": "sandbox_mode_changed",
    "block_uuid": "uuid-string",
    "sandbox_mode": "none" | "open" | "private",
    "creator_id": 123
}
```

## Testing

After implementing these changes, test the following scenarios:

1. Change block from `none` to `private` - verify sync receives the message
2. Change block from `private` to `open` - verify subscribers are notified
3. Change block from `private` to `none` - verify sandbox cache is cleared
