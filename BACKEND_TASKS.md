# Backend Tasks for omnimap-back

## Bug: Shared blocks changes not syncing to owner

**Issue:** When user A edits a shared block owned by user T, changes don't sync to user T.

**Root Cause Analysis:**

The `omnimap-sync` service logic is correct - it broadcasts `update_block` messages to ALL subscribers of a block without filtering by owner. The issue is likely in omnimap-back.

## Required Changes in omnimap-back

### 1. Ensure block owner is subscribed to their blocks

When a user creates a block, omnimap-back must send a `subscribe` message to RabbitMQ:

```json
{
    "action": "subscribe",
    "user_id": "<owner_user_id>",
    "block_uuids": ["<block_uuid>"]
}
```

**Check if this is happening:**
- Find the block creation endpoint/service
- Verify it sends a `subscribe` message after creating a block

### 2. Ensure `update_block` is sent when shared users edit

When ANY user (owner or shared user) edits a block, omnimap-back must send:

```json
{
    "action": "update_block",
    "block_uuid": "<block_uuid>",
    "block_data": { ... updated block data ... }
}
```

**Check if this is happening:**
- Find the block update endpoint/service
- Verify it sends `update_block` regardless of whether the editor is the owner

### 3. When granting access, subscribe the user to the block

When user T shares a block with user A, omnimap-back should send:

```json
{
    "action": "update_access",
    "user_id": "<user_A_id>",
    "permission": "grant",
    "start_block_ids": ["<root_block_id>"],
    "block_uuids": ["<all_block_uuids_in_subtree>"]
}
```

This will:
1. Add user A to `block:{block_uuid}` subscriber sets
2. Add block UUIDs to `subscriber:{user_A}:blocks` set
3. Notify user A about the access change

## How omnimap-sync Works (for reference)

### Redis Data Structures:
- `block:{block_uuid}` - Set of user IDs subscribed to this block
- `subscriber:{user_id}:blocks` - Set of block UUIDs a user is subscribed to
- `blockdata:{block_uuid}` - Hash with block data

### Message Flow:
1. Backend sends `update_block` to RabbitMQ
2. omnimap-sync receives it via `handle_message`
3. omnimap-sync calls `action_update_block`:
   - Saves block data to Redis (`blockdata:{block_uuid}`)
   - Fetches all subscribers: `redis.smembers(f"block:{block_uuid}")`
   - Sends WebSocket message to ALL subscribers

### Key Point:
omnimap-sync broadcasts to ALL users in the subscriber set. If user T (owner) is not receiving updates, either:
1. User T is not in the subscriber set (not subscribed to their own block)
2. Backend is not sending `update_block` message when user A edits

## Debugging Steps

1. Check Redis subscriber sets:
```bash
redis-cli SMEMBERS "block:<block_uuid>"
# Should include BOTH owner and shared user IDs
```

2. Check if `update_block` message is being sent:
```python
# Add logging in omnimap-back where block updates are processed
logger.info(f"Sending update_block for block {block_uuid}")
```

3. Check omnimap-sync logs:
```bash
# Should see:
# "Processed update_block for block_uuid=..."
# "No subscribers found for block_uuid=..." (if issue is missing subscribers)
```
