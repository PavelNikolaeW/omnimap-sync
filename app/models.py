# app/models.py
"""Pydantic models for RabbitMQ and WebSocket messages."""

from typing import Any, Literal
from pydantic import BaseModel, Field


# =============================================================================
# RabbitMQ Message Models (incoming from queue)
# =============================================================================

class UpdateBlockMessage(BaseModel):
    """Message for updating a single block."""
    block_uuid: str
    block_data: dict[str, Any]


class UpdateBlocksMessage(BaseModel):
    """Message for batch updating multiple blocks."""
    blocks: dict[str, dict[str, Any]]  # {uuid: block_data}


class UpdateAccessMessage(BaseModel):
    """Message for updating user access permissions."""
    user_id: str | int
    permission: Literal["deny", "grant"]
    start_block_ids: list[str]
    block_uuids: list[str]


class SubscribeMessage(BaseModel):
    """Message for subscribing user to blocks."""
    user_id: str | int
    block_uuids: list[str]


class UnsubscribeMessage(BaseModel):
    """Message for unsubscribing blocks (removes block entirely)."""
    block_uuids: list[str]


# =============================================================================
# WebSocket Request Models (incoming from client)
# =============================================================================

class BlockUpdateRequest(BaseModel):
    """Single block in get_updates request."""
    id: str
    updated_at: int = 0


class GetUpdatesRequest(BaseModel):
    """Request for getting block updates."""
    action: Literal["get_updates"]
    blocks: list[BlockUpdateRequest] = Field(default_factory=list)


class WebSocketRequest(BaseModel):
    """Generic WebSocket request with action."""
    action: str
    blocks: list[dict[str, Any]] = Field(default_factory=list)


# =============================================================================
# WebSocket Response Models (outgoing to client)
# =============================================================================

class ErrorResponse(BaseModel):
    """Error response sent to client."""
    type: Literal["error"] = "error"
    message: str


class BlockUpdateResponse(BaseModel):
    """Single block update notification."""
    type: Literal["block_update"] = "block_update"
    block_uuid: str
    data: dict[str, Any]


class BlockUpdatesBatchResponse(BaseModel):
    """Batch of block updates for a single user."""
    type: Literal["block_updates_batch"] = "block_updates_batch"
    updates: list[dict[str, Any]]


class BlockUpdatesResponse(BaseModel):
    """Response with list of block updates."""
    type: Literal["block_updates"] = "block_updates"
    updates: list[dict[str, Any]]


class BlockUpdateAccessResponse(BaseModel):
    """Response when access permissions change."""
    type: Literal["block_update_access"] = "block_update_access"
    start_block_ids: list[dict[str, Any]]
    block_uuids: list[str]
    permission: str


# =============================================================================
# Notification Event Models (Reminders & Subscriptions)
# =============================================================================

class NotificationEventMessage(BaseModel):
    """Base message for notification events from RabbitMQ."""
    type: str
    user_id: str | int
    data: dict[str, Any]


class ReminderEventResponse(BaseModel):
    """Response for reminder events sent to client."""
    type: str  # reminder_created, reminder_updated, reminder_deleted, reminder_triggered, reminder_snoozed
    data: dict[str, Any]


class SubscriptionEventResponse(BaseModel):
    """Response for subscription events sent to client."""
    type: str  # subscription_created, subscription_updated, subscription_deleted
    data: dict[str, Any]


# =============================================================================
# Chat Models (P2P and Group messaging)
# =============================================================================

# --- RabbitMQ incoming messages ---

class ChatDMMessage(BaseModel):
    """Direct message from RabbitMQ queue chat_dm."""
    recipient_id: str | int
    message: dict[str, Any]  # Contains id, sender_id, sender_username, content, created_at


class ChatGroupMessage(BaseModel):
    """Group message from RabbitMQ queue chat_group."""
    group_id: str
    group_name: str | None = None
    sender_id: str | int
    member_ids: list[str]
    message: dict[str, Any]  # Contains id, sender_id, sender_username, content, created_at


class ChatGroupUpdateMessage(BaseModel):
    """Group update notification from RabbitMQ queue chat_group_update."""
    group_id: str
    action: Literal["member_added", "member_removed", "renamed", "deleted"]
    member_ids: list[str]  # Current group members to notify
    data: dict[str, Any]  # Action-specific data


class ChatDMReadMessage(BaseModel):
    """DM read receipt from RabbitMQ."""
    user_id: str | int  # User who read messages
    recipient_id: str | int  # User to notify
    last_read_at: str  # ISO8601 timestamp


# --- WebSocket outgoing responses ---

class DMResponse(BaseModel):
    """Direct message notification sent to client."""
    type: Literal["dm"] = "dm"
    message: dict[str, Any]


class GroupMessageResponse(BaseModel):
    """Group message notification sent to client."""
    type: Literal["group_message"] = "group_message"
    group_id: str
    group_name: str | None = None
    message: dict[str, Any]


class DMTypingResponse(BaseModel):
    """DM typing indicator sent to client."""
    type: Literal["dm_typing"] = "dm_typing"
    user_id: str
    username: str | None = None
    is_typing: bool


class GroupTypingResponse(BaseModel):
    """Group typing indicator sent to client."""
    type: Literal["group_typing"] = "group_typing"
    group_id: str
    user_id: str
    username: str | None = None
    is_typing: bool


class DMReadResponse(BaseModel):
    """DM read receipt sent to client."""
    type: Literal["dm_read"] = "dm_read"
    user_id: str
    last_read_at: str


class PresenceResponse(BaseModel):
    """User presence status sent to client."""
    type: Literal["presence"] = "presence"
    user_id: str
    online: bool
    last_seen: str | None = None


class PresenceBatchResponse(BaseModel):
    """Batch presence response for multiple users."""
    type: Literal["presence_response"] = "presence_response"
    users: list[dict[str, Any]]


class GroupUpdateResponse(BaseModel):
    """Group update notification sent to client."""
    type: Literal["group_update"] = "group_update"
    group_id: str
    action: str
    data: dict[str, Any]
