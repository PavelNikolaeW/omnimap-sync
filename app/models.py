# app/models.py
"""Pydantic models for RabbitMQ and WebSocket messages."""

from typing import Any, Literal
from pydantic import BaseModel, Field


# =============================================================================
# RabbitMQ Message Models (incoming from queue)
# =============================================================================

class SandboxContextItem(BaseModel):
    """Sandbox info for a parent block."""
    mode: Literal["open", "private"]
    creator_id: int | str | None = None


class UpdateBlockMessage(BaseModel):
    """Message for updating a single block."""
    block_uuid: str
    block_data: dict[str, Any]
    # Sandbox context from backend - avoids Redis lookups
    # Format: {parent_uuid: {mode: "private", creator_id: 123}}
    sandbox_context: dict[str, SandboxContextItem] | None = None


class UpdateBlocksMessage(BaseModel):
    """Message for batch updating multiple blocks."""
    blocks: dict[str, dict[str, Any]]  # {uuid: block_data}
    # Sandbox context from backend - avoids Redis lookups
    # Format: {parent_uuid: {mode: "private", creator_id: 123}}
    sandbox_context: dict[str, SandboxContextItem] | None = None


class UpdateAccessMessage(BaseModel):
    """Message for updating user access permissions."""
    user_id: str | int
    permission: Literal["deny", "view", "edit", "edit_ac", "delete"]
    start_block_ids: list[str]
    block_uuids: list[str]
    block_data: list[dict[str, Any]] | None = None  # Block data from backend (avoids Redis lookup)


class SubscribeMessage(BaseModel):
    """Message for subscribing user to blocks."""
    user_id: str | int
    block_uuids: list[str]


class UnsubscribeMessage(BaseModel):
    """Message for unsubscribing blocks (removes block entirely)."""
    block_uuids: list[str]


class SandboxModeChangedMessage(BaseModel):
    """Message when a block's sandbox mode changes."""
    block_uuid: str
    sandbox_mode: Literal["none", "open", "private"]
    creator_id: int | str | None = None


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
    new_block_ids: list[str] = []  # IDs блоков которых нет у клиента


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
# Chat Event Models (P2P and Group messaging)
# =============================================================================

class ChatEventMessage(BaseModel):
    """
    Chat event from RabbitMQ (action: 'chat_event').

    Used by backend to send DM, group messages, and group updates.
    """
    type: Literal["dm", "group_message", "group_update"]
    sender_id: int | str | None = None
    recipient_id: int | str | None = None  # For DM
    group_id: str | None = None  # For group messages
    group_action: str | None = None  # For group_update: member_added, member_removed, etc.
    message: dict[str, Any] | None = None
    data: dict[str, Any] | None = None  # For group_update
    member_ids: list[int | str] = Field(default_factory=list)  # For group messages


class ChatEventResponse(BaseModel):
    """
    Chat event response sent to client via WebSocket.

    Format: { type: 'chat_event', event_type: '...', data: {...} }
    """
    type: Literal["chat_event"] = "chat_event"
    event_type: str  # dm, group_message, group_update
    data: dict[str, Any]


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


class PresenceBatchResponse(BaseModel):
    """Batch presence response for multiple users."""
    type: Literal["presence_response"] = "presence_response"
    users: list[dict[str, Any]]


class SandboxModeChangedResponse(BaseModel):
    """Response when a block's sandbox mode changes."""
    type: Literal["sandbox_mode_changed"] = "sandbox_mode_changed"
    block_uuid: str
    sandbox_mode: str


# =============================================================================
# Access Request Models (permission requests from users)
# =============================================================================

class AccessRequestMessage(BaseModel):
    """
    Access request event from RabbitMQ (action: 'access_request').

    Used by backend to notify about access requests:
    - new_request: sent to block owner when someone requests access
    - response: sent to requester when owner approves/denies
    """
    type: Literal["new_request", "response"]
    request_id: str
    # For new_request
    requester: dict[str, Any] | None = None  # {id: int, username: str}
    block: dict[str, Any] | None = None  # {id: str, title: str}
    owner_id: int | str | None = None
    # For response
    approved: bool | None = None
    permission: str | None = None
    user_id: int | str | None = None


class AccessRequestResponse(BaseModel):
    """
    Access request event sent to client via WebSocket.

    Format matches backend RabbitMQ message:
    - action: 'access_request'
    - type: 'new_request' | 'response'
    - All fields at top level (no nested 'data' object)
    """
    action: Literal["access_request"] = "access_request"
    type: Literal["new_request", "response"]
    request_id: str
    # For new_request
    requester: dict[str, Any] | None = None  # {id: int, username: str}
    block: dict[str, Any] | None = None  # {id: str, title: str}
    owner_id: int | str | None = None
    # For response
    approved: bool | None = None
    permission: str | None = None
    user_id: int | str | None = None
