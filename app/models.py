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
