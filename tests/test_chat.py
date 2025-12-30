"""Tests for P2P and group chat functionality."""

import pytest
import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from app.connection_manager import ConnectionManager
from app.models import (
    ChatDMMessage,
    ChatGroupMessage,
    ChatGroupUpdateMessage,
    ChatDMReadMessage,
    DMResponse,
    GroupMessageResponse,
    GroupUpdateResponse,
    DMReadResponse,
    DMTypingResponse,
    GroupTypingResponse,
    PresenceResponse,
    PresenceBatchResponse,
)


class TestChatModels:
    """Tests for chat-related Pydantic models."""

    def test_chat_dm_message_valid(self):
        """Test valid ChatDMMessage creation."""
        msg = ChatDMMessage(
            recipient_id="user_123",
            message={
                "id": "msg_1",
                "sender_id": "user_456",
                "sender_username": "john",
                "content": "Hello!",
                "created_at": "2024-01-01T12:00:00Z"
            }
        )
        assert msg.recipient_id == "user_123"
        assert msg.message["content"] == "Hello!"

    def test_chat_dm_message_int_recipient(self):
        """Test ChatDMMessage with integer recipient_id."""
        msg = ChatDMMessage(
            recipient_id=123,
            message={"id": "msg_1", "content": "Hi"}
        )
        assert msg.recipient_id == 123

    def test_chat_group_message_valid(self):
        """Test valid ChatGroupMessage creation."""
        msg = ChatGroupMessage(
            group_id="group_abc",
            group_name="Project Chat",
            sender_id="user_456",
            member_ids=["user_1", "user_2", "user_3"],
            message={
                "id": "msg_1",
                "sender_id": "user_456",
                "content": "Hello everyone!"
            }
        )
        assert msg.group_id == "group_abc"
        assert len(msg.member_ids) == 3

    def test_chat_group_update_message_valid(self):
        """Test valid ChatGroupUpdateMessage creation."""
        msg = ChatGroupUpdateMessage(
            group_id="group_abc",
            action="member_added",
            member_ids=["user_1", "user_2"],
            data={"new_member": "user_3", "added_by": "user_1"}
        )
        assert msg.action == "member_added"

    def test_dm_response_model(self):
        """Test DMResponse model."""
        resp = DMResponse(message={"id": "1", "content": "Hello"})
        data = resp.model_dump()
        assert data["type"] == "dm"
        assert data["message"]["content"] == "Hello"

    def test_group_message_response_model(self):
        """Test GroupMessageResponse model."""
        resp = GroupMessageResponse(
            group_id="group_1",
            group_name="Chat",
            message={"id": "1", "content": "Hi all"}
        )
        data = resp.model_dump()
        assert data["type"] == "group_message"
        assert data["group_id"] == "group_1"

    def test_dm_typing_response_model(self):
        """Test DMTypingResponse model."""
        resp = DMTypingResponse(
            user_id="user_123",
            username="john",
            is_typing=True
        )
        data = resp.model_dump()
        assert data["type"] == "dm_typing"
        assert data["is_typing"] is True

    def test_presence_response_model(self):
        """Test PresenceResponse model."""
        resp = PresenceResponse(
            user_id="user_123",
            online=True,
            last_seen="2024-01-01T12:00:00Z"
        )
        data = resp.model_dump()
        assert data["type"] == "presence"
        assert data["online"] is True


class TestConnectionManagerChatMethods:
    """Tests for ConnectionManager chat methods."""

    @pytest.fixture
    def manager(self):
        """Create a fresh ConnectionManager instance."""
        return ConnectionManager()

    @pytest.fixture
    def mock_ws(self):
        """Create a mock WebSocket."""
        ws = AsyncMock()
        ws.send_json = AsyncMock()
        return ws

    @pytest.fixture
    def mock_ws_2(self):
        """Create a second mock WebSocket."""
        ws = AsyncMock()
        ws.send_json = AsyncMock()
        return ws


class TestSendToUser(TestConnectionManagerChatMethods):
    """Tests for send_to_user method."""

    @pytest.mark.asyncio
    async def test_send_to_user_online(self, manager, mock_ws):
        """Test sending to an online user."""
        user_id = "user_123"
        message = {"type": "dm", "message": {"content": "Hello"}}
        await manager.connect(user_id, mock_ws)

        result = await manager.send_to_user(user_id, message)

        assert result is True
        mock_ws.send_json.assert_called_once_with(message)

    @pytest.mark.asyncio
    async def test_send_to_user_offline(self, manager):
        """Test sending to an offline user."""
        message = {"type": "dm", "message": {"content": "Hello"}}

        result = await manager.send_to_user("offline_user", message)

        assert result is False

    @pytest.mark.asyncio
    async def test_send_to_user_multiple_connections(self, manager, mock_ws, mock_ws_2):
        """Test sending to user with multiple connections."""
        user_id = "user_123"
        message = {"type": "dm", "message": {"content": "Hello"}}
        await manager.connect(user_id, mock_ws)
        await manager.connect(user_id, mock_ws_2)

        result = await manager.send_to_user(user_id, message)

        assert result is True
        mock_ws.send_json.assert_called_once_with(message)
        mock_ws_2.send_json.assert_called_once_with(message)


class TestSendToUsers(TestConnectionManagerChatMethods):
    """Tests for send_to_users method."""

    @pytest.mark.asyncio
    async def test_send_to_users_all_online(self, manager, mock_ws, mock_ws_2):
        """Test sending to multiple online users."""
        message = {"type": "test"}
        await manager.connect("user_1", mock_ws)
        await manager.connect("user_2", mock_ws_2)

        results = await manager.send_to_users(["user_1", "user_2"], message)

        assert results["user_1"] is True
        assert results["user_2"] is True
        mock_ws.send_json.assert_called_once_with(message)
        mock_ws_2.send_json.assert_called_once_with(message)

    @pytest.mark.asyncio
    async def test_send_to_users_some_offline(self, manager, mock_ws):
        """Test sending when some users are offline."""
        message = {"type": "test"}
        await manager.connect("user_1", mock_ws)

        results = await manager.send_to_users(["user_1", "user_2"], message)

        assert results["user_1"] is True
        assert results["user_2"] is False

    @pytest.mark.asyncio
    async def test_send_to_users_with_exclude(self, manager, mock_ws, mock_ws_2):
        """Test sending with excluded user."""
        message = {"type": "test"}
        await manager.connect("user_1", mock_ws)
        await manager.connect("user_2", mock_ws_2)

        results = await manager.send_to_users(
            ["user_1", "user_2"],
            message,
            exclude_user="user_1"
        )

        assert "user_1" not in results
        assert results["user_2"] is True
        mock_ws.send_json.assert_not_called()
        mock_ws_2.send_json.assert_called_once_with(message)

    @pytest.mark.asyncio
    async def test_send_to_users_empty_list(self, manager):
        """Test sending to empty user list."""
        results = await manager.send_to_users([], {"type": "test"})
        assert results == {}


class TestBroadcastToGroup(TestConnectionManagerChatMethods):
    """Tests for broadcast_to_group method."""

    @pytest.mark.asyncio
    async def test_broadcast_to_group(self, manager, mock_ws, mock_ws_2):
        """Test broadcasting to group members."""
        message = {"type": "group_message", "content": "Hello"}
        await manager.connect("user_1", mock_ws)
        await manager.connect("user_2", mock_ws_2)

        results = await manager.broadcast_to_group(
            ["user_1", "user_2"],
            message
        )

        assert results["user_1"] is True
        assert results["user_2"] is True

    @pytest.mark.asyncio
    async def test_broadcast_to_group_exclude_sender(self, manager, mock_ws, mock_ws_2):
        """Test broadcasting excludes sender."""
        message = {"type": "group_message", "content": "Hello"}
        await manager.connect("sender", mock_ws)
        await manager.connect("receiver", mock_ws_2)

        results = await manager.broadcast_to_group(
            ["sender", "receiver"],
            message,
            exclude_user="sender"
        )

        assert "sender" not in results
        assert results["receiver"] is True
        mock_ws.send_json.assert_not_called()
        mock_ws_2.send_json.assert_called_once()


class TestPresence(TestConnectionManagerChatMethods):
    """Tests for presence tracking methods."""

    @pytest.mark.asyncio
    async def test_is_user_online_true(self, manager, mock_ws):
        """Test is_user_online returns True for connected user."""
        await manager.connect("user_123", mock_ws)
        assert manager.is_user_online("user_123") is True

    @pytest.mark.asyncio
    async def test_is_user_online_false(self, manager):
        """Test is_user_online returns False for disconnected user."""
        assert manager.is_user_online("user_123") is False

    @pytest.mark.asyncio
    async def test_is_user_online_after_disconnect(self, manager, mock_ws):
        """Test is_user_online returns False after disconnect."""
        await manager.connect("user_123", mock_ws)
        await manager.disconnect("user_123", mock_ws)
        assert manager.is_user_online("user_123") is False

    def test_get_online_users(self, manager):
        """Test get_online_users returns correct statuses."""
        # Add some connections manually to avoid async
        manager.active_connections["user_1"] = [AsyncMock()]
        manager.active_connections["user_2"] = [AsyncMock()]

        result = manager.get_online_users(["user_1", "user_2", "user_3"])

        assert result["user_1"] is True
        assert result["user_2"] is True
        assert result["user_3"] is False

    def test_update_presence(self, manager):
        """Test update_presence sets timestamp."""
        user_id = "user_123"
        manager.update_presence(user_id)

        assert user_id in manager.user_presence
        assert isinstance(manager.user_presence[user_id], datetime)

    def test_get_presence_online(self, manager):
        """Test get_presence for online user."""
        user_id = "user_123"
        manager.active_connections[user_id] = [AsyncMock()]
        manager.update_presence(user_id)

        result = manager.get_presence(user_id)

        assert result["user_id"] == user_id
        assert result["online"] is True
        assert result["last_seen"] is not None

    def test_get_presence_offline(self, manager):
        """Test get_presence for offline user."""
        user_id = "user_123"
        manager.update_presence(user_id)

        result = manager.get_presence(user_id)

        assert result["user_id"] == user_id
        assert result["online"] is False
        assert result["last_seen"] is not None

    def test_get_presence_never_seen(self, manager):
        """Test get_presence for user never seen."""
        result = manager.get_presence("unknown_user")

        assert result["user_id"] == "unknown_user"
        assert result["online"] is False
        assert result["last_seen"] is None

    def test_get_presence_batch(self, manager):
        """Test get_presence_batch returns list of presence info."""
        manager.active_connections["user_1"] = [AsyncMock()]
        manager.update_presence("user_1")
        manager.update_presence("user_2")

        results = manager.get_presence_batch(["user_1", "user_2", "user_3"])

        assert len(results) == 3
        assert results[0]["user_id"] == "user_1"
        assert results[0]["online"] is True
        assert results[1]["user_id"] == "user_2"
        assert results[1]["online"] is False

    @pytest.mark.asyncio
    async def test_connect_updates_presence(self, manager, mock_ws):
        """Test that connect updates presence timestamp."""
        user_id = "user_123"

        await manager.connect(user_id, mock_ws)

        assert user_id in manager.user_presence
        assert isinstance(manager.user_presence[user_id], datetime)

    @pytest.mark.asyncio
    async def test_disconnect_updates_presence(self, manager, mock_ws):
        """Test that disconnect updates last_seen timestamp."""
        user_id = "user_123"
        await manager.connect(user_id, mock_ws)
        initial_time = manager.user_presence[user_id]

        # Wait a tiny bit to ensure time difference
        await asyncio.sleep(0.001)
        await manager.disconnect(user_id, mock_ws)

        # Should have updated timestamp
        assert manager.user_presence[user_id] >= initial_time


class TestRabbitMQChatHandlers:
    """Tests for RabbitMQ chat message handlers."""

    @pytest.fixture
    def mock_connection_manager(self):
        """Create a mock connection manager."""
        manager = MagicMock()
        manager.send_to_user = AsyncMock(return_value=True)
        manager.broadcast_to_group = AsyncMock(return_value={"user_1": True})
        return manager

    @pytest.mark.asyncio
    async def test_action_chat_dm(self, mock_connection_manager):
        """Test chat_dm action handler."""
        from app.rabbitmq_consumer import action_chat_dm

        message_data = {
            "action": "chat_dm",
            "recipient_id": "user_123",
            "message": {
                "id": "msg_1",
                "sender_id": "user_456",
                "content": "Hello!"
            }
        }

        with patch('app.rabbitmq_consumer.connection_manager', mock_connection_manager):
            await action_chat_dm(message_data)

        mock_connection_manager.send_to_user.assert_called_once()
        call_args = mock_connection_manager.send_to_user.call_args
        assert call_args[0][0] == "user_123"  # recipient_id
        assert call_args[0][1]["type"] == "dm"

    @pytest.mark.asyncio
    async def test_action_chat_dm_invalid_message(self, mock_connection_manager):
        """Test chat_dm with invalid message data."""
        from app.rabbitmq_consumer import action_chat_dm

        message_data = {
            "action": "chat_dm"
            # Missing required fields
        }

        with patch('app.rabbitmq_consumer.connection_manager', mock_connection_manager):
            await action_chat_dm(message_data)

        # Should not call send_to_user due to validation error
        mock_connection_manager.send_to_user.assert_not_called()

    @pytest.mark.asyncio
    async def test_action_chat_group(self, mock_connection_manager):
        """Test chat_group action handler."""
        from app.rabbitmq_consumer import action_chat_group

        message_data = {
            "action": "chat_group",
            "group_id": "group_abc",
            "group_name": "Project Chat",
            "sender_id": "user_1",
            "member_ids": ["user_1", "user_2", "user_3"],
            "message": {
                "id": "msg_1",
                "content": "Hello everyone!"
            }
        }

        with patch('app.rabbitmq_consumer.connection_manager', mock_connection_manager):
            await action_chat_group(message_data)

        mock_connection_manager.broadcast_to_group.assert_called_once()
        call_args = mock_connection_manager.broadcast_to_group.call_args
        assert "user_1" in call_args[0][0]  # member_ids
        assert call_args[1]["exclude_user"] == "user_1"  # sender excluded

    @pytest.mark.asyncio
    async def test_action_chat_group_update(self, mock_connection_manager):
        """Test chat_group_update action handler."""
        from app.rabbitmq_consumer import action_chat_group_update

        message_data = {
            "action": "chat_group_update",
            "group_id": "group_abc",
            "action": "member_added",
            "member_ids": ["user_1", "user_2"],
            "data": {"new_member": "user_3"}
        }

        with patch('app.rabbitmq_consumer.connection_manager', mock_connection_manager):
            await action_chat_group_update(message_data)

        mock_connection_manager.broadcast_to_group.assert_called_once()

    @pytest.mark.asyncio
    async def test_action_chat_dm_read(self, mock_connection_manager):
        """Test chat_dm_read action handler."""
        from app.rabbitmq_consumer import action_chat_dm_read

        message_data = {
            "action": "chat_dm_read",
            "user_id": "user_123",  # Reader
            "recipient_id": "user_456",  # Original sender to notify
            "last_read_at": "2024-01-01T12:00:00Z"
        }

        with patch('app.rabbitmq_consumer.connection_manager', mock_connection_manager):
            await action_chat_dm_read(message_data)

        mock_connection_manager.send_to_user.assert_called_once()
        call_args = mock_connection_manager.send_to_user.call_args
        assert call_args[0][0] == "user_456"  # recipient_id (original sender)
        assert call_args[0][1]["type"] == "dm_read"


class TestWebSocketChatHandlers:
    """Tests for WebSocket chat message handlers."""

    @pytest.fixture
    def mock_websocket(self):
        """Create a mock WebSocket."""
        ws = AsyncMock()
        ws.send_json = AsyncMock()
        return ws

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        redis = AsyncMock()
        redis.get = AsyncMock(return_value="john_doe")
        redis.smembers = AsyncMock(return_value={"user_1", "user_2"})
        return redis

    @pytest.mark.asyncio
    async def test_handle_chat_subscribe(self, mock_websocket):
        """Test chat_subscribe handler."""
        from app.websockets import handle_chat_subscribe, connection_manager

        with patch.object(connection_manager, 'update_presence') as mock_update:
            await handle_chat_subscribe(mock_websocket, "user_123")

        mock_update.assert_called_once_with("user_123")
        mock_websocket.send_json.assert_called_once()
        call_args = mock_websocket.send_json.call_args[0][0]
        assert call_args["type"] == "chat_subscribed"
        assert call_args["status"] == "ok"

    @pytest.mark.asyncio
    async def test_handle_presence_request(self, mock_websocket):
        """Test presence_request handler."""
        from app.websockets import handle_presence_request, connection_manager

        data = {"user_ids": ["user_1", "user_2"]}

        # Add some online users
        connection_manager.active_connections["user_1"] = [AsyncMock()]

        await handle_presence_request(mock_websocket, data)

        mock_websocket.send_json.assert_called_once()
        call_args = mock_websocket.send_json.call_args[0][0]
        assert call_args["type"] == "presence_response"
        assert len(call_args["users"]) == 2

    @pytest.mark.asyncio
    async def test_handle_presence_request_invalid_input(self, mock_websocket):
        """Test presence_request with invalid input."""
        from app.websockets import handle_presence_request

        data = {"user_ids": "not_a_list"}

        await handle_presence_request(mock_websocket, data)

        mock_websocket.send_json.assert_called_once()
        call_args = mock_websocket.send_json.call_args[0][0]
        assert call_args["type"] == "error"

    @pytest.mark.asyncio
    async def test_handle_dm_typing(self, mock_redis):
        """Test dm_typing handler."""
        from app.websockets import handle_dm_typing, connection_manager

        mock_ws = AsyncMock()
        mock_ws.send_json = AsyncMock()

        # Connect recipient
        await connection_manager.connect("user_456", mock_ws)

        data = {
            "type": "dm_typing",
            "recipient_id": "user_456",
            "is_typing": True
        }

        with patch('app.websockets.get_redis_pool', return_value=mock_redis):
            await handle_dm_typing("user_123", data)

        # Should have sent typing indicator
        mock_ws.send_json.assert_called()
        call_args = mock_ws.send_json.call_args[0][0]
        assert call_args["type"] == "dm_typing"
        assert call_args["user_id"] == "user_123"
        assert call_args["is_typing"] is True

        # Cleanup
        await connection_manager.disconnect("user_456", mock_ws)

    @pytest.mark.asyncio
    async def test_handle_dm_typing_missing_recipient(self):
        """Test dm_typing without recipient_id."""
        from app.websockets import handle_dm_typing

        data = {"type": "dm_typing", "is_typing": True}  # Missing recipient_id

        # Should not raise, just log warning
        await handle_dm_typing("user_123", data)

    @pytest.mark.asyncio
    async def test_handle_group_typing(self, mock_redis):
        """Test group_typing handler."""
        from app.websockets import handle_group_typing, connection_manager

        mock_ws = AsyncMock()
        mock_ws.send_json = AsyncMock()

        # Connect group members
        await connection_manager.connect("user_2", mock_ws)

        data = {
            "type": "group_typing",
            "group_id": "group_abc",
            "is_typing": True
        }

        with patch('app.websockets.get_redis_pool', return_value=mock_redis):
            await handle_group_typing("user_1", data)

        # Should have sent typing indicator to group member
        mock_ws.send_json.assert_called()
        call_args = mock_ws.send_json.call_args[0][0]
        assert call_args["type"] == "group_typing"
        assert call_args["group_id"] == "group_abc"

        # Cleanup
        await connection_manager.disconnect("user_2", mock_ws)


class TestHandleMessageChatActions:
    """Tests for handle_message with chat actions."""

    @pytest.fixture
    def mock_rabbitmq_message(self):
        """Create a mock RabbitMQ message."""
        message = AsyncMock()
        message.ack = AsyncMock()
        message.reject = AsyncMock()
        return message

    @pytest.mark.asyncio
    async def test_handle_message_chat_dm(self, mock_rabbitmq_message):
        """Test handle_message routes chat_dm correctly."""
        from app.rabbitmq_consumer import handle_message

        mock_rabbitmq_message.body = json.dumps({
            "action": "chat_dm",
            "recipient_id": "user_123",
            "message": {"id": "1", "content": "Hello"}
        }).encode()

        with patch('app.rabbitmq_consumer.action_chat_dm', new_callable=AsyncMock) as mock_action:
            await handle_message(mock_rabbitmq_message)

        mock_action.assert_called_once()
        mock_rabbitmq_message.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_message_chat_group(self, mock_rabbitmq_message):
        """Test handle_message routes chat_group correctly."""
        from app.rabbitmq_consumer import handle_message

        mock_rabbitmq_message.body = json.dumps({
            "action": "chat_group",
            "group_id": "group_1",
            "sender_id": "user_1",
            "member_ids": ["user_1", "user_2"],
            "message": {"id": "1", "content": "Hi"}
        }).encode()

        with patch('app.rabbitmq_consumer.action_chat_group', new_callable=AsyncMock) as mock_action:
            await handle_message(mock_rabbitmq_message)

        mock_action.assert_called_once()
        mock_rabbitmq_message.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_message_chat_group_update(self, mock_rabbitmq_message):
        """Test handle_message routes chat_group_update correctly."""
        from app.rabbitmq_consumer import handle_message

        mock_rabbitmq_message.body = json.dumps({
            "action": "chat_group_update",
            "group_id": "group_1",
            "member_ids": ["user_1", "user_2"],
            "data": {"action": "renamed"}
        }).encode()

        with patch('app.rabbitmq_consumer.action_chat_group_update', new_callable=AsyncMock) as mock_action:
            await handle_message(mock_rabbitmq_message)

        mock_action.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_message_chat_dm_read(self, mock_rabbitmq_message):
        """Test handle_message routes chat_dm_read correctly."""
        from app.rabbitmq_consumer import handle_message

        mock_rabbitmq_message.body = json.dumps({
            "action": "chat_dm_read",
            "user_id": "user_123",
            "recipient_id": "user_456",
            "last_read_at": "2024-01-01T12:00:00Z"
        }).encode()

        with patch('app.rabbitmq_consumer.action_chat_dm_read', new_callable=AsyncMock) as mock_action:
            await handle_message(mock_rabbitmq_message)

        mock_action.assert_called_once()
