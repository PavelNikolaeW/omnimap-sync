"""Tests for chat_event handler (action: 'chat_event' from backend)."""

import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch

from app.models import ChatEventMessage, ChatEventResponse


class TestChatEventModels:
    """Tests for chat event Pydantic models."""

    def test_chat_event_message_dm(self):
        """Test ChatEventMessage for DM."""
        msg = ChatEventMessage(
            type="dm",
            sender_id=123,
            recipient_id=456,
            message={
                "id": "msg_1",
                "content": "Hello!",
                "created_at": "2025-01-01T12:00:00Z"
            }
        )
        assert msg.type == "dm"
        assert msg.sender_id == 123
        assert msg.recipient_id == 456

    def test_chat_event_message_group_message(self):
        """Test ChatEventMessage for group_message."""
        msg = ChatEventMessage(
            type="group_message",
            group_id="group_abc",
            sender_id=123,
            message={"id": "msg_1", "content": "Hi all!"},
            member_ids=[456, 789, 101]
        )
        assert msg.type == "group_message"
        assert msg.group_id == "group_abc"
        assert len(msg.member_ids) == 3

    def test_chat_event_message_group_update(self):
        """Test ChatEventMessage for group_update."""
        msg = ChatEventMessage(
            type="group_update",
            group_id="group_abc",
            group_action="member_added",
            data={"user_id": 123, "username": "john"},
            member_ids=[456, 789]
        )
        assert msg.type == "group_update"
        assert msg.group_action == "member_added"

    def test_chat_event_response_model(self):
        """Test ChatEventResponse model."""
        resp = ChatEventResponse(
            event_type="dm",
            data={"sender_id": 123, "message": {"content": "Hello"}}
        )
        data = resp.model_dump()
        assert data["type"] == "chat_event"
        assert data["event_type"] == "dm"
        assert data["data"]["sender_id"] == 123


class TestActionChatEvent:
    """Tests for action_chat_event handler."""

    @pytest.fixture
    def mock_connection_manager(self):
        """Create a mock connection manager."""
        manager = MagicMock()
        manager.send_to_user = AsyncMock(return_value=True)
        manager.send_to_users = AsyncMock(return_value={"user_1": True, "user_2": True})
        return manager

    @pytest.mark.asyncio
    async def test_action_chat_event_dm(self, mock_connection_manager):
        """Test chat_event with type=dm sends to both recipient and sender."""
        from app.rabbitmq_consumer import action_chat_event

        message_data = {
            "action": "chat_event",
            "type": "dm",
            "sender_id": 123,
            "recipient_id": 456,
            "message": {
                "id": "msg_1",
                "content": "Hello!",
                "created_at": "2025-01-01T12:00:00Z"
            }
        }

        with patch('app.rabbitmq_consumer.connection_manager', mock_connection_manager):
            await action_chat_event(message_data)

        # Should be called twice: once for recipient, once for sender echo
        assert mock_connection_manager.send_to_user.call_count == 2

        # Check first call (recipient)
        first_call = mock_connection_manager.send_to_user.call_args_list[0]
        assert first_call[0][0] == "456"  # recipient_id as string
        response_data = first_call[0][1]
        assert response_data["type"] == "chat_event"
        assert response_data["event_type"] == "dm"
        assert response_data["data"]["recipient_id"] == 456
        assert response_data["data"]["sender_id"] == 123
        assert response_data["data"]["message"]["recipient_id"] == 456

        # Check second call (sender echo)
        second_call = mock_connection_manager.send_to_user.call_args_list[1]
        assert second_call[0][0] == "123"  # sender_id as string

    @pytest.mark.asyncio
    async def test_action_chat_event_dm_self_message(self, mock_connection_manager):
        """Test chat_event DM to self (sender == recipient) sends only once."""
        from app.rabbitmq_consumer import action_chat_event

        message_data = {
            "action": "chat_event",
            "type": "dm",
            "sender_id": 123,
            "recipient_id": 123,  # Same as sender
            "message": {"id": "msg_1", "content": "Note to self"}
        }

        with patch('app.rabbitmq_consumer.connection_manager', mock_connection_manager):
            await action_chat_event(message_data)

        # Should be called only once (recipient only, no duplicate for sender)
        mock_connection_manager.send_to_user.assert_called_once()
        call_args = mock_connection_manager.send_to_user.call_args
        assert call_args[0][0] == "123"

    @pytest.mark.asyncio
    async def test_action_chat_event_dm_no_sender_id(self, mock_connection_manager):
        """Test chat_event DM without sender_id sends only to recipient."""
        from app.rabbitmq_consumer import action_chat_event

        message_data = {
            "action": "chat_event",
            "type": "dm",
            "recipient_id": 456,
            "message": {"id": "msg_1", "content": "System message"}
            # No sender_id
        }

        with patch('app.rabbitmq_consumer.connection_manager', mock_connection_manager):
            await action_chat_event(message_data)

        # Should be called only once (recipient only)
        mock_connection_manager.send_to_user.assert_called_once()
        call_args = mock_connection_manager.send_to_user.call_args
        assert call_args[0][0] == "456"

    @pytest.mark.asyncio
    async def test_action_chat_event_dm_missing_recipient(self, mock_connection_manager):
        """Test chat_event DM without recipient_id."""
        from app.rabbitmq_consumer import action_chat_event

        message_data = {
            "action": "chat_event",
            "type": "dm",
            "sender_id": 123,
            "message": {"id": "msg_1", "content": "Hello!"}
            # Missing recipient_id
        }

        with patch('app.rabbitmq_consumer.connection_manager', mock_connection_manager):
            await action_chat_event(message_data)

        # Should not call send_to_user due to missing recipient
        mock_connection_manager.send_to_user.assert_not_called()

    @pytest.mark.asyncio
    async def test_action_chat_event_group_message(self, mock_connection_manager):
        """Test chat_event with type=group_message."""
        from app.rabbitmq_consumer import action_chat_event

        message_data = {
            "action": "chat_event",
            "type": "group_message",
            "group_id": "group_abc",
            "sender_id": 123,
            "message": {"id": "msg_1", "content": "Hi all!"},
            "member_ids": [123, 456, 789]
        }

        with patch('app.rabbitmq_consumer.connection_manager', mock_connection_manager):
            await action_chat_event(message_data)

        mock_connection_manager.send_to_users.assert_called_once()
        call_args = mock_connection_manager.send_to_users.call_args
        assert "123" in call_args[0][0] or "456" in call_args[0][0]  # member_ids
        assert call_args[1]["exclude_user"] == "123"  # sender excluded

    @pytest.mark.asyncio
    async def test_action_chat_event_group_message_no_members(self, mock_connection_manager):
        """Test chat_event group_message without member_ids."""
        from app.rabbitmq_consumer import action_chat_event

        message_data = {
            "action": "chat_event",
            "type": "group_message",
            "group_id": "group_abc",
            "sender_id": 123,
            "message": {"id": "msg_1", "content": "Hi!"},
            "member_ids": []
        }

        with patch('app.rabbitmq_consumer.connection_manager', mock_connection_manager):
            await action_chat_event(message_data)

        # Should not call send_to_users due to empty member_ids
        mock_connection_manager.send_to_users.assert_not_called()

    @pytest.mark.asyncio
    async def test_action_chat_event_group_update(self, mock_connection_manager):
        """Test chat_event with type=group_update."""
        from app.rabbitmq_consumer import action_chat_event

        message_data = {
            "action": "chat_event",
            "type": "group_update",
            "group_id": "group_abc",
            "group_action": "member_added",
            "data": {"user_id": 999, "username": "newuser"},
            "member_ids": [456, 789, 999]
        }

        with patch('app.rabbitmq_consumer.connection_manager', mock_connection_manager):
            await action_chat_event(message_data)

        mock_connection_manager.send_to_users.assert_called_once()
        call_args = mock_connection_manager.send_to_users.call_args
        assert call_args[0][1]["event_type"] == "group_update"
        assert call_args[0][1]["data"]["group_action"] == "member_added"

    @pytest.mark.asyncio
    async def test_action_chat_event_invalid_type(self, mock_connection_manager):
        """Test chat_event with invalid type."""
        from app.rabbitmq_consumer import action_chat_event

        message_data = {
            "action": "chat_event",
            "type": "invalid_type",
            "sender_id": 123
        }

        with patch('app.rabbitmq_consumer.connection_manager', mock_connection_manager):
            # Should not raise, validation error is caught
            await action_chat_event(message_data)

        mock_connection_manager.send_to_user.assert_not_called()
        mock_connection_manager.send_to_users.assert_not_called()


class TestHandleMessageChatEvent:
    """Tests for handle_message with action='chat_event'."""

    @pytest.fixture
    def mock_rabbitmq_message(self):
        """Create a mock RabbitMQ message."""
        message = AsyncMock()
        message.ack = AsyncMock()
        message.reject = AsyncMock()
        return message

    @pytest.mark.asyncio
    async def test_handle_message_chat_event_dm(self, mock_rabbitmq_message):
        """Test handle_message routes chat_event DM correctly."""
        from app.rabbitmq_consumer import handle_message

        mock_rabbitmq_message.body = json.dumps({
            "action": "chat_event",
            "type": "dm",
            "sender_id": 123,
            "recipient_id": 456,
            "message": {"id": "1", "content": "Hello"}
        }).encode()

        with patch('app.rabbitmq_consumer.action_chat_event', new_callable=AsyncMock) as mock_action:
            await handle_message(mock_rabbitmq_message)

        mock_action.assert_called_once()
        mock_rabbitmq_message.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_message_chat_event_group_message(self, mock_rabbitmq_message):
        """Test handle_message routes chat_event group_message correctly."""
        from app.rabbitmq_consumer import handle_message

        mock_rabbitmq_message.body = json.dumps({
            "action": "chat_event",
            "type": "group_message",
            "group_id": "group_1",
            "sender_id": 123,
            "member_ids": [123, 456, 789],
            "message": {"id": "1", "content": "Hi all"}
        }).encode()

        with patch('app.rabbitmq_consumer.action_chat_event', new_callable=AsyncMock) as mock_action:
            await handle_message(mock_rabbitmq_message)

        mock_action.assert_called_once()
        mock_rabbitmq_message.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_message_chat_event_group_update(self, mock_rabbitmq_message):
        """Test handle_message routes chat_event group_update correctly."""
        from app.rabbitmq_consumer import handle_message

        mock_rabbitmq_message.body = json.dumps({
            "action": "chat_event",
            "type": "group_update",
            "group_id": "group_1",
            "group_action": "member_removed",
            "member_ids": [456, 789],
            "data": {"user_id": 123}
        }).encode()

        with patch('app.rabbitmq_consumer.action_chat_event', new_callable=AsyncMock) as mock_action:
            await handle_message(mock_rabbitmq_message)

        mock_action.assert_called_once()
        mock_rabbitmq_message.ack.assert_called_once()
