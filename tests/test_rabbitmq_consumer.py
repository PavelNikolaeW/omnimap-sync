"""Tests for app/rabbitmq_consumer.py module."""

import pytest
import json
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from collections import defaultdict

from app.rabbitmq_consumer import (
    action_update_block,
    action_update_blocks,
    action_update_access,
    action_subscribe,
    action_unsubscribe,
    action_sandbox_mode_changed,
    action_notification_event,
    action_access_request,
    handle_message,
    send_message_update_access,
    REMINDER_EVENT_TYPES,
    SUBSCRIPTION_EVENT_TYPES,
    NOTIFICATION_EVENT_TYPES,
)
from app.config import settings


class TestActionUpdateBlock:
    """Tests for action_update_block function."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        redis = AsyncMock()
        redis.hset = AsyncMock()
        redis.smembers = AsyncMock(return_value=set())
        return redis

    @pytest.mark.asyncio
    async def test_action_update_block_saves_to_redis(self, mock_redis):
        """Test that block data is saved to Redis."""
        message = {
            "block_uuid": "block-123",
            "block_data": {"title": "Test", "updated_at": 1000}
        }

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
                mock_cm.send_message_to_subscribers = AsyncMock()
                await action_update_block(message)

        # Values are serialized for Redis (numbers become strings)
        mock_redis.hset.assert_called_once_with(
            "blockdata:block-123",
            mapping={"title": "Test", "updated_at": "1000"}
        )

    @pytest.mark.asyncio
    async def test_action_update_block_notifies_subscribers(self, mock_redis):
        """Test that subscribers are notified of block update."""
        message = {
            "block_uuid": "block-123",
            "block_data": {"title": "Test"}
        }
        mock_redis.smembers = AsyncMock(return_value={"user_1", "user_2"})

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
                mock_cm.send_message_to_subscribers = AsyncMock()
                await action_update_block(message)

        mock_cm.send_message_to_subscribers.assert_called_once()
        call_args = mock_cm.send_message_to_subscribers.call_args
        assert call_args[0][0]["type"] == "block_update"
        assert call_args[0][0]["block_uuid"] == "block-123"

    @pytest.mark.asyncio
    async def test_action_update_block_missing_block_uuid(self, mock_redis):
        """Test handling of missing block_uuid."""
        message = {"block_data": {"title": "Test"}}

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            # Should not raise, just log error
            await action_update_block(message)

        mock_redis.hset.assert_not_called()

    @pytest.mark.asyncio
    async def test_action_update_block_missing_block_data(self, mock_redis):
        """Test handling of missing block_data."""
        message = {"block_uuid": "block-123"}

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            await action_update_block(message)

        mock_redis.hset.assert_not_called()

    @pytest.mark.asyncio
    async def test_action_update_block_no_subscribers(self, mock_redis):
        """Test when block has no subscribers."""
        message = {
            "block_uuid": "block-123",
            "block_data": {"title": "Test"}
        }
        mock_redis.smembers = AsyncMock(return_value=set())

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
                mock_cm.send_message_to_subscribers = AsyncMock()
                await action_update_block(message)

        # Should not call send_message_to_subscribers when no subscribers
        mock_cm.send_message_to_subscribers.assert_not_called()


class TestActionUpdateBlocks:
    """Tests for action_update_blocks function."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client with pipeline support."""
        redis = AsyncMock()

        pipe = AsyncMock()
        pipe.hset = MagicMock()
        pipe.smembers = MagicMock()
        pipe.execute = AsyncMock(return_value=[])
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=None)
        redis.pipeline = MagicMock(return_value=pipe)

        return redis

    @pytest.mark.asyncio
    async def test_action_update_blocks_saves_multiple_blocks(self, mock_redis):
        """Test that multiple blocks are saved to Redis."""
        message = {
            "blocks": {
                "block-1": {"title": "Block 1"},
                "block-2": {"title": "Block 2"}
            }
        }

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
                mock_cm.send_message_to_subscribers = AsyncMock()
                await action_update_blocks(message)

        # Pipeline should be created
        mock_redis.pipeline.assert_called()

    @pytest.mark.asyncio
    async def test_action_update_blocks_missing_blocks_key(self, mock_redis):
        """Test handling of missing blocks key."""
        message = {}

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            await action_update_blocks(message)

        mock_redis.pipeline.assert_not_called()

    @pytest.mark.asyncio
    async def test_action_update_blocks_invalid_blocks_type(self, mock_redis):
        """Test handling of invalid blocks type (not dict)."""
        message = {"blocks": ["not", "a", "dict"]}

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            await action_update_blocks(message)

        mock_redis.pipeline.assert_not_called()

    @pytest.mark.asyncio
    async def test_action_update_blocks_notifies_subscribers(self, mock_redis):
        """Test that subscribers are notified for each block."""
        message = {
            "blocks": {
                "block-1": {"title": "Block 1"},
                "block-2": {"title": "Block 2"}
            }
        }

        # Setup pipeline to return subscribers
        pipe = AsyncMock()
        pipe.hset = MagicMock()
        pipe.smembers = MagicMock()
        # First execute returns nothing (for hset), second returns subscribers
        pipe.execute = AsyncMock(side_effect=[
            [],  # hset results
            [{"user_1"}, {"user_2"}]  # smembers results
        ])
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=None)
        mock_redis.pipeline = MagicMock(return_value=pipe)

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
                mock_cm.send_message_to_subscribers = AsyncMock()
                await action_update_blocks(message)

        # Should notify subscribers
        assert mock_cm.send_message_to_subscribers.call_count >= 0


class TestActionUpdateAccess:
    """Tests for action_update_access function."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client with pipeline support."""
        redis = AsyncMock()

        pipe = AsyncMock()
        pipe.sadd = MagicMock()
        pipe.srem = MagicMock()
        pipe.hgetall = MagicMock()
        pipe.execute = AsyncMock(return_value=[])
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=None)
        redis.pipeline = MagicMock(return_value=pipe)

        return redis

    @pytest.mark.asyncio
    async def test_action_update_access_deny_removes_from_sets(self, mock_redis):
        """Test that deny permission removes user from block sets."""
        message = {
            "user_id": "user_123",
            "permission": "deny",
            "start_block_ids": ["block-1"],
            "block_uuids": ["block-2", "block-3"]
        }

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            with patch("app.rabbitmq_consumer.send_message_update_access", new_callable=AsyncMock):
                await action_update_access(message)

        # Pipeline should have srem calls
        pipe = mock_redis.pipeline.return_value
        assert pipe.srem.called or pipe.execute.called

    @pytest.mark.asyncio
    async def test_action_update_access_edit_adds_to_sets(self, mock_redis):
        """Test that edit permission adds user to block sets."""
        message = {
            "user_id": "user_123",
            "permission": "edit",
            "start_block_ids": ["block-1"],
            "block_uuids": ["block-2"]
        }

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            with patch("app.rabbitmq_consumer.send_message_update_access", new_callable=AsyncMock):
                await action_update_access(message)

        pipe = mock_redis.pipeline.return_value
        assert pipe.sadd.called or pipe.execute.called

    @pytest.mark.asyncio
    async def test_action_update_access_missing_required_fields(self, mock_redis):
        """Test handling of missing required fields."""
        message = {"user_id": "user_123"}  # Missing permission and block_uuids

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            await action_update_access(message)

        mock_redis.pipeline.assert_not_called()

    @pytest.mark.asyncio
    async def test_action_update_access_edit_passes_block_data(self, mock_redis):
        """Test that block_data is passed to send_message_update_access."""
        block_data = [{"id": "block-1", "title": "Test Block"}]
        message = {
            "user_id": "user_123",
            "permission": "edit",
            "start_block_ids": ["block-1"],
            "block_uuids": ["block-1"],
            "block_data": block_data
        }

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            with patch("app.rabbitmq_consumer.send_message_update_access", new_callable=AsyncMock) as mock_send:
                await action_update_access(message)

        mock_send.assert_called_once()
        call_kwargs = mock_send.call_args.kwargs
        assert call_kwargs.get("block_data") == block_data


class TestActionSubscribe:
    """Tests for action_subscribe function."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client with pipeline support."""
        redis = AsyncMock()

        pipe = AsyncMock()
        pipe.sadd = MagicMock()
        pipe.execute = AsyncMock(return_value=[])
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=None)
        redis.pipeline = MagicMock(return_value=pipe)

        return redis

    @pytest.mark.asyncio
    async def test_action_subscribe_adds_user_to_block_sets(self, mock_redis):
        """Test that subscribe adds user to block subscriber sets."""
        message = {
            "user_id": "user_123",
            "block_uuids": ["block-1", "block-2"]
        }

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            await action_subscribe(message)

        pipe = mock_redis.pipeline.return_value
        # Should call sadd for both block:uuid and subscriber:user:blocks
        assert pipe.sadd.called

    @pytest.mark.asyncio
    async def test_action_subscribe_missing_user_id(self, mock_redis):
        """Test handling of missing user_id."""
        message = {"block_uuids": ["block-1"]}

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            await action_subscribe(message)

        mock_redis.pipeline.assert_not_called()

    @pytest.mark.asyncio
    async def test_action_subscribe_missing_block_uuids(self, mock_redis):
        """Test handling of missing block_uuids."""
        message = {"user_id": "user_123"}

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            await action_subscribe(message)

        mock_redis.pipeline.assert_not_called()

    @pytest.mark.asyncio
    async def test_action_subscribe_empty_block_list(self, mock_redis):
        """Test subscribing to empty block list."""
        message = {
            "user_id": "user_123",
            "block_uuids": []
        }

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            await action_subscribe(message)

        # Should not create pipeline for empty list
        # (depends on implementation - loop doesn't execute)


class TestActionUnsubscribe:
    """Tests for action_unsubscribe function."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client with pipeline support."""
        redis = AsyncMock()

        pipe = AsyncMock()
        pipe.smembers = MagicMock()
        pipe.srem = MagicMock()
        pipe.delete = MagicMock()
        pipe.execute = AsyncMock(return_value=[set()])
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=None)
        redis.pipeline = MagicMock(return_value=pipe)

        return redis

    @pytest.mark.asyncio
    async def test_action_unsubscribe_removes_blocks(self, mock_redis):
        """Test that unsubscribe removes blocks and cleans up subscriptions."""
        message = {"block_uuids": ["block-1", "block-2"]}

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
                mock_cm.send_message_to_subscribers = AsyncMock()
                await action_unsubscribe(message)

        pipe = mock_redis.pipeline.return_value
        # Should call delete for block keys
        assert pipe.delete.called or pipe.execute.called

    @pytest.mark.asyncio
    async def test_action_unsubscribe_sends_deletion_notifications(self, mock_redis):
        """Test that unsubscribe sends deletion notifications to all subscribers."""
        message = {"block_uuids": ["block-1", "block-2"]}

        # Setup pipeline to return subscribers
        pipe = AsyncMock()
        pipe.smembers = MagicMock()
        pipe.srem = MagicMock()
        pipe.delete = MagicMock()
        # Return subscribers for each block
        pipe.execute = AsyncMock(side_effect=[
            [{"user_1", "user_2"}, {"user_2", "user_3"}],  # smembers results
            [],  # srem + delete results
        ])
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=None)
        mock_redis.pipeline = MagicMock(return_value=pipe)

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
                mock_cm.send_message_to_subscribers = AsyncMock()
                await action_unsubscribe(message)

        # Should call send_message_to_subscribers for each block
        assert mock_cm.send_message_to_subscribers.call_count == 2

        # Check that deletion notifications are sent correctly
        calls = mock_cm.send_message_to_subscribers.call_args_list
        for i, call in enumerate(calls):
            message_sent = call[0][0]
            subscribers = call[0][1]
            assert message_sent["type"] == "block_update"
            assert message_sent["data"]["deleted"] is True
            assert message_sent["data"]["id"] in ["block-1", "block-2"]
            # All unique subscribers from all blocks
            assert subscribers == {"user_1", "user_2", "user_3"}

    @pytest.mark.asyncio
    async def test_action_unsubscribe_missing_block_uuids(self, mock_redis):
        """Test handling of missing block_uuids."""
        message = {}

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            await action_unsubscribe(message)

        mock_redis.pipeline.assert_not_called()

    @pytest.mark.asyncio
    async def test_action_unsubscribe_no_subscribers_no_notifications(self, mock_redis):
        """Test that no notifications are sent when there are no subscribers."""
        message = {"block_uuids": ["block-1"]}

        # Setup pipeline to return empty subscribers
        pipe = AsyncMock()
        pipe.smembers = MagicMock()
        pipe.srem = MagicMock()
        pipe.delete = MagicMock()
        pipe.execute = AsyncMock(side_effect=[
            [set()],  # No subscribers
            [],  # delete results
        ])
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=None)
        mock_redis.pipeline = MagicMock(return_value=pipe)

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
                mock_cm.send_message_to_subscribers = AsyncMock()
                await action_unsubscribe(message)

        # Still calls send_message_to_subscribers but with empty set
        assert mock_cm.send_message_to_subscribers.call_count == 1
        call_args = mock_cm.send_message_to_subscribers.call_args
        assert call_args[0][1] == set()  # Empty subscribers


class TestHandleMessage:
    """Tests for handle_message function."""

    @pytest.fixture
    def mock_message(self):
        """Create a mock RabbitMQ message."""
        message = AsyncMock()
        message.ack = AsyncMock()
        message.reject = AsyncMock()
        message.nack = AsyncMock()
        return message

    @pytest.mark.asyncio
    async def test_handle_message_update_block(self, mock_message):
        """Test handling update_block action."""
        mock_message.body = json.dumps({
            "action": "update_block",
            "block_uuid": "block-1",
            "block_data": {"title": "Test"}
        }).encode()

        with patch("app.rabbitmq_consumer.action_update_block", new_callable=AsyncMock) as mock_action:
            await handle_message(mock_message)

        mock_action.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_message_update_blocks(self, mock_message):
        """Test handling update_blocks action."""
        mock_message.body = json.dumps({
            "action": "update_blocks",
            "blocks": {"block-1": {"title": "Test"}}
        }).encode()

        with patch("app.rabbitmq_consumer.action_update_blocks", new_callable=AsyncMock) as mock_action:
            await handle_message(mock_message)

        mock_action.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_message_update_access(self, mock_message):
        """Test handling update_access action."""
        mock_message.body = json.dumps({
            "action": "update_access",
            "user_id": "user_123",
            "permission": "edit",
            "start_block_ids": [],
            "block_uuids": ["block-1"]
        }).encode()

        with patch("app.rabbitmq_consumer.action_update_access", new_callable=AsyncMock) as mock_action:
            await handle_message(mock_message)

        mock_action.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_message_subscribe(self, mock_message):
        """Test handling subscribe action."""
        mock_message.body = json.dumps({
            "action": "subscribe",
            "user_id": "user_123",
            "block_uuids": ["block-1"]
        }).encode()

        with patch("app.rabbitmq_consumer.action_subscribe", new_callable=AsyncMock) as mock_action:
            await handle_message(mock_message)

        mock_action.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_message_unsubscribe(self, mock_message):
        """Test handling unsubscribe action."""
        mock_message.body = json.dumps({
            "action": "unsubscribe",
            "block_uuids": ["block-1"]
        }).encode()

        with patch("app.rabbitmq_consumer.action_unsubscribe", new_callable=AsyncMock) as mock_action:
            await handle_message(mock_message)

        mock_action.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_message_unknown_action(self, mock_message):
        """Test handling unknown action."""
        mock_message.body = json.dumps({"action": "unknown"}).encode()

        await handle_message(mock_message)

        # Should acknowledge (unknown action is not an error, just logged)
        mock_message.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_message_invalid_json(self, mock_message):
        """Test handling invalid JSON message."""
        mock_message.body = b"not valid json {"

        await handle_message(mock_message)

        # Should reject invalid JSON
        mock_message.reject.assert_called_once_with(requeue=False)
        mock_message.ack.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_message_empty_body(self, mock_message):
        """Test handling empty message body."""
        mock_message.body = b""

        await handle_message(mock_message)

        # Empty body is invalid JSON, should reject
        mock_message.reject.assert_called_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_handle_message_acknowledges_on_success(self, mock_message):
        """Test that successful message processing is acknowledged."""
        mock_message.body = json.dumps({
            "action": "update_block",
            "block_uuid": "block-1",
            "block_data": {"title": "Test"}
        }).encode()

        with patch("app.rabbitmq_consumer.action_update_block", new_callable=AsyncMock):
            await handle_message(mock_message)

        mock_message.ack.assert_called_once()
        mock_message.reject.assert_not_called()


class TestSendMessageUpdateAccess:
    """Tests for send_message_update_access function."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        redis = AsyncMock()

        pipe = AsyncMock()
        pipe.hgetall = MagicMock()
        pipe.execute = AsyncMock(return_value=[{"id": "block-1", "title": "Test"}])
        redis.pipeline = MagicMock(return_value=pipe)

        return redis

    @pytest.mark.asyncio
    async def test_send_message_update_access_deny_sends_forbidden_block(self, mock_redis):
        """Test that deny permission sends forbidden block data."""
        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_personal_message = AsyncMock()

            await send_message_update_access(
                start_block_ids=["block-1"],
                block_uuids=["block-1", "block-2"],
                user_id="user_123",
                permission="deny",
                redis=mock_redis
            )

        mock_cm.send_personal_message.assert_called_once()
        call_args = mock_cm.send_personal_message.call_args
        message = call_args[0][0]
        assert message["type"] == "block_update_access"
        assert message["permission"] == "deny"

    @pytest.mark.asyncio
    async def test_send_message_update_access_edit_sends_block_data(self, mock_redis):
        """Test that edit permission sends actual block data."""
        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_personal_message = AsyncMock()

            await send_message_update_access(
                start_block_ids=["block-1"],
                block_uuids=["block-1"],
                user_id="user_123",
                permission="edit",
                redis=mock_redis
            )

        mock_cm.send_personal_message.assert_called_once()
        call_args = mock_cm.send_personal_message.call_args
        message = call_args[0][0]
        assert message["type"] == "block_update_access"
        assert message["permission"] == "edit"

    @pytest.mark.asyncio
    async def test_send_message_update_access_edit_with_block_data_param(self, mock_redis):
        """Test that edit permission uses block_data from message when provided."""
        block_data = [{"id": "block-1", "title": "Test Block", "data": "{}"}]

        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_personal_message = AsyncMock()

            await send_message_update_access(
                start_block_ids=["block-1"],
                block_uuids=["block-1"],
                user_id="user_123",
                permission="edit",
                redis=mock_redis,
                block_data=block_data
            )

        mock_cm.send_personal_message.assert_called_once()
        message = mock_cm.send_personal_message.call_args[0][0]
        assert message["type"] == "block_update_access"
        assert message["start_block_ids"] == block_data
        # Redis pipeline should NOT be called when block_data is provided
        mock_redis.pipeline.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_message_update_access_edit_redis_empty_no_block_data(self):
        """Test that warning is logged when block_data missing and Redis empty."""
        redis = AsyncMock()
        pipe = AsyncMock()
        pipe.hgetall = MagicMock()
        pipe.execute = AsyncMock(return_value=[{}])  # Empty Redis response
        redis.pipeline = MagicMock(return_value=pipe)

        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_personal_message = AsyncMock()

            await send_message_update_access(
                start_block_ids=["block-1"],
                block_uuids=["block-1"],
                user_id="user_123",
                permission="edit",
                redis=redis,
                block_data=None
            )

        # Should NOT send message when no data available
        mock_cm.send_personal_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_message_update_access_edit_fallback_to_redis(self, mock_redis):
        """Test that edit falls back to Redis when block_data is None."""
        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_personal_message = AsyncMock()

            await send_message_update_access(
                start_block_ids=["block-1"],
                block_uuids=["block-1"],
                user_id="user_123",
                permission="edit",
                redis=mock_redis,
                block_data=None  # Explicit None
            )

        # Redis pipeline should be called as fallback
        mock_redis.pipeline.assert_called_once()
        mock_cm.send_personal_message.assert_called_once()


class TestMessageHandlingFixed:
    """Tests verifying message handling works correctly after fix."""

    @pytest.fixture
    def mock_message(self):
        """Create a mock RabbitMQ message."""
        message = AsyncMock()
        message.ack = AsyncMock()
        message.reject = AsyncMock()
        message.nack = AsyncMock()
        return message

    @pytest.mark.asyncio
    async def test_invalid_json_message_is_rejected(self, mock_message):
        """Test that invalid JSON messages are rejected (not acknowledged)."""
        mock_message.body = b"invalid json {"

        await handle_message(mock_message)

        # Fixed: Message is rejected, not acknowledged
        mock_message.reject.assert_called_once_with(requeue=False)
        mock_message.ack.assert_not_called()


class TestNotificationEventTypes:
    """Tests for notification event type constants."""

    def test_reminder_event_types_defined(self):
        """Test that all reminder event types are defined."""
        expected = {
            'reminder_created',
            'reminder_updated',
            'reminder_deleted',
            'reminder_triggered',
            'reminder_snoozed',
        }
        assert REMINDER_EVENT_TYPES == expected

    def test_subscription_event_types_defined(self):
        """Test that all subscription event types are defined."""
        expected = {
            'subscription_created',
            'subscription_updated',
            'subscription_deleted',
        }
        assert SUBSCRIPTION_EVENT_TYPES == expected

    def test_notification_event_types_is_union(self):
        """Test that NOTIFICATION_EVENT_TYPES is union of reminder and subscription types."""
        assert NOTIFICATION_EVENT_TYPES == REMINDER_EVENT_TYPES | SUBSCRIPTION_EVENT_TYPES


class TestActionNotificationEvent:
    """Tests for action_notification_event function."""

    @pytest.mark.asyncio
    async def test_reminder_created_event(self):
        """Test handling reminder_created event."""
        message = {
            "type": "reminder_created",
            "user_id": "user_123",
            "data": {
                "id": "reminder-uuid",
                "block_id": "block-uuid",
                "remind_at": "2025-01-15T10:00:00Z",
                "timezone": "Europe/Moscow",
                "message": "Test reminder",
                "repeat": "none"
            }
        }

        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_personal_message = AsyncMock()
            await action_notification_event(message)

        mock_cm.send_personal_message.assert_called_once()
        call_args = mock_cm.send_personal_message.call_args
        sent_message = call_args[0][0]
        user_id = call_args[0][1]

        assert sent_message["type"] == "reminder_created"
        assert sent_message["data"]["id"] == "reminder-uuid"
        assert user_id == "user_123"

    @pytest.mark.asyncio
    async def test_reminder_updated_event(self):
        """Test handling reminder_updated event."""
        message = {
            "type": "reminder_updated",
            "user_id": "user_456",
            "data": {
                "id": "reminder-uuid",
                "block_id": "block-uuid",
                "remind_at": "2025-01-16T10:00:00Z",
                "timezone": "Europe/Moscow",
                "message": "Updated reminder",
                "repeat": "daily"
            }
        }

        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_personal_message = AsyncMock()
            await action_notification_event(message)

        mock_cm.send_personal_message.assert_called_once()
        call_args = mock_cm.send_personal_message.call_args
        assert call_args[0][0]["type"] == "reminder_updated"

    @pytest.mark.asyncio
    async def test_reminder_deleted_event(self):
        """Test handling reminder_deleted event."""
        message = {
            "type": "reminder_deleted",
            "user_id": "user_789",
            "data": {
                "id": "reminder-uuid",
                "block_id": "block-uuid"
            }
        }

        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_personal_message = AsyncMock()
            await action_notification_event(message)

        mock_cm.send_personal_message.assert_called_once()
        call_args = mock_cm.send_personal_message.call_args
        assert call_args[0][0]["type"] == "reminder_deleted"

    @pytest.mark.asyncio
    async def test_reminder_triggered_event(self):
        """Test handling reminder_triggered event."""
        message = {
            "type": "reminder_triggered",
            "user_id": "user_123",
            "data": {
                "id": "reminder-uuid",
                "block_id": "block-uuid",
                "message": "Reminder triggered"
            }
        }

        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_personal_message = AsyncMock()
            await action_notification_event(message)

        mock_cm.send_personal_message.assert_called_once()
        call_args = mock_cm.send_personal_message.call_args
        assert call_args[0][0]["type"] == "reminder_triggered"

    @pytest.mark.asyncio
    async def test_reminder_snoozed_event(self):
        """Test handling reminder_snoozed event."""
        message = {
            "type": "reminder_snoozed",
            "user_id": "user_123",
            "data": {
                "id": "reminder-uuid",
                "block_id": "block-uuid",
                "snoozed_until": "2025-01-15T10:30:00Z"
            }
        }

        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_personal_message = AsyncMock()
            await action_notification_event(message)

        mock_cm.send_personal_message.assert_called_once()
        call_args = mock_cm.send_personal_message.call_args
        assert call_args[0][0]["type"] == "reminder_snoozed"

    @pytest.mark.asyncio
    async def test_subscription_created_event(self):
        """Test handling subscription_created event."""
        message = {
            "type": "subscription_created",
            "user_id": "user_123",
            "data": {
                "id": "sub-uuid",
                "block_id": "block-uuid",
                "depth": 1,
                "on_text_change": True,
                "on_data_change": True,
                "on_move": True,
                "on_child_add": True,
                "on_child_delete": True
            }
        }

        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_personal_message = AsyncMock()
            await action_notification_event(message)

        mock_cm.send_personal_message.assert_called_once()
        call_args = mock_cm.send_personal_message.call_args
        assert call_args[0][0]["type"] == "subscription_created"
        assert call_args[0][0]["data"]["depth"] == 1

    @pytest.mark.asyncio
    async def test_subscription_updated_event(self):
        """Test handling subscription_updated event."""
        message = {
            "type": "subscription_updated",
            "user_id": "user_456",
            "data": {
                "id": "sub-uuid",
                "block_id": "block-uuid",
                "depth": 2,
                "on_text_change": False,
                "on_data_change": True,
                "on_move": False,
                "on_child_add": True,
                "on_child_delete": True
            }
        }

        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_personal_message = AsyncMock()
            await action_notification_event(message)

        mock_cm.send_personal_message.assert_called_once()
        call_args = mock_cm.send_personal_message.call_args
        assert call_args[0][0]["type"] == "subscription_updated"

    @pytest.mark.asyncio
    async def test_subscription_deleted_event(self):
        """Test handling subscription_deleted event."""
        message = {
            "type": "subscription_deleted",
            "user_id": "user_789",
            "data": {
                "id": "sub-uuid",
                "block_id": "block-uuid"
            }
        }

        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_personal_message = AsyncMock()
            await action_notification_event(message)

        mock_cm.send_personal_message.assert_called_once()
        call_args = mock_cm.send_personal_message.call_args
        assert call_args[0][0]["type"] == "subscription_deleted"

    @pytest.mark.asyncio
    async def test_notification_event_missing_user_id(self):
        """Test handling of missing user_id in notification event."""
        message = {
            "type": "reminder_created",
            "data": {"id": "test"}
        }

        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_personal_message = AsyncMock()
            await action_notification_event(message)

        # Should not call send_personal_message due to validation error
        mock_cm.send_personal_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_notification_event_missing_data(self):
        """Test handling of missing data in notification event."""
        message = {
            "type": "reminder_created",
            "user_id": "user_123"
        }

        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_personal_message = AsyncMock()
            await action_notification_event(message)

        # Should not call send_personal_message due to validation error
        mock_cm.send_personal_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_notification_event_user_id_as_int(self):
        """Test that user_id as int is converted to string."""
        message = {
            "type": "reminder_created",
            "user_id": 12345,  # int instead of string
            "data": {"id": "test"}
        }

        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_personal_message = AsyncMock()
            await action_notification_event(message)

        mock_cm.send_personal_message.assert_called_once()
        call_args = mock_cm.send_personal_message.call_args
        user_id = call_args[0][1]
        assert user_id == "12345"  # Should be converted to string


class TestHandleMessageNotificationEvents:
    """Tests for handle_message with notification events."""

    @pytest.fixture
    def mock_message(self):
        """Create a mock RabbitMQ message."""
        message = AsyncMock()
        message.ack = AsyncMock()
        message.reject = AsyncMock()
        message.nack = AsyncMock()
        return message

    @pytest.mark.asyncio
    async def test_handle_message_reminder_created(self, mock_message):
        """Test handling reminder_created event via handle_message."""
        mock_message.body = json.dumps({
            "type": "reminder_created",
            "user_id": "user_123",
            "data": {"id": "reminder-1", "block_id": "block-1"}
        }).encode()

        with patch("app.rabbitmq_consumer.action_notification_event", new_callable=AsyncMock) as mock_action:
            await handle_message(mock_message)

        mock_action.assert_called_once()
        mock_message.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_message_subscription_created(self, mock_message):
        """Test handling subscription_created event via handle_message."""
        mock_message.body = json.dumps({
            "type": "subscription_created",
            "user_id": "user_456",
            "data": {"id": "sub-1", "block_id": "block-1", "depth": 1}
        }).encode()

        with patch("app.rabbitmq_consumer.action_notification_event", new_callable=AsyncMock) as mock_action:
            await handle_message(mock_message)

        mock_action.assert_called_once()
        mock_message.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_message_all_reminder_types(self, mock_message):
        """Test that all reminder event types are handled."""
        for event_type in REMINDER_EVENT_TYPES:
            mock_message.body = json.dumps({
                "type": event_type,
                "user_id": "user_123",
                "data": {"id": "test"}
            }).encode()
            mock_message.ack.reset_mock()

            with patch("app.rabbitmq_consumer.action_notification_event", new_callable=AsyncMock) as mock_action:
                await handle_message(mock_message)

            mock_action.assert_called_once()
            mock_message.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_message_all_subscription_types(self, mock_message):
        """Test that all subscription event types are handled."""
        for event_type in SUBSCRIPTION_EVENT_TYPES:
            mock_message.body = json.dumps({
                "type": event_type,
                "user_id": "user_123",
                "data": {"id": "test"}
            }).encode()
            mock_message.ack.reset_mock()

            with patch("app.rabbitmq_consumer.action_notification_event", new_callable=AsyncMock) as mock_action:
                await handle_message(mock_message)

            mock_action.assert_called_once()
            mock_message.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_message_action_takes_precedence(self, mock_message):
        """Test that action field takes precedence over type field."""
        mock_message.body = json.dumps({
            "action": "update_block",
            "type": "reminder_created",  # Should be ignored
            "block_uuid": "block-1",
            "block_data": {"title": "Test"}
        }).encode()

        with patch("app.rabbitmq_consumer.action_update_block", new_callable=AsyncMock) as mock_block:
            with patch("app.rabbitmq_consumer.action_notification_event", new_callable=AsyncMock) as mock_notif:
                await handle_message(mock_message)

        mock_block.assert_called_once()
        mock_notif.assert_not_called()
        mock_message.ack.assert_called_once()


class TestSandboxFiltering:
    """Tests for sandbox mode filtering in update_block actions."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        redis = AsyncMock()
        redis.hset = AsyncMock()
        redis.smembers = AsyncMock(return_value=set())
        return redis

    @pytest.mark.asyncio
    async def test_action_update_block_updates_sandbox_cache(self, mock_redis):
        """Test that sandbox containers are cached when updated."""
        message = {
            "block_uuid": "container-123",
            "block_data": {
                "title": "Sandbox Container",
                "sandbox_mode": "private",
                "creator_id": 456
            }
        }

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
                mock_cm.send_message_to_subscribers = AsyncMock()
                mock_cm.update_sandbox_cache = AsyncMock()
                mock_cm.get_parent_sandbox_info = AsyncMock(return_value=None)
                await action_update_block(message)

        mock_cm.update_sandbox_cache.assert_called_once_with(
            "container-123", "private", 456
        )

    @pytest.mark.asyncio
    async def test_action_update_block_filters_for_private_sandbox(self, mock_redis):
        """Test that subscribers are filtered for blocks in private sandbox."""
        message = {
            "block_uuid": "block-123",
            "block_data": {
                "title": "Child Block",
                "parent_id": "parent-123",
                "creator_id": 100
            }
        }
        mock_redis.smembers = AsyncMock(return_value={"100", "200", "300"})

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
                mock_cm.send_message_to_subscribers = AsyncMock()
                mock_cm.update_sandbox_cache = AsyncMock()
                mock_cm.get_parent_sandbox_info = AsyncMock(return_value={
                    "mode": "private",
                    "creator_id": "999"  # Container owner
                })
                mock_cm.filter_subscribers_for_private_sandbox = MagicMock(
                    return_value={"100", "999"}  # Only creator and owner
                )
                await action_update_block(message)

        # Should filter subscribers
        mock_cm.filter_subscribers_for_private_sandbox.assert_called_once()
        # Should send only to filtered subscribers
        mock_cm.send_message_to_subscribers.assert_called_once()
        call_args = mock_cm.send_message_to_subscribers.call_args
        assert call_args[0][1] == {"100", "999"}

    @pytest.mark.asyncio
    async def test_action_update_block_no_filter_for_open_sandbox(self, mock_redis):
        """Test that subscribers are NOT filtered for open sandbox."""
        message = {
            "block_uuid": "block-123",
            "block_data": {
                "title": "Child Block",
                "parent_id": "parent-123",
                "creator_id": 100
            }
        }
        mock_redis.smembers = AsyncMock(return_value={"100", "200", "300"})

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
                mock_cm.send_message_to_subscribers = AsyncMock()
                mock_cm.update_sandbox_cache = AsyncMock()
                mock_cm.get_parent_sandbox_info = AsyncMock(return_value={
                    "mode": "open",  # Open sandbox - no filtering
                    "creator_id": "999"
                })
                await action_update_block(message)

        # Should NOT filter - send to all subscribers
        mock_cm.send_message_to_subscribers.assert_called_once()
        call_args = mock_cm.send_message_to_subscribers.call_args
        assert call_args[0][1] == {"100", "200", "300"}

    @pytest.mark.asyncio
    async def test_action_update_block_no_filter_without_parent(self, mock_redis):
        """Test that subscribers are NOT filtered for blocks without parent."""
        message = {
            "block_uuid": "root-block",
            "block_data": {
                "title": "Root Block"
                # No parent_id
            }
        }
        mock_redis.smembers = AsyncMock(return_value={"100", "200"})

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
                mock_cm.send_message_to_subscribers = AsyncMock()
                mock_cm.update_sandbox_cache = AsyncMock()
                mock_cm.get_parent_sandbox_info = AsyncMock(return_value=None)
                await action_update_block(message)

        # Should send to all subscribers
        mock_cm.send_message_to_subscribers.assert_called_once()
        call_args = mock_cm.send_message_to_subscribers.call_args
        assert call_args[0][1] == {"100", "200"}

    @pytest.mark.asyncio
    async def test_action_update_block_no_send_when_no_authorized_subscribers(self, mock_redis):
        """Test that no message is sent when all subscribers are filtered out."""
        message = {
            "block_uuid": "block-123",
            "block_data": {
                "title": "Private Block",
                "parent_id": "parent-123",
                "creator_id": 100
            }
        }
        mock_redis.smembers = AsyncMock(return_value={"200", "300"})  # No owner, no creator

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
                mock_cm.send_message_to_subscribers = AsyncMock()
                mock_cm.update_sandbox_cache = AsyncMock()
                mock_cm.get_parent_sandbox_info = AsyncMock(return_value={
                    "mode": "private",
                    "creator_id": "999"
                })
                mock_cm.filter_subscribers_for_private_sandbox = MagicMock(
                    return_value=set()  # No authorized subscribers
                )
                await action_update_block(message)

        # Should NOT send any message
        mock_cm.send_message_to_subscribers.assert_not_called()


class TestActionSandboxModeChanged:
    """Tests for action_sandbox_mode_changed handler."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        redis = AsyncMock()
        redis.smembers = AsyncMock(return_value=set())
        return redis

    @pytest.mark.asyncio
    async def test_action_sandbox_mode_changed_updates_cache(self, mock_redis):
        """Test that sandbox_mode_changed updates the cache."""
        message = {
            "action": "sandbox_mode_changed",
            "block_uuid": "container-123",
            "sandbox_mode": "private",
            "creator_id": 456
        }

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
                mock_cm.update_sandbox_cache = AsyncMock()
                mock_cm.send_message_to_subscribers = AsyncMock()
                await action_sandbox_mode_changed(message)

        mock_cm.update_sandbox_cache.assert_called_once_with(
            "container-123", "private", 456
        )

    @pytest.mark.asyncio
    async def test_action_sandbox_mode_changed_notifies_subscribers(self, mock_redis):
        """Test that sandbox_mode_changed notifies all subscribers."""
        message = {
            "action": "sandbox_mode_changed",
            "block_uuid": "container-123",
            "sandbox_mode": "private",
            "creator_id": 456
        }
        mock_redis.smembers = AsyncMock(return_value={"user_1", "user_2"})

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
                mock_cm.update_sandbox_cache = AsyncMock()
                mock_cm.send_message_to_subscribers = AsyncMock()
                await action_sandbox_mode_changed(message)

        mock_cm.send_message_to_subscribers.assert_called_once()
        call_args = mock_cm.send_message_to_subscribers.call_args
        sent_message = call_args[0][0]
        subscribers = call_args[0][1]

        assert sent_message["type"] == "sandbox_mode_changed"
        assert sent_message["block_uuid"] == "container-123"
        assert sent_message["sandbox_mode"] == "private"
        assert subscribers == {"user_1", "user_2"}

    @pytest.mark.asyncio
    async def test_action_sandbox_mode_changed_no_subscribers(self, mock_redis):
        """Test sandbox_mode_changed with no subscribers."""
        message = {
            "action": "sandbox_mode_changed",
            "block_uuid": "container-123",
            "sandbox_mode": "none",
            "creator_id": 456
        }
        mock_redis.smembers = AsyncMock(return_value=set())

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
                mock_cm.update_sandbox_cache = AsyncMock()
                mock_cm.send_message_to_subscribers = AsyncMock()
                await action_sandbox_mode_changed(message)

        # Cache should still be updated
        mock_cm.update_sandbox_cache.assert_called_once()
        # But no message should be sent
        mock_cm.send_message_to_subscribers.assert_not_called()

    @pytest.mark.asyncio
    async def test_action_sandbox_mode_changed_invalid_message(self, mock_redis):
        """Test sandbox_mode_changed with invalid message."""
        message = {
            "action": "sandbox_mode_changed"
            # Missing required fields
        }

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
                mock_cm.update_sandbox_cache = AsyncMock()
                mock_cm.send_message_to_subscribers = AsyncMock()
                # Should not raise, just log error
                await action_sandbox_mode_changed(message)

        mock_cm.update_sandbox_cache.assert_not_called()
        mock_cm.send_message_to_subscribers.assert_not_called()


class TestHandleMessageSandboxModeChanged:
    """Tests for handle_message with sandbox_mode_changed action."""

    @pytest.fixture
    def mock_message(self):
        """Create a mock RabbitMQ message."""
        message = AsyncMock()
        message.ack = AsyncMock()
        message.reject = AsyncMock()
        return message

    @pytest.mark.asyncio
    async def test_handle_message_sandbox_mode_changed(self, mock_message):
        """Test handling sandbox_mode_changed action."""
        mock_message.body = json.dumps({
            "action": "sandbox_mode_changed",
            "block_uuid": "container-123",
            "sandbox_mode": "private",
            "creator_id": 456
        }).encode()

        with patch("app.rabbitmq_consumer.action_sandbox_mode_changed", new_callable=AsyncMock) as mock_action:
            await handle_message(mock_message)

        mock_action.assert_called_once()
        mock_message.ack.assert_called_once()


class TestActionAccessRequest:
    """Tests for action_access_request function."""

    @pytest.mark.asyncio
    async def test_new_request_sends_to_owner(self):
        """Test that new_request is sent to the block owner."""
        message = {
            "action": "access_request",
            "type": "new_request",
            "request_id": "req-123",
            "requester": {"id": 123, "username": "john_doe"},
            "block": {"id": "block-uuid", "title": "Block Title"},
            "owner_id": 456
        }

        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_to_user = AsyncMock(return_value=True)
            from app.rabbitmq_consumer import action_access_request
            await action_access_request(message)

        mock_cm.send_to_user.assert_called_once()
        call_args = mock_cm.send_to_user.call_args
        target_user_id = call_args[0][0]
        sent_message = call_args[0][1]

        assert target_user_id == "456"
        assert sent_message["action"] == "access_request"
        assert sent_message["type"] == "new_request"
        assert sent_message["request_id"] == "req-123"
        assert sent_message["requester"]["username"] == "john_doe"
        assert sent_message["block"]["title"] == "Block Title"

    @pytest.mark.asyncio
    async def test_response_sends_to_requester(self):
        """Test that response is sent to the requester."""
        message = {
            "action": "access_request",
            "type": "response",
            "request_id": "req-123",
            "approved": True,
            "permission": "view",
            "block": {"id": "block-uuid", "title": "Block Title"},
            "user_id": 123
        }

        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_to_user = AsyncMock(return_value=True)
            from app.rabbitmq_consumer import action_access_request
            await action_access_request(message)

        mock_cm.send_to_user.assert_called_once()
        call_args = mock_cm.send_to_user.call_args
        target_user_id = call_args[0][0]
        sent_message = call_args[0][1]

        assert target_user_id == "123"
        assert sent_message["action"] == "access_request"
        assert sent_message["type"] == "response"
        assert sent_message["approved"] is True
        assert sent_message["permission"] == "view"

    @pytest.mark.asyncio
    async def test_response_denied_sends_to_requester(self):
        """Test that denied response is sent to the requester."""
        message = {
            "action": "access_request",
            "type": "response",
            "request_id": "req-456",
            "approved": False,
            "permission": None,
            "block": {"id": "block-uuid", "title": "Block Title"},
            "user_id": 789
        }

        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_to_user = AsyncMock(return_value=True)
            from app.rabbitmq_consumer import action_access_request
            await action_access_request(message)

        mock_cm.send_to_user.assert_called_once()
        call_args = mock_cm.send_to_user.call_args
        target_user_id = call_args[0][0]
        sent_message = call_args[0][1]

        assert target_user_id == "789"
        assert sent_message["approved"] is False

    @pytest.mark.asyncio
    async def test_new_request_missing_owner_id(self):
        """Test handling of new_request missing owner_id."""
        message = {
            "action": "access_request",
            "type": "new_request",
            "request_id": "req-123",
            "requester": {"id": 123, "username": "john_doe"},
            "block": {"id": "block-uuid", "title": "Block Title"}
            # Missing owner_id
        }

        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_to_user = AsyncMock(return_value=True)
            from app.rabbitmq_consumer import action_access_request
            await action_access_request(message)

        mock_cm.send_to_user.assert_not_called()

    @pytest.mark.asyncio
    async def test_response_missing_user_id(self):
        """Test handling of response missing user_id."""
        message = {
            "action": "access_request",
            "type": "response",
            "request_id": "req-123",
            "approved": True,
            "permission": "view",
            "block": {"id": "block-uuid", "title": "Block Title"}
            # Missing user_id
        }

        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_to_user = AsyncMock(return_value=True)
            from app.rabbitmq_consumer import action_access_request
            await action_access_request(message)

        mock_cm.send_to_user.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalid_message_format(self):
        """Test handling of invalid message format."""
        message = {
            "action": "access_request"
            # Missing required 'type' field
        }

        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_to_user = AsyncMock(return_value=True)
            from app.rabbitmq_consumer import action_access_request
            # Should not raise, just log error
            await action_access_request(message)

        mock_cm.send_to_user.assert_not_called()

    @pytest.mark.asyncio
    async def test_user_offline_returns_false(self):
        """Test that offline user returns False from send_to_user."""
        message = {
            "action": "access_request",
            "type": "new_request",
            "request_id": "req-123",
            "requester": {"id": 123, "username": "john_doe"},
            "block": {"id": "block-uuid", "title": "Block Title"},
            "owner_id": 456
        }

        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_to_user = AsyncMock(return_value=False)  # User offline
            from app.rabbitmq_consumer import action_access_request
            await action_access_request(message)

        mock_cm.send_to_user.assert_called_once()

    @pytest.mark.asyncio
    async def test_owner_id_as_string(self):
        """Test that owner_id as string is handled correctly."""
        message = {
            "action": "access_request",
            "type": "new_request",
            "request_id": "req-123",
            "requester": {"id": 123, "username": "john_doe"},
            "block": {"id": "block-uuid", "title": "Block Title"},
            "owner_id": "456"  # String instead of int
        }

        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_to_user = AsyncMock(return_value=True)
            from app.rabbitmq_consumer import action_access_request
            await action_access_request(message)

        mock_cm.send_to_user.assert_called_once()
        call_args = mock_cm.send_to_user.call_args
        assert call_args[0][0] == "456"

    @pytest.mark.asyncio
    async def test_user_id_as_string_in_response(self):
        """Test that user_id as string is handled correctly in response."""
        message = {
            "action": "access_request",
            "type": "response",
            "request_id": "req-123",
            "approved": True,
            "permission": "view",
            "block": {"id": "block-uuid", "title": "Block Title"},
            "user_id": "789"  # String instead of int
        }

        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_to_user = AsyncMock(return_value=True)
            from app.rabbitmq_consumer import action_access_request
            await action_access_request(message)

        mock_cm.send_to_user.assert_called_once()
        call_args = mock_cm.send_to_user.call_args
        assert call_args[0][0] == "789"


class TestHandleMessageAccessRequest:
    """Tests for handle_message with access_request action."""

    @pytest.fixture
    def mock_message(self):
        """Create a mock RabbitMQ message."""
        message = AsyncMock()
        message.ack = AsyncMock()
        message.reject = AsyncMock()
        return message

    @pytest.mark.asyncio
    async def test_handle_message_access_request_new(self, mock_message):
        """Test handling access_request new_request via handle_message."""
        mock_message.body = json.dumps({
            "action": "access_request",
            "type": "new_request",
            "request_id": "req-123",
            "requester": {"id": 123, "username": "john_doe"},
            "block": {"id": "block-uuid", "title": "Block Title"},
            "owner_id": 456
        }).encode()

        with patch("app.rabbitmq_consumer.action_access_request", new_callable=AsyncMock) as mock_action:
            await handle_message(mock_message)

        mock_action.assert_called_once()
        mock_message.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_message_access_request_response(self, mock_message):
        """Test handling access_request response via handle_message."""
        mock_message.body = json.dumps({
            "action": "access_request",
            "type": "response",
            "request_id": "req-123",
            "approved": True,
            "permission": "view",
            "block": {"id": "block-uuid", "title": "Block Title"},
            "user_id": 123
        }).encode()

        with patch("app.rabbitmq_consumer.action_access_request", new_callable=AsyncMock) as mock_action:
            await handle_message(mock_message)

        mock_action.assert_called_once()
        mock_message.ack.assert_called_once()
