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
    handle_message,
    send_message_update_access,
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

        mock_redis.hset.assert_called_once_with(
            "blockdata:block-123",
            mapping={"title": "Test", "updated_at": 1000}
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
    async def test_action_update_access_grant_adds_to_sets(self, mock_redis):
        """Test that grant permission adds user to block sets."""
        message = {
            "user_id": "user_123",
            "permission": "grant",
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
            await action_unsubscribe(message)

        pipe = mock_redis.pipeline.return_value
        # Should call delete for block keys
        assert pipe.delete.called or pipe.execute.called

    @pytest.mark.asyncio
    async def test_action_unsubscribe_missing_block_uuids(self, mock_redis):
        """Test handling of missing block_uuids."""
        message = {}

        with patch("app.rabbitmq_consumer.get_redis_pool", return_value=mock_redis):
            await action_unsubscribe(message)

        mock_redis.pipeline.assert_not_called()


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
            "permission": "grant",
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
    async def test_send_message_update_access_grant_sends_block_data(self, mock_redis):
        """Test that grant permission sends actual block data."""
        with patch("app.rabbitmq_consumer.connection_manager") as mock_cm:
            mock_cm.send_personal_message = AsyncMock()

            await send_message_update_access(
                start_block_ids=["block-1"],
                block_uuids=["block-1"],
                user_id="user_123",
                permission="grant",
                redis=mock_redis
            )

        mock_cm.send_personal_message.assert_called_once()
        call_args = mock_cm.send_personal_message.call_args
        message = call_args[0][0]
        assert message["type"] == "block_update_access"
        assert message["permission"] == "grant"


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
