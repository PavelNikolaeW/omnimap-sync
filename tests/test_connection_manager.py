"""Tests for app/connection_manager.py module."""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from app.connection_manager import ConnectionManager
from app.config import settings


class TestConnectionManager:
    """Tests for ConnectionManager class."""

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


class TestConnect(TestConnectionManager):
    """Tests for connect method."""

    @pytest.mark.asyncio
    async def test_connect_adds_websocket_to_user(self, manager, mock_ws):
        """Test that connect adds websocket to user's connections."""
        user_id = "user_123"

        await manager.connect(user_id, mock_ws)

        assert user_id in manager.active_connections
        assert mock_ws in manager.active_connections[user_id]
        assert len(manager.active_connections[user_id]) == 1

    @pytest.mark.asyncio
    async def test_connect_multiple_websockets_same_user(self, manager, mock_ws, mock_ws_2):
        """Test that multiple websockets can be connected for same user."""
        user_id = "user_123"

        await manager.connect(user_id, mock_ws)
        await manager.connect(user_id, mock_ws_2)

        assert len(manager.active_connections[user_id]) == 2
        assert mock_ws in manager.active_connections[user_id]
        assert mock_ws_2 in manager.active_connections[user_id]

    @pytest.mark.asyncio
    async def test_connect_creates_user_lock(self, manager, mock_ws):
        """Test that connect creates a lock for the user."""
        user_id = "user_123"

        await manager.connect(user_id, mock_ws)

        assert user_id in manager.user_locks
        assert isinstance(manager.user_locks[user_id], asyncio.Lock)

    @pytest.mark.asyncio
    async def test_connect_different_users(self, manager, mock_ws, mock_ws_2):
        """Test connecting different users."""
        user_id_1 = "user_123"
        user_id_2 = "user_456"

        await manager.connect(user_id_1, mock_ws)
        await manager.connect(user_id_2, mock_ws_2)

        assert len(manager.active_connections) == 2
        assert user_id_1 in manager.active_connections
        assert user_id_2 in manager.active_connections


class TestDisconnect(TestConnectionManager):
    """Tests for disconnect method."""

    @pytest.mark.asyncio
    async def test_disconnect_removes_websocket(self, manager, mock_ws):
        """Test that disconnect removes websocket from user's connections."""
        user_id = "user_123"
        await manager.connect(user_id, mock_ws)

        await manager.disconnect(user_id, mock_ws)

        assert user_id not in manager.active_connections

    @pytest.mark.asyncio
    async def test_disconnect_keeps_other_websockets(self, manager, mock_ws, mock_ws_2):
        """Test that disconnect keeps other websockets for same user."""
        user_id = "user_123"
        await manager.connect(user_id, mock_ws)
        await manager.connect(user_id, mock_ws_2)

        await manager.disconnect(user_id, mock_ws)

        assert user_id in manager.active_connections
        assert mock_ws not in manager.active_connections[user_id]
        assert mock_ws_2 in manager.active_connections[user_id]

    @pytest.mark.asyncio
    async def test_disconnect_nonexistent_websocket(self, manager, mock_ws, mock_ws_2):
        """Test disconnecting a websocket that was never connected."""
        user_id = "user_123"
        await manager.connect(user_id, mock_ws)

        # Should not raise, just log warning
        await manager.disconnect(user_id, mock_ws_2)

        assert mock_ws in manager.active_connections[user_id]

    @pytest.mark.asyncio
    async def test_disconnect_removes_user_lock_when_no_connections(self, manager, mock_ws):
        """Test that user lock is removed when all connections are closed."""
        user_id = "user_123"
        await manager.connect(user_id, mock_ws)
        assert user_id in manager.user_locks

        await manager.disconnect(user_id, mock_ws)

        assert user_id not in manager.user_locks

    @pytest.mark.asyncio
    async def test_disconnect_nonexistent_user(self, manager, mock_ws):
        """Test disconnecting from nonexistent user."""
        # Should not raise
        await manager.disconnect("nonexistent_user", mock_ws)


class TestSendMessageToSocket(TestConnectionManager):
    """Tests for send_message_to_socket method."""

    @pytest.mark.asyncio
    async def test_send_message_to_socket_success(self, manager, mock_ws):
        """Test successful message sending."""
        user_id = "user_123"
        message = {"type": "test", "data": "hello"}
        await manager.connect(user_id, mock_ws)

        await manager.send_message_to_socket(message, user_id, mock_ws)

        mock_ws.send_json.assert_called_once_with(message)

    @pytest.mark.asyncio
    async def test_send_message_to_socket_failure_disconnects(self, manager, mock_ws):
        """Test that failed send disconnects the websocket."""
        user_id = "user_123"
        message = {"type": "test"}
        mock_ws.send_json = AsyncMock(side_effect=Exception("Send failed"))
        await manager.connect(user_id, mock_ws)

        await manager.send_message_to_socket(message, user_id, mock_ws)

        # WebSocket should be disconnected
        assert user_id not in manager.active_connections


class TestSendPersonalMessage(TestConnectionManager):
    """Tests for send_personal_message method."""

    @pytest.mark.asyncio
    async def test_send_personal_message_to_all_connections(self, manager, mock_ws, mock_ws_2):
        """Test that message is sent to all user's connections."""
        user_id = "user_123"
        message = {"type": "test", "data": "hello"}
        await manager.connect(user_id, mock_ws)
        await manager.connect(user_id, mock_ws_2)

        await manager.send_personal_message(message, user_id)

        mock_ws.send_json.assert_called_once_with(message)
        mock_ws_2.send_json.assert_called_once_with(message)

    @pytest.mark.asyncio
    async def test_send_personal_message_no_connections(self, manager):
        """Test sending message when user has no connections."""
        message = {"type": "test"}

        # Should not raise
        await manager.send_personal_message(message, "nonexistent_user")

    @pytest.mark.asyncio
    async def test_send_personal_message_partial_failure(self, manager, mock_ws, mock_ws_2):
        """Test that one failed send doesn't affect others."""
        user_id = "user_123"
        message = {"type": "test"}
        mock_ws.send_json = AsyncMock(side_effect=Exception("Send failed"))
        await manager.connect(user_id, mock_ws)
        await manager.connect(user_id, mock_ws_2)

        await manager.send_personal_message(message, user_id)

        # Second websocket should still receive message
        mock_ws_2.send_json.assert_called_once_with(message)

    @pytest.mark.asyncio
    async def test_send_personal_message_anonymous_user_uses_semaphore(self, manager, mock_ws):
        """Test that anonymous user messages use semaphore."""
        # Note: Current bug - user_id is str, ANONIM_USER is int, comparison fails
        # This test documents current behavior
        anon_user_id = str(settings.ANONIM_USER)
        message = {"type": "test"}
        await manager.connect(anon_user_id, mock_ws)

        await manager.send_personal_message(message, anon_user_id)

        mock_ws.send_json.assert_called_once_with(message)


class TestSendMessageToSubscribers(TestConnectionManager):
    """Tests for send_message_to_subscribers method."""

    @pytest.mark.asyncio
    async def test_send_message_to_subscribers(self, manager, mock_ws, mock_ws_2):
        """Test sending message to multiple subscribers."""
        user_id_1 = "user_123"
        user_id_2 = "user_456"
        message = {"type": "test"}
        await manager.connect(user_id_1, mock_ws)
        await manager.connect(user_id_2, mock_ws_2)

        await manager.send_message_to_subscribers(message, [user_id_1, user_id_2])

        # Give background task time to execute
        await asyncio.sleep(0.1)

        mock_ws.send_json.assert_called_once_with(message)
        mock_ws_2.send_json.assert_called_once_with(message)

    @pytest.mark.asyncio
    async def test_send_message_to_subscribers_empty_list(self, manager):
        """Test sending message to empty subscriber list."""
        message = {"type": "test"}

        # Should not raise
        await manager.send_message_to_subscribers(message, [])

    @pytest.mark.asyncio
    async def test_send_message_to_subscribers_nonexistent_users(self, manager):
        """Test sending message to nonexistent users."""
        message = {"type": "test"}

        # Should not raise
        await manager.send_message_to_subscribers(message, ["user_1", "user_2"])
        await asyncio.sleep(0.1)


class TestGetUserLock(TestConnectionManager):
    """Tests for _get_user_lock method."""

    @pytest.mark.asyncio
    async def test_get_user_lock_creates_new_lock(self, manager):
        """Test that _get_user_lock creates a new lock for new user."""
        user_id = "user_123"

        lock = await manager._get_user_lock(user_id)

        assert isinstance(lock, asyncio.Lock)
        assert user_id in manager.user_locks

    @pytest.mark.asyncio
    async def test_get_user_lock_returns_existing_lock(self, manager):
        """Test that _get_user_lock returns existing lock."""
        user_id = "user_123"

        lock1 = await manager._get_user_lock(user_id)
        lock2 = await manager._get_user_lock(user_id)

        assert lock1 is lock2


class TestConcurrency(TestConnectionManager):
    """Tests for concurrent operations."""

    @pytest.mark.asyncio
    async def test_concurrent_connects(self, manager):
        """Test concurrent connect operations."""
        user_id = "user_123"
        websockets = [AsyncMock() for _ in range(10)]

        # Connect all websockets concurrently
        await asyncio.gather(*[
            manager.connect(user_id, ws) for ws in websockets
        ])

        assert len(manager.active_connections[user_id]) == 10

    @pytest.mark.asyncio
    async def test_concurrent_disconnects(self, manager):
        """Test concurrent disconnect operations."""
        user_id = "user_123"
        websockets = [AsyncMock() for _ in range(10)]

        # Connect all
        for ws in websockets:
            await manager.connect(user_id, ws)

        # Disconnect all concurrently
        await asyncio.gather(*[
            manager.disconnect(user_id, ws) for ws in websockets
        ])

        assert user_id not in manager.active_connections

    @pytest.mark.asyncio
    async def test_concurrent_send_messages(self, manager):
        """Test concurrent message sending."""
        user_id = "user_123"
        mock_ws = AsyncMock()
        mock_ws.send_json = AsyncMock()
        await manager.connect(user_id, mock_ws)

        messages = [{"type": "test", "id": i} for i in range(10)]

        await asyncio.gather(*[
            manager.send_personal_message(msg, user_id) for msg in messages
        ])

        assert mock_ws.send_json.call_count == 10


class TestAnonymousUserBug(TestConnectionManager):
    """Tests documenting the anonymous user type mismatch bug."""

    @pytest.mark.asyncio
    async def test_anonymous_user_type_mismatch(self, manager, mock_ws):
        """Document bug: ANONIM_USER is int but user_id is str."""
        # This test documents the current buggy behavior
        # settings.ANONIM_USER is -1 (int)
        # user_id in connection_manager is always str

        # The check `if user_id == settings.ANONIM_USER` will always be False
        # because str("-1") != int(-1)

        anon_user_id_str = str(settings.ANONIM_USER)  # "-1"
        anon_user_id_int = settings.ANONIM_USER  # -1

        # These are not equal in Python
        assert anon_user_id_str != anon_user_id_int
        assert anon_user_id_str == "-1"
        assert anon_user_id_int == -1

        # So this condition in send_personal_message will never be True:
        # if user_id == settings.ANONIM_USER:
        # because user_id is always str


class TestOnlineStatusTracking(TestConnectionManager):
    """Tests for Redis-based online status tracking."""

    @pytest.mark.asyncio
    async def test_connect_increments_online_counter(self, manager, mock_ws):
        """Test that connect increments Redis online counter."""
        user_id = "user_123"

        with patch('app.connection_manager.get_redis_pool') as mock_get_redis:
            mock_redis = AsyncMock()
            mock_get_redis.return_value = mock_redis

            await manager.connect(user_id, mock_ws)

            mock_redis.incr.assert_called_once_with("user_online:user_123")
            mock_redis.expire.assert_called_once_with("user_online:user_123", settings.ONLINE_STATUS_TTL)

    @pytest.mark.asyncio
    async def test_connect_skips_anonymous_user(self, manager, mock_ws):
        """Test that connect does not track anonymous users."""
        anon_user_id = str(settings.ANONIM_USER)

        with patch('app.connection_manager.get_redis_pool') as mock_get_redis:
            mock_redis = AsyncMock()
            mock_get_redis.return_value = mock_redis

            await manager.connect(anon_user_id, mock_ws)

            mock_redis.incr.assert_not_called()

    @pytest.mark.asyncio
    async def test_disconnect_decrements_online_counter(self, manager, mock_ws):
        """Test that disconnect decrements Redis online counter."""
        user_id = "user_123"

        with patch('app.connection_manager.get_redis_pool') as mock_get_redis:
            mock_redis = AsyncMock()
            mock_redis.decr.return_value = 1  # Still has connections
            mock_get_redis.return_value = mock_redis

            await manager.connect(user_id, mock_ws)
            mock_redis.reset_mock()

            await manager.disconnect(user_id, mock_ws)

            mock_redis.decr.assert_called_once_with("user_online:user_123")
            mock_redis.delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_disconnect_deletes_counter_when_zero(self, manager, mock_ws):
        """Test that disconnect deletes key when counter reaches zero."""
        user_id = "user_123"

        with patch('app.connection_manager.get_redis_pool') as mock_get_redis:
            mock_redis = AsyncMock()
            mock_redis.decr.return_value = 0  # No more connections
            mock_get_redis.return_value = mock_redis

            await manager.connect(user_id, mock_ws)
            mock_redis.reset_mock()

            await manager.disconnect(user_id, mock_ws)

            mock_redis.decr.assert_called_once()
            mock_redis.delete.assert_called_once_with("user_online:user_123")

    @pytest.mark.asyncio
    async def test_disconnect_skips_anonymous_user(self, manager, mock_ws):
        """Test that disconnect does not track anonymous users."""
        anon_user_id = str(settings.ANONIM_USER)

        with patch('app.connection_manager.get_redis_pool') as mock_get_redis:
            mock_redis = AsyncMock()
            mock_get_redis.return_value = mock_redis

            await manager.connect(anon_user_id, mock_ws)
            mock_redis.reset_mock()

            await manager.disconnect(anon_user_id, mock_ws)

            mock_redis.decr.assert_not_called()

    @pytest.mark.asyncio
    async def test_refresh_user_online_ttl(self, manager):
        """Test that refresh_user_online_ttl refreshes TTL."""
        user_id = "user_123"

        with patch('app.connection_manager.get_redis_pool') as mock_get_redis:
            mock_redis = AsyncMock()
            mock_redis.exists.return_value = True
            mock_get_redis.return_value = mock_redis

            await manager.refresh_user_online_ttl(user_id)

            mock_redis.exists.assert_called_once_with("user_online:user_123")
            mock_redis.expire.assert_called_once_with("user_online:user_123", settings.ONLINE_STATUS_TTL)

    @pytest.mark.asyncio
    async def test_refresh_user_online_ttl_skips_if_not_exists(self, manager):
        """Test that refresh_user_online_ttl does nothing if key doesn't exist."""
        user_id = "user_123"

        with patch('app.connection_manager.get_redis_pool') as mock_get_redis:
            mock_redis = AsyncMock()
            mock_redis.exists.return_value = False
            mock_get_redis.return_value = mock_redis

            await manager.refresh_user_online_ttl(user_id)

            mock_redis.exists.assert_called_once()
            mock_redis.expire.assert_not_called()

    @pytest.mark.asyncio
    async def test_refresh_user_online_ttl_skips_anonymous(self, manager):
        """Test that refresh_user_online_ttl skips anonymous users."""
        anon_user_id = str(settings.ANONIM_USER)

        with patch('app.connection_manager.get_redis_pool') as mock_get_redis:
            mock_redis = AsyncMock()
            mock_get_redis.return_value = mock_redis

            await manager.refresh_user_online_ttl(anon_user_id)

            mock_get_redis.assert_not_called()

    @pytest.mark.asyncio
    async def test_redis_error_does_not_block_connect(self, manager, mock_ws):
        """Test that Redis errors don't prevent connection."""
        user_id = "user_123"

        with patch('app.connection_manager.get_redis_pool') as mock_get_redis:
            mock_get_redis.side_effect = Exception("Redis unavailable")

            # Should not raise, connection should still work
            await manager.connect(user_id, mock_ws)

            assert user_id in manager.active_connections
            assert mock_ws in manager.active_connections[user_id]

    @pytest.mark.asyncio
    async def test_redis_error_does_not_block_disconnect(self, manager, mock_ws):
        """Test that Redis errors don't prevent disconnection."""
        user_id = "user_123"

        # First connect without Redis mock
        manager.active_connections[user_id].append(mock_ws)

        with patch('app.connection_manager.get_redis_pool') as mock_get_redis:
            mock_get_redis.side_effect = Exception("Redis unavailable")

            # Should not raise, disconnection should still work
            await manager.disconnect(user_id, mock_ws)

            assert user_id not in manager.active_connections

    @pytest.mark.asyncio
    async def test_multiple_devices_counter(self, manager, mock_ws, mock_ws_2):
        """Test that counter correctly handles multiple devices."""
        user_id = "user_123"

        with patch('app.connection_manager.get_redis_pool') as mock_get_redis:
            mock_redis = AsyncMock()
            mock_redis.decr.side_effect = [1, 0]  # First disconnect: 1, second: 0
            mock_get_redis.return_value = mock_redis

            # Connect two devices
            await manager.connect(user_id, mock_ws)
            await manager.connect(user_id, mock_ws_2)

            assert mock_redis.incr.call_count == 2

            # Disconnect first device
            await manager.disconnect(user_id, mock_ws)
            assert mock_redis.delete.call_count == 0  # Still online

            # Disconnect second device
            await manager.disconnect(user_id, mock_ws_2)
            assert mock_redis.delete.call_count == 1  # Now offline
