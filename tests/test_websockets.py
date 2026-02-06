"""Tests for app/websockets.py module."""

import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta

import jwt
from fastapi import WebSocketDisconnect

from app.websockets import websocket_endpoint, handle_get_updates, connection_manager
from app.config import settings


class TestWebsocketEndpoint:
    """Tests for websocket_endpoint function."""

    @pytest.fixture
    def mock_ws(self):
        """Create a mock WebSocket."""
        ws = AsyncMock()
        ws.accept = AsyncMock()
        ws.close = AsyncMock()
        ws.send_json = AsyncMock()
        ws.receive_text = AsyncMock()
        ws.query_params = {}
        return ws

    @pytest.fixture
    def valid_token(self):
        """Generate a valid JWT token."""
        payload = {
            "user_id": 12345,
            "exp": datetime.utcnow() + timedelta(hours=1),
        }
        return jwt.encode(payload, settings.jwt_secret_key or "test_secret", algorithm="HS256")

    @pytest.mark.asyncio
    async def test_websocket_endpoint_no_token_closes_connection(self, mock_ws):
        """Test that missing token closes the connection."""
        mock_ws.query_params = {}

        await websocket_endpoint(mock_ws)

        mock_ws.accept.assert_called_once()
        mock_ws.send_json.assert_called_once()
        error_message = mock_ws.send_json.call_args[0][0]
        assert error_message["type"] == "error"
        assert "Token is required" in error_message["message"]
        mock_ws.close.assert_called_once_with(code=1008)

    @pytest.mark.asyncio
    async def test_websocket_endpoint_empty_token_closes_connection(self, mock_ws):
        """Test that empty token closes the connection."""
        mock_ws.query_params = {"token": ""}

        await websocket_endpoint(mock_ws)

        mock_ws.accept.assert_called_once()
        mock_ws.send_json.assert_called()
        mock_ws.close.assert_called_once_with(code=1008)

    @pytest.mark.asyncio
    async def test_websocket_endpoint_valid_token_accepts_connection(self, mock_ws, valid_token):
        """Test that valid token accepts connection."""
        mock_ws.query_params = {"token": valid_token}
        mock_ws.receive_text = AsyncMock(side_effect=WebSocketDisconnect())

        with patch("app.websockets.verify_jwt", return_value=True):
            with patch.object(connection_manager, "connect", new_callable=AsyncMock):
                with patch.object(connection_manager, "disconnect", new_callable=AsyncMock):
                    await websocket_endpoint(mock_ws)

        mock_ws.accept.assert_called_once()

    @pytest.mark.asyncio
    async def test_websocket_endpoint_invalid_jwt_closes_connection(self, mock_ws):
        """Test that invalid JWT closes connection instead of granting access."""
        mock_ws.query_params = {"token": "invalid.jwt.token"}
        mock_ws.receive_text = AsyncMock(side_effect=WebSocketDisconnect())

        with patch("app.websockets.verify_jwt", return_value=False):
            with patch.object(connection_manager, "connect", new_callable=AsyncMock) as mock_connect:
                with patch.object(connection_manager, "disconnect", new_callable=AsyncMock):
                    await websocket_endpoint(mock_ws)

        # Fixed: User should NOT be connected when JWT is invalid
        mock_connect.assert_not_called()
        mock_ws.close.assert_called_once_with(code=1008)

    @pytest.mark.asyncio
    async def test_websocket_endpoint_jwt_without_user_id_closes_connection(self, mock_ws):
        """Test that JWT without user_id closes the connection."""
        # Create token without user_id
        payload = {
            "exp": datetime.utcnow() + timedelta(hours=1),
        }
        token = jwt.encode(payload, settings.jwt_secret_key or "test_secret", algorithm="HS256")
        mock_ws.query_params = {"token": token}

        with patch("app.websockets.verify_jwt", return_value=True):
            await websocket_endpoint(mock_ws)

        mock_ws.close.assert_called_once_with(code=1008)
        # Check that error message was sent
        send_calls = mock_ws.send_json.call_args_list
        assert any("user_id is missing" in str(call) for call in send_calls)

    @pytest.mark.asyncio
    async def test_websocket_endpoint_handles_get_updates_action(self, mock_ws, valid_token):
        """Test that get_updates action is handled."""
        mock_ws.query_params = {"token": valid_token}

        # First call returns message, second raises disconnect
        mock_ws.receive_text = AsyncMock(side_effect=[
            json.dumps({"action": "get_updates", "blocks": []}),
            WebSocketDisconnect()
        ])

        with patch("app.websockets.verify_jwt", return_value=True):
            with patch.object(connection_manager, "connect", new_callable=AsyncMock):
                with patch.object(connection_manager, "disconnect", new_callable=AsyncMock):
                    with patch("app.websockets.handle_get_updates", new_callable=AsyncMock) as mock_handler:
                        await websocket_endpoint(mock_ws)

        mock_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_websocket_endpoint_ping_returns_pong(self, mock_ws, valid_token):
        """Test that ping action returns pong response."""
        mock_ws.query_params = {"token": valid_token}

        mock_ws.receive_text = AsyncMock(side_effect=[
            json.dumps({"action": "ping"}),
            WebSocketDisconnect()
        ])

        with patch("app.websockets.verify_jwt", return_value=True):
            with patch.object(connection_manager, "connect", new_callable=AsyncMock):
                with patch.object(connection_manager, "disconnect", new_callable=AsyncMock):
                    await websocket_endpoint(mock_ws)

        # Check that pong was sent
        send_calls = mock_ws.send_json.call_args_list
        assert any(
            call[0][0].get("type") == "pong"
            for call in send_calls
        )

    @pytest.mark.asyncio
    async def test_websocket_endpoint_unknown_action_returns_error(self, mock_ws, valid_token):
        """Test that unknown action returns error message."""
        mock_ws.query_params = {"token": valid_token}

        mock_ws.receive_text = AsyncMock(side_effect=[
            json.dumps({"action": "unknown_action"}),
            WebSocketDisconnect()
        ])

        with patch("app.websockets.verify_jwt", return_value=True):
            with patch.object(connection_manager, "connect", new_callable=AsyncMock):
                with patch.object(connection_manager, "disconnect", new_callable=AsyncMock):
                    await websocket_endpoint(mock_ws)

        # Check that error was sent
        send_calls = mock_ws.send_json.call_args_list
        assert any(
            call[0][0].get("type") == "error" and "Unknown action" in call[0][0].get("message", "")
            for call in send_calls
        )

    @pytest.mark.asyncio
    async def test_websocket_endpoint_invalid_json_returns_error(self, mock_ws, valid_token):
        """Test that invalid JSON returns error message."""
        mock_ws.query_params = {"token": valid_token}

        mock_ws.receive_text = AsyncMock(side_effect=[
            "not valid json {{{",
            WebSocketDisconnect()
        ])

        with patch("app.websockets.verify_jwt", return_value=True):
            with patch.object(connection_manager, "connect", new_callable=AsyncMock):
                with patch.object(connection_manager, "disconnect", new_callable=AsyncMock):
                    await websocket_endpoint(mock_ws)

        # Check that error was sent
        send_calls = mock_ws.send_json.call_args_list
        assert any(
            call[0][0].get("type") == "error" and "Invalid JSON" in call[0][0].get("message", "")
            for call in send_calls
        )

    @pytest.mark.asyncio
    async def test_websocket_endpoint_disconnect_calls_manager_disconnect(self, mock_ws, valid_token):
        """Test that disconnect properly calls connection_manager.disconnect."""
        mock_ws.query_params = {"token": valid_token}
        mock_ws.receive_text = AsyncMock(side_effect=WebSocketDisconnect())

        with patch("app.websockets.verify_jwt", return_value=True):
            with patch.object(connection_manager, "connect", new_callable=AsyncMock):
                with patch.object(connection_manager, "disconnect", new_callable=AsyncMock) as mock_disconnect:
                    await websocket_endpoint(mock_ws)

        mock_disconnect.assert_called_once()


class TestHandleGetUpdates:
    """Tests for handle_get_updates function."""

    @pytest.fixture
    def mock_ws(self):
        """Create a mock WebSocket."""
        ws = AsyncMock()
        ws.send_json = AsyncMock()
        return ws

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        redis = AsyncMock()
        redis.smembers = AsyncMock(return_value=set())
        redis.hgetall = AsyncMock(return_value={})

        pipe = AsyncMock()
        pipe.hgetall = MagicMock()
        pipe.execute = AsyncMock(return_value=[])
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=None)
        redis.pipeline = MagicMock(return_value=pipe)

        return redis

    @pytest.mark.asyncio
    async def test_handle_get_updates_invalid_blocks_type(self, mock_ws):
        """Test that non-list blocks returns error."""
        with patch("app.websockets.get_redis_pool", new_callable=AsyncMock):
            await handle_get_updates(mock_ws, "user_123", "not a list")

        mock_ws.send_json.assert_called_once()
        response = mock_ws.send_json.call_args[0][0]
        assert response["type"] == "error"
        assert "must be a list" in response["message"]

    @pytest.mark.asyncio
    async def test_handle_get_updates_no_subscribed_blocks(self, mock_ws, mock_redis):
        """Test response when user has no subscribed blocks."""
        mock_redis.smembers = AsyncMock(return_value=set())

        with patch("app.websockets.get_redis_pool", return_value=mock_redis):
            await handle_get_updates(mock_ws, "user_123", [{"id": "block-1", "updated_at": 0}])

        mock_ws.send_json.assert_called_once()
        response = mock_ws.send_json.call_args[0][0]
        assert response["type"] == "block_updates"
        assert response["updates"] == []

    @pytest.mark.asyncio
    async def test_handle_get_updates_filters_unsubscribed_blocks(self, mock_ws, mock_redis):
        """Test that unsubscribed blocks are filtered out."""
        # User is subscribed to block-1 but not block-2
        mock_redis.smembers = AsyncMock(return_value={"block-1"})

        with patch("app.websockets.get_redis_pool", return_value=mock_redis):
            await handle_get_updates(mock_ws, "user_123", [
                {"id": "block-1", "updated_at": 0},
                {"id": "block-2", "updated_at": 0}
            ])

        mock_ws.send_json.assert_called_once()
        response = mock_ws.send_json.call_args[0][0]
        assert response["type"] == "block_updates"
        # block-2 should be marked as deleted
        deleted_blocks = [u for u in response["updates"] if u.get("deleted")]
        assert len(deleted_blocks) == 1
        assert deleted_blocks[0]["id"] == "block-2"

    @pytest.mark.asyncio
    async def test_handle_get_updates_returns_newer_blocks(self, mock_ws, mock_redis):
        """Test that only blocks newer than client's version are returned."""
        mock_redis.smembers = AsyncMock(return_value={"block-1"})

        pipe = AsyncMock()
        pipe.hgetall = MagicMock()
        # Redis has newer version (updated_at=1000) than client (updated_at=500)
        pipe.execute = AsyncMock(return_value=[{"id": "block-1", "updated_at": "1000", "title": "Updated"}])
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=None)
        mock_redis.pipeline = MagicMock(return_value=pipe)

        with patch("app.websockets.get_redis_pool", return_value=mock_redis):
            await handle_get_updates(mock_ws, "user_123", [
                {"id": "block-1", "updated_at": 500}
            ])

        mock_ws.send_json.assert_called_once()
        response = mock_ws.send_json.call_args[0][0]
        assert response["type"] == "block_updates"
        assert len(response["updates"]) == 1
        assert response["updates"][0]["id"] == "block-1"

    @pytest.mark.asyncio
    async def test_handle_get_updates_skips_safety_margin_blocks(self, mock_ws, mock_redis):
        """Test that blocks with diff=1 (safety margin) are not returned."""
        mock_redis.smembers = AsyncMock(return_value={"block-1"})

        pipe = AsyncMock()
        pipe.hgetall = MagicMock()
        # Redis updated_at=1000, client sends 999 (=1000-1 safety margin) → diff=1 → skip
        pipe.execute = AsyncMock(return_value=[{"id": "block-1", "updated_at": "1000", "title": "Same"}])
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=None)
        mock_redis.pipeline = MagicMock(return_value=pipe)

        with patch("app.websockets.get_redis_pool", return_value=mock_redis):
            await handle_get_updates(mock_ws, "user_123", [
                {"id": "block-1", "updated_at": 999}
            ])

        mock_ws.send_json.assert_called_once()
        response = mock_ws.send_json.call_args[0][0]
        assert response["type"] == "block_updates"
        assert len([u for u in response["updates"] if not u.get("deleted")]) == 0

    @pytest.mark.asyncio
    async def test_handle_get_updates_returns_block_with_real_change(self, mock_ws, mock_redis):
        """Test that blocks with diff>1 (real change) are returned."""
        mock_redis.smembers = AsyncMock(return_value={"block-1"})

        pipe = AsyncMock()
        pipe.hgetall = MagicMock()
        # Redis updated_at=1002, client sends 999 (=1000-1) → diff=3 → return
        pipe.execute = AsyncMock(return_value=[{"id": "block-1", "updated_at": "1002", "title": "Changed"}])
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=None)
        mock_redis.pipeline = MagicMock(return_value=pipe)

        with patch("app.websockets.get_redis_pool", return_value=mock_redis):
            await handle_get_updates(mock_ws, "user_123", [
                {"id": "block-1", "updated_at": 999}
            ])

        mock_ws.send_json.assert_called_once()
        response = mock_ws.send_json.call_args[0][0]
        assert len(response["updates"]) == 1
        assert response["updates"][0]["id"] == "block-1"

    @pytest.mark.asyncio
    async def test_handle_get_updates_skips_older_blocks(self, mock_ws, mock_redis):
        """Test that blocks older than client's version are skipped."""
        mock_redis.smembers = AsyncMock(return_value={"block-1"})

        pipe = AsyncMock()
        pipe.hgetall = MagicMock()
        # Redis has older version (updated_at=500) than client (updated_at=1000)
        pipe.execute = AsyncMock(return_value=[{"id": "block-1", "updated_at": "500", "title": "Old"}])
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=None)
        mock_redis.pipeline = MagicMock(return_value=pipe)

        with patch("app.websockets.get_redis_pool", return_value=mock_redis):
            await handle_get_updates(mock_ws, "user_123", [
                {"id": "block-1", "updated_at": 1000}
            ])

        mock_ws.send_json.assert_called_once()
        response = mock_ws.send_json.call_args[0][0]
        assert response["type"] == "block_updates"
        # No updates since client has newer version
        assert len([u for u in response["updates"] if not u.get("deleted")]) == 0

    @pytest.mark.asyncio
    async def test_handle_get_updates_empty_blocks_list(self, mock_ws, mock_redis):
        """Test with empty blocks list."""
        mock_redis.smembers = AsyncMock(return_value={"block-1"})

        with patch("app.websockets.get_redis_pool", return_value=mock_redis):
            await handle_get_updates(mock_ws, "user_123", [])

        mock_ws.send_json.assert_called_once()
        response = mock_ws.send_json.call_args[0][0]
        assert response["type"] == "block_updates"
        assert response["updates"] == []

    @pytest.mark.asyncio
    async def test_handle_get_updates_string_updated_at_from_client(self, mock_ws, mock_redis):
        """Test that string updated_at from client is handled correctly (round-trip scenario)."""
        mock_redis.smembers = AsyncMock(return_value={"block-1"})

        pipe = AsyncMock()
        pipe.hgetall = MagicMock()
        pipe.execute = AsyncMock(return_value=[{"id": "block-1", "updated_at": "2000", "title": "Updated"}])
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=None)
        mock_redis.pipeline = MagicMock(return_value=pipe)

        with patch("app.websockets.get_redis_pool", return_value=mock_redis):
            await handle_get_updates(mock_ws, "user_123", [
                {"id": "block-1", "updated_at": "1000"}  # String from previous response
            ])

        mock_ws.send_json.assert_called_once()
        response = mock_ws.send_json.call_args[0][0]
        assert response["type"] == "block_updates"
        assert len(response["updates"]) == 1
        assert response["updates"][0]["id"] == "block-1"

    @pytest.mark.asyncio
    async def test_handle_get_updates_response_updated_at_is_int(self, mock_ws, mock_redis):
        """Test that updated_at in response is an integer, not a string."""
        mock_redis.smembers = AsyncMock(return_value={"block-1"})

        pipe = AsyncMock()
        pipe.hgetall = MagicMock()
        pipe.execute = AsyncMock(return_value=[{"id": "block-1", "updated_at": "2000", "title": "Test"}])
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=None)
        mock_redis.pipeline = MagicMock(return_value=pipe)

        with patch("app.websockets.get_redis_pool", return_value=mock_redis):
            await handle_get_updates(mock_ws, "user_123", [
                {"id": "block-1", "updated_at": 0}
            ])

        response = mock_ws.send_json.call_args[0][0]
        updated_block = response["updates"][0]
        assert isinstance(updated_block["updated_at"], int)
        assert updated_block["updated_at"] == 2000

    @pytest.mark.asyncio
    async def test_handle_get_updates_none_updated_at_from_client(self, mock_ws, mock_redis):
        """Test that None updated_at from client defaults to 0."""
        mock_redis.smembers = AsyncMock(return_value={"block-1"})

        pipe = AsyncMock()
        pipe.hgetall = MagicMock()
        pipe.execute = AsyncMock(return_value=[{"id": "block-1", "updated_at": "1000", "title": "Test"}])
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=None)
        mock_redis.pipeline = MagicMock(return_value=pipe)

        with patch("app.websockets.get_redis_pool", return_value=mock_redis):
            await handle_get_updates(mock_ws, "user_123", [
                {"id": "block-1", "updated_at": None}
            ])

        response = mock_ws.send_json.call_args[0][0]
        assert len(response["updates"]) == 1

    @pytest.mark.asyncio
    async def test_handle_get_updates_missing_updated_at_defaults_to_zero(self, mock_ws, mock_redis):
        """Test that missing updated_at defaults to 0 and block is returned."""
        mock_redis.smembers = AsyncMock(return_value={"block-1"})

        pipe = AsyncMock()
        pipe.hgetall = MagicMock()
        pipe.execute = AsyncMock(return_value=[{"id": "block-1", "updated_at": "1000", "title": "Test"}])
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=None)
        mock_redis.pipeline = MagicMock(return_value=pipe)

        with patch("app.websockets.get_redis_pool", return_value=mock_redis):
            await handle_get_updates(mock_ws, "user_123", [
                {"id": "block-1"}  # No updated_at key
            ])

        response = mock_ws.send_json.call_args[0][0]
        assert len(response["updates"]) == 1

    @pytest.mark.asyncio
    async def test_handle_get_updates_new_blocks_updated_at_is_int(self, mock_ws, mock_redis):
        """Test that new_blocks also return updated_at as int."""
        # User subscribed to block-1 and block-2, but client only sends block-1
        mock_redis.smembers = AsyncMock(return_value={"block-1", "block-2"})

        call_count = 0

        def make_pipeline(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            pipe = AsyncMock()
            pipe.hgetall = MagicMock()
            pipe.__aenter__ = AsyncMock(return_value=pipe)
            pipe.__aexit__ = AsyncMock(return_value=None)
            if call_count == 1:
                # First pipeline: updated blocks check
                pipe.execute = AsyncMock(return_value=[
                    {"id": "block-1", "updated_at": "500", "title": "Old"}
                ])
            else:
                # Second pipeline: new blocks data
                pipe.execute = AsyncMock(return_value=[
                    {"id": "block-2", "updated_at": "3000", "title": "New Block"}
                ])
            return pipe

        mock_redis.pipeline = MagicMock(side_effect=make_pipeline)

        with patch("app.websockets.get_redis_pool", return_value=mock_redis):
            await handle_get_updates(mock_ws, "user_123", [
                {"id": "block-1", "updated_at": 1000}
            ])

        response = mock_ws.send_json.call_args[0][0]
        assert len(response["new_blocks"]) == 1
        new_block = response["new_blocks"][0]
        assert isinstance(new_block["updated_at"], int)
        assert new_block["updated_at"] == 3000


class TestJWTVerificationFixed:
    """Tests verifying that JWT verification works correctly after fix."""

    @pytest.fixture
    def mock_ws(self):
        """Create a mock WebSocket."""
        ws = AsyncMock()
        ws.accept = AsyncMock()
        ws.close = AsyncMock()
        ws.send_json = AsyncMock()
        ws.receive_text = AsyncMock(side_effect=WebSocketDisconnect())
        ws.query_params = {}
        return ws

    @pytest.mark.asyncio
    async def test_verify_jwt_result_is_used(self, mock_ws):
        """Test that verify_jwt result is properly used to reject invalid tokens."""
        payload = {"user_id": 99999}
        fake_token = jwt.encode(payload, "wrong_secret", algorithm="HS256")
        mock_ws.query_params = {"token": fake_token}

        with patch("app.websockets.verify_jwt", return_value=False) as mock_verify:
            with patch.object(connection_manager, "connect", new_callable=AsyncMock) as mock_connect:
                with patch.object(connection_manager, "disconnect", new_callable=AsyncMock):
                    await websocket_endpoint(mock_ws)

        # verify_jwt is called
        mock_verify.assert_called_once_with(fake_token)

        # Fixed: User is NOT connected when verify_jwt returns False
        mock_connect.assert_not_called()
        mock_ws.close.assert_called_once_with(code=1008)

    @pytest.mark.asyncio
    async def test_signature_verification_enabled(self, mock_ws):
        """Test that JWT signature verification is now enabled."""
        # Create token signed with wrong key
        payload = {"user_id": 12345}
        token_wrong_key = jwt.encode(payload, "completely_wrong_key", algorithm="HS256")
        mock_ws.query_params = {"token": token_wrong_key}

        with patch("app.websockets.verify_jwt", return_value=False):
            with patch.object(connection_manager, "connect", new_callable=AsyncMock) as mock_connect:
                with patch.object(connection_manager, "disconnect", new_callable=AsyncMock):
                    await websocket_endpoint(mock_ws)

        # Fixed: User is NOT connected with wrong signature
        mock_connect.assert_not_called()
        mock_ws.close.assert_called_once_with(code=1008)
