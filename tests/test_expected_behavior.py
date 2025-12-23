"""
Tests for EXPECTED behavior.

These tests define how the code SHOULD work.
They will FAIL on current implementation and should PASS after fixes.

Run with: pytest tests/test_expected_behavior.py -v
"""

import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta

import jwt
from fastapi import WebSocketDisconnect

from app.websockets import websocket_endpoint, connection_manager
from app.config import settings


class TestExpectedJWTBehavior:
    """Tests for how JWT verification SHOULD work."""

    @pytest.fixture
    def mock_ws(self):
        ws = AsyncMock()
        ws.accept = AsyncMock()
        ws.close = AsyncMock()
        ws.send_json = AsyncMock()
        ws.receive_text = AsyncMock(side_effect=WebSocketDisconnect())
        ws.query_params = {}
        return ws

    @pytest.mark.asyncio
    async def test_invalid_jwt_should_disconnect_user(self, mock_ws):
        """User with invalid JWT should be disconnected, not connected as anonymous."""
        payload = {"user_id": 12345}
        token = jwt.encode(payload, "any_key", algorithm="HS256")
        mock_ws.query_params = {"token": token}

        with patch("app.websockets.verify_jwt", return_value=False):  # Token is INVALID
            with patch.object(connection_manager, "connect", new_callable=AsyncMock) as mock_connect:
                with patch.object(connection_manager, "disconnect", new_callable=AsyncMock):
                    await websocket_endpoint(mock_ws)

        # EXPECTED: User should NOT be connected when JWT is invalid
        mock_connect.assert_not_called()
        # EXPECTED: Connection should be closed
        mock_ws.close.assert_called()

    @pytest.mark.asyncio
    async def test_jwt_with_wrong_signature_should_be_rejected(self, mock_ws):
        """JWT signed with wrong key should be rejected."""
        payload = {"user_id": 99999}
        token = jwt.encode(payload, "WRONG_SECRET_KEY", algorithm="HS256")
        mock_ws.query_params = {"token": token}

        # Don't mock verify_jwt - let it actually verify
        with patch.object(connection_manager, "connect", new_callable=AsyncMock) as mock_connect:
            with patch.object(connection_manager, "disconnect", new_callable=AsyncMock):
                # This should verify signature and reject the token
                await websocket_endpoint(mock_ws)

        # EXPECTED: User should NOT be connected with wrong signature
        mock_connect.assert_not_called()
        mock_ws.close.assert_called()

    @pytest.mark.asyncio
    async def test_malformed_jwt_should_disconnect_not_grant_anonymous(self, mock_ws):
        """Malformed JWT should disconnect user, not grant anonymous access."""
        mock_ws.query_params = {"token": "not.a.valid.jwt.token"}

        with patch("app.websockets.verify_jwt", return_value=False):
            with patch.object(connection_manager, "connect", new_callable=AsyncMock) as mock_connect:
                with patch.object(connection_manager, "disconnect", new_callable=AsyncMock):
                    await websocket_endpoint(mock_ws)

        # EXPECTED: Should disconnect, not connect as anonymous
        mock_connect.assert_not_called()
        mock_ws.close.assert_called()


class TestExpectedAnonymousUserBehavior:
    """Tests for how anonymous user handling SHOULD work."""

    @pytest.mark.asyncio
    async def test_anonymous_user_should_use_semaphore(self):
        """Anonymous user connections should use semaphore for rate limiting."""
        from app.connection_manager import ConnectionManager

        manager = ConnectionManager()
        anon_user_id = str(settings.ANONIM_USER)  # "-1"

        mock_ws = AsyncMock()
        mock_ws.send_json = AsyncMock()
        await manager.connect(anon_user_id, mock_ws)

        # Track semaphore usage
        semaphore_used = False
        original_acquire = manager.anon_semaphore.acquire

        async def track_acquire():
            nonlocal semaphore_used
            semaphore_used = True
            return await original_acquire()

        manager.anon_semaphore.acquire = track_acquire

        await manager.send_personal_message({"type": "test"}, anon_user_id)

        # EXPECTED: Semaphore should be used for anonymous user
        assert semaphore_used, "Semaphore should be used for anonymous user"


class TestExpectedMessageHandling:
    """Tests for how message handling SHOULD work."""

    @pytest.fixture
    def mock_message(self):
        message = AsyncMock()
        message.ack = AsyncMock()
        message.reject = AsyncMock()
        message.nack = AsyncMock()
        return message

    @pytest.mark.asyncio
    async def test_invalid_json_should_be_rejected_not_acknowledged(self, mock_message):
        """Invalid JSON messages should be rejected (nack), not acknowledged."""
        from app.rabbitmq_consumer import handle_message

        mock_message.body = b"invalid json {"

        await handle_message(mock_message)

        # EXPECTED: Message should be rejected, not silently acknowledged
        assert mock_message.reject.called or mock_message.nack.called, \
            "Invalid message should be rejected, not acknowledged"


class TestExpectedRedisClientBehavior:
    """Tests for how Redis client SHOULD work."""

    @pytest.mark.asyncio
    async def test_redis_client_should_have_close_function(self):
        """Redis client module should have a close function for graceful shutdown."""
        import app.redis_client

        # EXPECTED: Module should have close_redis_pool function
        assert hasattr(app.redis_client, 'close_redis_pool'), \
            "Redis client should have close_redis_pool function"

        # EXPECTED: Function should be callable
        assert callable(app.redis_client.close_redis_pool), \
            "close_redis_pool should be callable"


class TestExpectedHTTPClientBehavior:
    """Tests for how HTTP client SHOULD work."""

    @pytest.mark.asyncio
    async def test_verify_jwt_should_have_timeout(self):
        """verify_jwt should have a timeout to prevent hanging."""
        import inspect
        from app.auth import verify_jwt

        source = inspect.getsource(verify_jwt)

        # EXPECTED: Source should contain timeout configuration
        assert "timeout" in source.lower(), \
            "verify_jwt should configure HTTP timeout"
