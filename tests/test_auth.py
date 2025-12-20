"""Tests for app/auth.py module."""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import httpx

from app.auth import verify_jwt
from app.config import settings


class TestVerifyJWT:
    """Tests for verify_jwt function."""

    @pytest.mark.asyncio
    async def test_verify_jwt_valid_token_returns_true(self):
        """Test that valid token returns True."""
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("app.auth.httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            result = await verify_jwt("valid_token")

            assert result is True
            mock_client.post.assert_called_once_with(
                settings.auth_service_url,
                json={"token": "valid_token"}
            )

    @pytest.mark.asyncio
    async def test_verify_jwt_invalid_token_returns_false(self):
        """Test that invalid token (non-200 response) returns False."""
        mock_response = MagicMock()
        mock_response.status_code = 401

        with patch("app.auth.httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            result = await verify_jwt("invalid_token")

            assert result is False

    @pytest.mark.asyncio
    async def test_verify_jwt_server_error_returns_false(self):
        """Test that server error (500) returns False."""
        mock_response = MagicMock()
        mock_response.status_code = 500

        with patch("app.auth.httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            result = await verify_jwt("some_token")

            assert result is False

    @pytest.mark.asyncio
    async def test_verify_jwt_connection_error_returns_false(self):
        """Test that connection error returns False."""
        with patch("app.auth.httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(side_effect=httpx.ConnectError("Connection failed"))
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            result = await verify_jwt("some_token")

            assert result is False

    @pytest.mark.asyncio
    async def test_verify_jwt_timeout_returns_false(self):
        """Test that timeout returns False."""
        with patch("app.auth.httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(side_effect=httpx.TimeoutException("Timeout"))
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            result = await verify_jwt("some_token")

            assert result is False

    @pytest.mark.asyncio
    async def test_verify_jwt_generic_exception_returns_false(self):
        """Test that generic exception returns False."""
        with patch("app.auth.httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(side_effect=Exception("Unknown error"))
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            result = await verify_jwt("some_token")

            assert result is False

    @pytest.mark.asyncio
    async def test_verify_jwt_calls_correct_url(self):
        """Test that verify_jwt calls the correct auth service URL."""
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("app.auth.httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            await verify_jwt("test_token")

            mock_client.post.assert_called_once()
            call_args = mock_client.post.call_args
            assert call_args[0][0] == settings.auth_service_url
            assert call_args[1]["json"] == {"token": "test_token"}

    @pytest.mark.asyncio
    async def test_verify_jwt_empty_token(self):
        """Test verify_jwt with empty token."""
        mock_response = MagicMock()
        mock_response.status_code = 400

        with patch("app.auth.httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            result = await verify_jwt("")

            assert result is False
