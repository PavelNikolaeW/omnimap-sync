"""Tests for app/redis_client.py module."""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from app.redis_client import get_redis_pool, close_redis_pool
from app.config import settings


class TestGetRedisPool:
    """Tests for get_redis_pool function."""

    @pytest.fixture(autouse=True)
    async def reset_redis_client(self):
        """Reset global redis_client before each test."""
        import app.redis_client
        app.redis_client._redis_client = None
        yield
        app.redis_client._redis_client = None

    @pytest.mark.asyncio
    async def test_get_redis_pool_creates_client(self):
        """Test that get_redis_pool creates a Redis client."""
        mock_redis = AsyncMock()

        with patch("app.redis_client.redis.from_url", new_callable=AsyncMock) as mock_from_url:
            mock_from_url.return_value = mock_redis

            result = await get_redis_pool()

            mock_from_url.assert_called_once_with(
                settings.redis_url,
                decode_responses=True,
                max_connections=20
            )
            assert result == mock_redis

    @pytest.mark.asyncio
    async def test_get_redis_pool_returns_singleton(self):
        """Test that get_redis_pool returns the same instance on subsequent calls."""
        mock_redis = AsyncMock()

        with patch("app.redis_client.redis.from_url", new_callable=AsyncMock) as mock_from_url:
            mock_from_url.return_value = mock_redis

            result1 = await get_redis_pool()
            result2 = await get_redis_pool()

            # from_url should only be called once (singleton pattern)
            assert mock_from_url.call_count == 1
            assert result1 is result2

    @pytest.mark.asyncio
    async def test_get_redis_pool_uses_settings_url(self):
        """Test that get_redis_pool uses URL from settings."""
        mock_redis = AsyncMock()

        with patch("app.redis_client.redis.from_url", new_callable=AsyncMock) as mock_from_url:
            mock_from_url.return_value = mock_redis

            await get_redis_pool()

            call_args = mock_from_url.call_args
            assert call_args[0][0] == settings.redis_url

    @pytest.mark.asyncio
    async def test_get_redis_pool_sets_decode_responses(self):
        """Test that decode_responses=True is set."""
        mock_redis = AsyncMock()

        with patch("app.redis_client.redis.from_url", new_callable=AsyncMock) as mock_from_url:
            mock_from_url.return_value = mock_redis

            await get_redis_pool()

            call_kwargs = mock_from_url.call_args[1]
            assert call_kwargs["decode_responses"] is True

    @pytest.mark.asyncio
    async def test_get_redis_pool_sets_max_connections(self):
        """Test that max_connections is set to 20."""
        mock_redis = AsyncMock()

        with patch("app.redis_client.redis.from_url", new_callable=AsyncMock) as mock_from_url:
            mock_from_url.return_value = mock_redis

            await get_redis_pool()

            call_kwargs = mock_from_url.call_args[1]
            assert call_kwargs["max_connections"] == 20


class TestCloseRedisPool:
    """Tests for close_redis_pool function."""

    @pytest.fixture(autouse=True)
    async def reset_redis_client(self):
        """Reset global redis_client before each test."""
        import app.redis_client
        app.redis_client._redis_client = None
        yield
        app.redis_client._redis_client = None

    @pytest.mark.asyncio
    async def test_close_redis_pool_exists(self):
        """Test that close_redis_pool function exists."""
        import app.redis_client

        assert hasattr(app.redis_client, 'close_redis_pool')
        assert callable(app.redis_client.close_redis_pool)

    @pytest.mark.asyncio
    async def test_close_redis_pool_closes_connection(self):
        """Test that close_redis_pool closes the connection."""
        mock_redis = AsyncMock()
        mock_redis.close = AsyncMock()

        with patch("app.redis_client.redis.from_url", new_callable=AsyncMock) as mock_from_url:
            mock_from_url.return_value = mock_redis

            # Create connection
            await get_redis_pool()

            # Close connection
            await close_redis_pool()

            mock_redis.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_redis_pool_resets_client(self):
        """Test that close_redis_pool resets the client to None."""
        import app.redis_client

        mock_redis = AsyncMock()
        mock_redis.close = AsyncMock()

        with patch("app.redis_client.redis.from_url", new_callable=AsyncMock) as mock_from_url:
            mock_from_url.return_value = mock_redis

            await get_redis_pool()
            assert app.redis_client._redis_client is not None

            await close_redis_pool()
            assert app.redis_client._redis_client is None

    @pytest.mark.asyncio
    async def test_close_redis_pool_safe_when_not_initialized(self):
        """Test that close_redis_pool is safe to call when not initialized."""
        import app.redis_client
        app.redis_client._redis_client = None

        # Should not raise
        await close_redis_pool()


class TestRedisClientThreadSafety:
    """Tests for thread-safety of Redis client."""

    @pytest.fixture(autouse=True)
    async def reset_redis_client(self):
        """Reset global redis_client before each test."""
        import app.redis_client
        app.redis_client._redis_client = None
        yield
        app.redis_client._redis_client = None

    @pytest.mark.asyncio
    async def test_redis_client_uses_lock(self):
        """Test that Redis client uses lock for initialization."""
        import app.redis_client

        # Check that lock exists
        assert hasattr(app.redis_client, '_redis_lock')


class TestRedisClientIntegration:
    """Integration-style tests for Redis client."""

    @pytest.fixture(autouse=True)
    async def reset_redis_client(self):
        """Reset global redis_client before each test."""
        import app.redis_client
        app.redis_client._redis_client = None
        yield
        app.redis_client._redis_client = None

    @pytest.mark.asyncio
    async def test_redis_client_can_be_used_for_operations(self):
        """Test that returned client can perform Redis operations."""
        mock_redis = AsyncMock()
        mock_redis.ping = AsyncMock(return_value=True)
        mock_redis.get = AsyncMock(return_value="value")
        mock_redis.set = AsyncMock(return_value=True)

        with patch("app.redis_client.redis.from_url", new_callable=AsyncMock) as mock_from_url:
            mock_from_url.return_value = mock_redis

            client = await get_redis_pool()

            # Test ping
            result = await client.ping()
            assert result is True

            # Test get
            result = await client.get("key")
            assert result == "value"

            # Test set
            result = await client.set("key", "value")
            assert result is True
