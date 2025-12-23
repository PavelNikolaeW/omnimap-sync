import pytest
import asyncio
import json
import os
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta

# Set testing environment BEFORE importing app modules
os.environ['TESTING'] = 'true'

import jwt
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.websockets import WebSocket

from app.config import settings


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def valid_user_id():
    """Return a valid user ID for testing."""
    return 12345


@pytest.fixture
def anonymous_user_id():
    """Return anonymous user ID."""
    return settings.ANONIM_USER


@pytest.fixture
def valid_jwt_token(valid_user_id):
    """Generate a valid JWT token for testing."""
    payload = {
        "user_id": valid_user_id,
        "exp": datetime.utcnow() + timedelta(hours=1),
        "iat": datetime.utcnow(),
    }
    return jwt.encode(payload, settings.jwt_secret_key or "test_secret", algorithm="HS256")


@pytest.fixture
def expired_jwt_token(valid_user_id):
    """Generate an expired JWT token for testing."""
    payload = {
        "user_id": valid_user_id,
        "exp": datetime.utcnow() - timedelta(hours=1),
        "iat": datetime.utcnow() - timedelta(hours=2),
    }
    return jwt.encode(payload, settings.jwt_secret_key or "test_secret", algorithm="HS256")


@pytest.fixture
def invalid_jwt_token():
    """Return an invalid JWT token."""
    return "invalid.token.here"


@pytest.fixture
def jwt_token_without_user_id():
    """Generate a JWT token without user_id."""
    payload = {
        "exp": datetime.utcnow() + timedelta(hours=1),
        "iat": datetime.utcnow(),
    }
    return jwt.encode(payload, settings.jwt_secret_key or "test_secret", algorithm="HS256")


@pytest.fixture
def mock_websocket():
    """Create a mock WebSocket for testing."""
    ws = AsyncMock(spec=WebSocket)
    ws.query_params = {"token": "test_token"}
    ws.accept = AsyncMock()
    ws.close = AsyncMock()
    ws.send_json = AsyncMock()
    ws.receive_text = AsyncMock()
    return ws


@pytest.fixture
def mock_redis():
    """Create a mock Redis client."""
    redis = AsyncMock()
    redis.ping = AsyncMock(return_value=True)
    redis.hset = AsyncMock()
    redis.hgetall = AsyncMock(return_value={})
    redis.smembers = AsyncMock(return_value=set())
    redis.sadd = AsyncMock()
    redis.srem = AsyncMock()
    redis.delete = AsyncMock()
    redis.pipeline = MagicMock()

    # Mock pipeline context manager
    pipe = AsyncMock()
    pipe.hset = AsyncMock()
    pipe.hgetall = AsyncMock()
    pipe.smembers = AsyncMock()
    pipe.sadd = AsyncMock()
    pipe.srem = AsyncMock()
    pipe.delete = AsyncMock()
    pipe.execute = AsyncMock(return_value=[])
    pipe.__aenter__ = AsyncMock(return_value=pipe)
    pipe.__aexit__ = AsyncMock(return_value=None)
    redis.pipeline.return_value = pipe

    return redis


@pytest.fixture
def mock_rabbitmq_message():
    """Create a mock RabbitMQ message."""
    message = AsyncMock()
    message.body = b'{}'

    # Mock context manager for message.process()
    process_ctx = AsyncMock()
    process_ctx.__aenter__ = AsyncMock()
    process_ctx.__aexit__ = AsyncMock()
    message.process = MagicMock(return_value=process_ctx)

    return message


@pytest.fixture
def sample_block_data():
    """Return sample block data for testing."""
    return {
        "id": "block-uuid-123",
        "title": "Test Block",
        "children": json.dumps([]),
        "updated_at": 1703000000,
        "data": json.dumps({"color": [0, 100, 100, 0], "childOrder": []})
    }


@pytest.fixture
def sample_blocks_list(sample_block_data):
    """Return a list of sample blocks."""
    return [
        {"id": "block-uuid-1", "updated_at": 1703000000},
        {"id": "block-uuid-2", "updated_at": 1703000001},
        {"id": "block-uuid-3", "updated_at": 1703000002},
    ]
