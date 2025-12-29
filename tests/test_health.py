"""Tests for health check endpoint."""

import os

# Set testing environment BEFORE importing app modules
os.environ['TESTING'] = 'true'

from unittest.mock import patch, AsyncMock

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    """Create test client with mocked Redis and RabbitMQ."""
    with patch('app.main.get_redis_pool') as mock_redis_pool, \
         patch('app.main.connect_robust') as mock_rabbitmq, \
         patch('app.main.start_consumer') as mock_consumer:
        # Mock Redis
        mock_redis = AsyncMock()
        mock_redis.ping = AsyncMock(return_value=True)
        mock_redis_pool.return_value = mock_redis

        # Mock RabbitMQ connection
        mock_conn = AsyncMock()
        mock_conn.close = AsyncMock()
        mock_rabbitmq.return_value = mock_conn

        # Mock consumer task
        mock_consumer.return_value = AsyncMock()

        from app.main import app
        with TestClient(app) as client:
            yield client


def test_health_endpoint_returns_200(client):
    """Test that /health endpoint returns 200 status."""
    response = client.get("/health")
    assert response.status_code == 200


def test_health_endpoint_returns_healthy_status(client):
    """Test that /health endpoint returns healthy status in body."""
    response = client.get("/health")
    assert response.json() == {"status": "healthy"}


def test_health_endpoint_has_cors_header(client):
    """Test that /health endpoint has Access-Control-Allow-Origin header."""
    response = client.get("/health")
    assert "access-control-allow-origin" in response.headers
    assert response.headers["access-control-allow-origin"] == "*"
