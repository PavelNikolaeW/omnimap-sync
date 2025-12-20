# app/config.py
"""Application configuration using Pydantic settings."""

import json
import os
from typing import Any

from pydantic import field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.

    Environment variables can be set directly or in a .env file.
    """

    # Server configuration
    host: str = "0.0.0.0"
    port: int = 7999

    # External services
    redis_url: str = "redis://localhost:6379"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    # Message queue configuration
    queue_name: str = 'block_update'
    exchange_name: str = 'block_update'
    exchange_type: str = 'direct'  # direct, fanout, topic, headers
    routing_key: str = 'block_update'

    # Authentication settings
    auth_service_url: str = "http://localhost:8000/api/v1/token/verify/"
    jwt_algorithms: str = "HS256"
    jwt_secret_key: str = ""  # Required in production, validated below

    # Performance tuning
    block_portion: int = 500  # Chunk size for Redis pipeline operations
    MAX_CONCURRENT_SENDS: int = 10  # Semaphore limit for anonymous user

    # Special user IDs
    ANONIM_USER: int = -1  # Anonymous user ID

    # Forbidden block template (shown when access denied)
    FORBIDDEN_BLOCK: dict[str, Any] = {
        'id': '',
        'title': 'block 403 forbidden',
        'children': json.dumps([]),
        'updated_at': 946684801,
        'data': json.dumps({'color': [0, 100, 100, 0], 'childOrder': []})
    }

    @field_validator('jwt_secret_key')
    @classmethod
    def validate_jwt_secret_key(cls, v: str) -> str:
        """Validate JWT secret key is secure (32+ characters)."""
        if not v or len(v) < 32:
            # Allow empty/short key in test environment
            if os.getenv('TESTING', '').lower() in ('1', 'true', 'yes'):
                return v or 'test_secret_key_for_testing_only_32ch'
            raise ValueError(
                'jwt_secret_key must be at least 32 characters. '
                'Set JWT_SECRET_KEY environment variable or in .env file.'
            )
        return v

    class Config:
        env_file = ".env"


settings = Settings()
