# app/config.py
import json

from pydantic_settings import BaseSettings
import typing as t


class Settings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 7999
    redis_url: str = "redis://localhost:6379"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    queue_name: str = 'block_update'
    auth_service_url: str = "https://localhost:8080/api/v1/token/verify/"
    secret_key: str = "secret_key"
    jwt_algorithms: str = "HS256"
    jwt_secret_key: t.Optional[str] = None
    block_portion: int = 500
    ANONIM_USER: int = -1
    MAX_CONCURRENT_SENDS: int = 10
    FORBIDDEN_BLOCK: dict = {'id': '',
                             'title': 'block 403 forbidden',
                             'children': json.dumps([]),
                             'updated_at': 946684801,
                             'data': json.dumps({'color': [0, 100, 100, 0], 'childOrder': []})
                             }

    class Config:
        env_file = ".env"


settings = Settings()
