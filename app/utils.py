# app/utils.py
"""Utility functions for data processing."""

import json
from typing import Any


def prepare_block_data_for_redis(block_data: dict[str, Any]) -> dict[str, str]:
    """
    Prepare block data for Redis hash storage.

    Redis hashes only store strings, so we need to properly serialize values:
    - None values are converted to JSON "null" (not Python str "None")
    - Complex types (dict, list) are JSON serialized
    - Other values are converted to strings

    Args:
        block_data: Dictionary with block data from RabbitMQ

    Returns:
        Dictionary with all values as strings suitable for Redis hset
    """
    result: dict[str, str] = {}
    for key, value in block_data.items():
        if value is None:
            result[key] = "null"
        elif isinstance(value, (dict, list)):
            result[key] = json.dumps(value)
        elif isinstance(value, bool):
            result[key] = json.dumps(value)
        else:
            result[key] = str(value)
    return result


def parse_redis_block_data(redis_data: dict[str, str]) -> dict[str, Any]:
    """
    Parse block data retrieved from Redis hash.

    Converts stored string values back to their proper types:
    - "null" is converted to Python None
    - JSON strings (starting with { or [) are parsed
    - "true"/"false" are converted to booleans
    - Numeric strings are kept as strings (IDs, timestamps)

    Args:
        redis_data: Dictionary from Redis hgetall

    Returns:
        Dictionary with properly typed values
    """
    result: dict[str, Any] = {}
    for key, value in redis_data.items():
        if value == "null":
            result[key] = None
        elif value == "true":
            result[key] = True
        elif value == "false":
            result[key] = False
        elif value.startswith(("{", "[")):
            try:
                result[key] = json.loads(value)
            except json.JSONDecodeError:
                result[key] = value
        else:
            result[key] = value
    return result
