# tests/test_utils.py
"""Tests for utility functions."""

import pytest
from app.utils import prepare_block_data_for_redis, parse_redis_block_data


class TestPrepareBlockDataForRedis:
    """Tests for prepare_block_data_for_redis function."""

    def test_none_value_becomes_null_string(self):
        """None values should be converted to 'null' string."""
        data = {"parent_id": None, "name": "test"}
        result = prepare_block_data_for_redis(data)
        assert result["parent_id"] == "null"
        assert result["name"] == "test"

    def test_dict_value_is_json_serialized(self):
        """Dict values should be JSON serialized."""
        data = {"properties": {"color": "red", "size": 10}}
        result = prepare_block_data_for_redis(data)
        assert result["properties"] == '{"color": "red", "size": 10}'

    def test_list_value_is_json_serialized(self):
        """List values should be JSON serialized."""
        data = {"tags": ["tag1", "tag2"]}
        result = prepare_block_data_for_redis(data)
        assert result["tags"] == '["tag1", "tag2"]'

    def test_boolean_values_are_json_serialized(self):
        """Boolean values should be JSON serialized."""
        data = {"is_active": True, "is_deleted": False}
        result = prepare_block_data_for_redis(data)
        assert result["is_active"] == "true"
        assert result["is_deleted"] == "false"

    def test_numeric_values_become_strings(self):
        """Numeric values should be converted to strings."""
        data = {"updated_at": 1234567890, "count": 42}
        result = prepare_block_data_for_redis(data)
        assert result["updated_at"] == "1234567890"
        assert result["count"] == "42"

    def test_string_values_remain_strings(self):
        """String values should remain as strings."""
        data = {"id": "block-123", "type": "text"}
        result = prepare_block_data_for_redis(data)
        assert result["id"] == "block-123"
        assert result["type"] == "text"

    def test_complex_block_data(self):
        """Test with realistic block data structure."""
        data = {
            "id": "block-uuid-123",
            "parent_id": None,
            "type": "text",
            "properties": {"content": "Hello"},
            "children": ["child-1", "child-2"],
            "is_root": True,
            "updated_at": 1704067200,
        }
        result = prepare_block_data_for_redis(data)
        assert result["id"] == "block-uuid-123"
        assert result["parent_id"] == "null"
        assert result["type"] == "text"
        assert result["properties"] == '{"content": "Hello"}'
        assert result["children"] == '["child-1", "child-2"]'
        assert result["is_root"] == "true"
        assert result["updated_at"] == "1704067200"


class TestParseRedisBlockData:
    """Tests for parse_redis_block_data function."""

    def test_null_string_becomes_none(self):
        """'null' string should be converted to Python None."""
        data = {"parent_id": "null", "name": "test"}
        result = parse_redis_block_data(data)
        assert result["parent_id"] is None
        assert result["name"] == "test"

    def test_json_dict_is_parsed(self):
        """JSON dict strings should be parsed."""
        data = {"properties": '{"color": "red", "size": 10}'}
        result = parse_redis_block_data(data)
        assert result["properties"] == {"color": "red", "size": 10}

    def test_json_list_is_parsed(self):
        """JSON list strings should be parsed."""
        data = {"tags": '["tag1", "tag2"]'}
        result = parse_redis_block_data(data)
        assert result["tags"] == ["tag1", "tag2"]

    def test_boolean_strings_are_parsed(self):
        """'true' and 'false' strings should be converted to booleans."""
        data = {"is_active": "true", "is_deleted": "false"}
        result = parse_redis_block_data(data)
        assert result["is_active"] is True
        assert result["is_deleted"] is False

    def test_numeric_strings_remain_strings(self):
        """Numeric strings should remain as strings (IDs, timestamps)."""
        data = {"updated_at": "1234567890", "id": "12345"}
        result = parse_redis_block_data(data)
        assert result["updated_at"] == "1234567890"
        assert result["id"] == "12345"

    def test_regular_strings_remain_strings(self):
        """Regular strings should remain as strings."""
        data = {"type": "text", "content": "Hello world"}
        result = parse_redis_block_data(data)
        assert result["type"] == "text"
        assert result["content"] == "Hello world"

    def test_invalid_json_remains_string(self):
        """Invalid JSON starting with { or [ should remain as string."""
        data = {"broken": "{not valid json"}
        result = parse_redis_block_data(data)
        assert result["broken"] == "{not valid json"

    def test_complex_block_data(self):
        """Test with realistic Redis data structure."""
        data = {
            "id": "block-uuid-123",
            "parent_id": "null",
            "type": "text",
            "properties": '{"content": "Hello"}',
            "children": '["child-1", "child-2"]',
            "is_root": "true",
            "updated_at": "1704067200",
        }
        result = parse_redis_block_data(data)
        assert result["id"] == "block-uuid-123"
        assert result["parent_id"] is None
        assert result["type"] == "text"
        assert result["properties"] == {"content": "Hello"}
        assert result["children"] == ["child-1", "child-2"]
        assert result["is_root"] is True
        assert result["updated_at"] == "1704067200"


class TestRoundTrip:
    """Tests for prepare -> parse round trip."""

    def test_roundtrip_preserves_none(self):
        """None values should survive round trip."""
        original = {"parent_id": None}
        redis_data = prepare_block_data_for_redis(original)
        parsed = parse_redis_block_data(redis_data)
        assert parsed["parent_id"] is None

    def test_roundtrip_preserves_complex_data(self):
        """Complex data should survive round trip."""
        original = {
            "id": "block-123",
            "parent_id": None,
            "properties": {"nested": {"deep": True}},
            "tags": ["a", "b", "c"],
            "is_active": True,
            "count": 42,
        }
        redis_data = prepare_block_data_for_redis(original)
        parsed = parse_redis_block_data(redis_data)

        assert parsed["id"] == "block-123"
        assert parsed["parent_id"] is None
        assert parsed["properties"] == {"nested": {"deep": True}}
        assert parsed["tags"] == ["a", "b", "c"]
        assert parsed["is_active"] is True
        # Note: count becomes string after round trip (expected for Redis)
        assert parsed["count"] == "42"
