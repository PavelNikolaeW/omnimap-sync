# app/metrics.py
"""Prometheus metrics definitions for omnimap-sync."""

from prometheus_client import Counter, Gauge, Histogram

# =============================================================================
# WebSocket Connections
# =============================================================================

ws_connections_active = Gauge(
    "ws_connections_active",
    "Current number of active WebSocket connections",
)

ws_connections_total = Counter(
    "ws_connections_total",
    "Total WebSocket connections since startup",
)

ws_disconnections_total = Counter(
    "ws_disconnections_total",
    "Total WebSocket disconnections since startup",
)

ws_users_active = Gauge(
    "ws_users_active",
    "Number of unique connected users",
)

# =============================================================================
# WebSocket Messages
# =============================================================================

ws_messages_sent_total = Counter(
    "ws_messages_sent_total",
    "Total messages sent to WebSocket clients",
)

ws_messages_received_total = Counter(
    "ws_messages_received_total",
    "Total messages received from WebSocket clients",
    ["action"],
)

ws_send_errors_total = Counter(
    "ws_send_errors_total",
    "Total errors when sending messages to WebSocket clients",
)

# =============================================================================
# RabbitMQ
# =============================================================================

rabbitmq_messages_total = Counter(
    "rabbitmq_messages_total",
    "Total RabbitMQ messages processed by action type",
    ["action"],
)

rabbitmq_message_errors_total = Counter(
    "rabbitmq_message_errors_total",
    "Total RabbitMQ message processing errors by action type",
    ["action"],
)

rabbitmq_message_duration_seconds = Histogram(
    "rabbitmq_message_duration_seconds",
    "Time spent processing a RabbitMQ message",
    ["action"],
)

rabbitmq_reconnects_total = Counter(
    "rabbitmq_reconnects_total",
    "Total RabbitMQ reconnection attempts",
)

# =============================================================================
# Auth
# =============================================================================

auth_requests_total = Counter(
    "auth_requests_total",
    "Total auth verification requests by result",
    ["result"],
)

auth_request_duration_seconds = Histogram(
    "auth_request_duration_seconds",
    "Latency of auth service verification requests",
)
