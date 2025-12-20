# app/auth.py
"""JWT verification with external auth service."""

import logging

import httpx

from app.config import settings

logger = logging.getLogger("realtime_service")

# HTTP client timeout configuration
# - 10 seconds for connection establishment
# - 30 seconds for request completion
HTTP_TIMEOUT = httpx.Timeout(timeout=30.0, connect=10.0)


async def verify_jwt(token: str) -> bool:
    """
    Verify JWT token with external auth service.

    Args:
        token: The JWT token string to verify

    Returns:
        True if token is valid, False otherwise
    """
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        try:
            response = await client.post(
                settings.auth_service_url,
                json={"token": token}
            )
            if response.status_code == 200:
                return True
            else:
                logger.warning(f"JWT verification failed with status {response.status_code}")
        except httpx.TimeoutException:
            logger.error("JWT verification timed out")
        except httpx.ConnectError as e:
            logger.error(f"Failed to connect to auth service: {e}")
        except Exception:
            logger.exception("Unexpected error verifying JWT token")
    return False
