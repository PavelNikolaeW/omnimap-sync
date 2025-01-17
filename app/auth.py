# app/auth.py
import httpx
from app.config import settings
import logging

logger = logging.getLogger("realtime_service")


async def verify_jwt(token: str) -> bool:
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(settings.auth_service_url, json={"token": token})
            if response.status_code == 200:
                return True
            else:
                logger.warning("Invalid JWT token received.")
        except Exception as e:
            logger.error(f"Error verifying JWT token: {e}")
    return False
