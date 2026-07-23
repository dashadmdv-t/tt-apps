from fastapi import (
    HTTPException,
    Security,
    status,
)
from fastapi.security import APIKeyHeader

from src.core.config import get_settings

api_key_header = APIKeyHeader(name="X-API-Key")


async def verify_api_key(api_key: str = Security(api_key_header)):
    if api_key != get_settings().api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )
