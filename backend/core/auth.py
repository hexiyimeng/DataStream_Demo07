from fastapi import HTTPException, Header, status
import os

API_KEYS = None
if os.getenv("BRAINFLOW_API_KEYS"):
    API_KEYS = {k.strip() for k in os.getenv("BRAINFLOW_API_KEYS", "").split(",") if k and k.strip()}


def _mask_token(token: str) -> str:
    """脱敏 token，只显示前4位和后2位"""
    if not token or len(token) < 8:
        return "***"
    return f"{token[:4]}...{token[-2:]}"


async def verify_api_key(authorization: str = Header(None)):
    """
    Verify API key from Authorization header.
    If BRAINFLOW_API_KEYS is not set (development mode), authentication is skipped.
    """
    if API_KEYS is None:  # 未配置则跳过验证（开发模式）
        return
    if authorization is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key. Please provide Authorization header.",
        )

    auth_value = authorization.strip()
    if not auth_value.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header must use Bearer token format.",
        )

    key = auth_value[len("Bearer "):].strip()
    if not key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Empty API key.",
        )

    if key not in API_KEYS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API key.",
        )
    return key


def verify_websocket_token(token: str) -> bool:
    """
    Verify WebSocket connection token.
    Returns True if valid, False otherwise.

    If BRAINFLOW_API_KEYS is not set (development mode), authentication is skipped.
    """
    if API_KEYS is None:  # 未配置则跳过验证（开发模式）
        return True

    if not token:
        return False

    return token.strip() in API_KEYS
