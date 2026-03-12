from fastapi import HTTPException, Header, status, Depends
import os

API_KEYS = set(os.getenv("BRAINFLOW_API_KEYS", "").split(",")) if os.getenv("BRAINFLOW_API_KEYS") else None


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
    key = authorization.replace("Bearer ", "")
    if key not in API_KEYS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API key.",
        )
    return key
