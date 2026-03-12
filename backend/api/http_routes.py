from fastapi import APIRouter, Depends
from core.registry import get_node_info
from core.auth import verify_api_key
from services.dask_service import dask_service

router = APIRouter()

@router.get("/object_info", dependencies=[Depends(verify_api_key)])
async def get_node_definitions():
    return get_node_info()

@router.get("/dashboard_url", dependencies=[Depends(verify_api_key)])
async def get_dashboard_url():
    """
    获取 Dask Dashboard 的实际 URL
    始终返回 localhost:8787，因为前端需要从浏览器访问本地运行的服务
    """
    client = dask_service.get_client()
    if client and client.dashboard_link:
        # 提取端口号（如果 Dask 使用了其他端口）
        # dashboard_link 格式通常是 "http://127.0.0.1:8787/status" 或 "http://192.168.x.x:8787/status"
        try:
            # 尝试提取端口号
            if ":" in client.dashboard_link:
                # 从 "http://127.0.0.1:8787/status" 提取 "8787"
                parts = client.dashboard_link.split(":")
                if len(parts) >= 3:
                    port = parts[2].split("/")[0]
                    # 始终返回 localhost
                    return {"dashboard_url": f"http://localhost:{port}"}
        except Exception:
            pass
        # 默认返回 localhost:8787
        return {"dashboard_url": "http://localhost:8787"}
    return {"dashboard_url": "http://localhost:8787"}