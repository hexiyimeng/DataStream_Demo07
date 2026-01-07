# monitor_service.py
import asyncio
import psutil

# 全局 Dask 客户端缓存
_DASK_CLIENT = None


def get_dask_dashboard():
    """获取 Dask 仪表盘地址，如果没启动则尝试启动"""
    global _DASK_CLIENT
    try:
        from dask.distributed import Client
        if _DASK_CLIENT is None:
            # processes=False 适合图像处理，避免多进程内存拷贝开销
            _DASK_CLIENT = Client(processes=False)
        return _DASK_CLIENT.dashboard_link
    except ImportError:
        return None
    except Exception as e:
        print(f"Dask Init Error: {e}")
        return None


async def run_monitor_loop(websocket):
    """独立的监控循环任务"""
    try:
        # 1. 先发一次 Dask 状态
        dash_link = get_dask_dashboard()
        if dash_link:
            await websocket.send_json({"type": "log", "message": f"📊 Dask Dashboard: {dash_link}"})

        # 2. 循环汇报系统资源
        while True:
            mem = psutil.virtual_memory()
            cpu = psutil.cpu_percent()

            # 只发送日志，不干扰主流程
            msg = f"🖥️ [System] RAM: {mem.percent}% | CPU: {cpu}%"
            await websocket.send_json({"type": "log", "message": msg})

            await asyncio.sleep(2)  # 每2秒一次

    except asyncio.CancelledError:
        pass  # 正常停止
    except Exception as e:
        print(f"Monitor Error: {e}")