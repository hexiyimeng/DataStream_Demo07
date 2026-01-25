# backend/dask_manager.py
import logging
import platform  # <--- 新增
import ctypes  # <--- 新增
import torch
from dask.distributed import Client, LocalCluster

logger = logging.getLogger("BrainFlow.Dask")

_GLOBAL_CLIENT = None
_GLOBAL_CLUSTER = None


# === 跨平台内存清理函数 ===
def trim_memory():
    """
    运行在 Worker 上的清理函数。
    自动判断系统：Linux 强制归还内存，Windows 仅做 GC。
    """
    import gc
    import platform
    import ctypes

    # 1. Python 层清理 (通用)
    gc.collect()

    # 2. 操作系统层清理 (Linux 独占神器)
    # 这能极大降低 Unmanaged Memory
    if platform.system() == "Linux":
        try:
            libc = ctypes.CDLL("libc.so.6")
            libc.malloc_trim(0)
        except Exception:
            pass

    return 0


def start_dask_cluster():
    global _GLOBAL_CLIENT, _GLOBAL_CLUSTER

    if _GLOBAL_CLIENT is not None:
        logger.info("Dask 集群已在运行，跳过启动。")
        return _GLOBAL_CLIENT

    logger.info("正在初始化 Dask 本地集群...")

    # 1. 硬件资源判断
    if torch.cuda.is_available():
        n_workers = 1
        threads_per_worker = 1  # GPU 必须单线程
        logger.info("检测到 GPU: 启用单 Worker 单线程模式。")
    else:
        # CPU 模式：Linux 和 Windows 策略不同
        import multiprocessing
        cpu_count = multiprocessing.cpu_count()

        # 留 1-2 个核给系统和 Web 服务
        n_workers = max(1, cpu_count - 2)
        threads_per_worker = 1
        logger.info(f"CPU 模式: 启用 {n_workers} Worker 并行。")

    try:
        # 2. 启动集群
        _GLOBAL_CLUSTER = LocalCluster(
            processes=True,
            n_workers=n_workers,
            threads_per_worker=threads_per_worker,
            dashboard_address=':8787'
        )
        _GLOBAL_CLIENT = Client(_GLOBAL_CLUSTER)

        # === 关键修改：注册 Linux 内存自动回收 ===
        # 告诉 Dask Scheduler：每隔 60 秒，命令所有 Worker 执行一次 trim_memory
        if platform.system() == "Linux":
            logger.info("🐧 Linux 环境检测: 已启用 malloc_trim 自动内存回收 (60s 周期)")
            _GLOBAL_CLIENT.run_on_scheduler(
                lambda dask_scheduler: dask_scheduler.loop.call_later(
                    60,
                    lambda: _GLOBAL_CLIENT.run(trim_memory)
                )
            )
        else:
            logger.info("🪟 Windows 环境检测: malloc_trim 不可用 (仅依赖 Python GC)")

        link = _GLOBAL_CLIENT.dashboard_link
        logger.info(f"Dask 集群启动成功!")
        logger.info(f"监控仪表盘: {link}")

        return _GLOBAL_CLIENT

    except Exception as e:
        logger.error(f"Dask 启动失败: {e}")
        return None


def get_client():
    if _GLOBAL_CLIENT: return _GLOBAL_CLIENT
    try:
        return Client.current()
    except:
        return None


def stop_dask_cluster():
    global _GLOBAL_CLIENT, _GLOBAL_CLUSTER
    if _GLOBAL_CLIENT: _GLOBAL_CLIENT.close()
    if _GLOBAL_CLUSTER: _GLOBAL_CLUSTER.close()
    logger.info("Dask 集群已关闭。")