import logging
import platform
import os
import time
import tempfile
import dask.config
from dask.distributed import Client, LocalCluster

# 引用统一的 logger
from core.logger import logger
from core.config import config
from distributed import WorkerPlugin

# torch 延迟导入
try:
    import torch
    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False
    logger.warning("PyTorch 未安装, GPU features will be disabled")


# ==========================================
# Dask 内存阈值配置（可被环境变量覆盖）
# ==========================================
def _get_dask_memory_thresholds():
    """
    获取 Dask worker 内存阈值，支持环境变量覆盖。
    返回 dict，包含 target/spill/pause/terminate
    """
    # 默认值：保守策略，避免 worker 内存触发过晚
    defaults = {
        "distributed.worker.memory.target": 0.60,  # 60% 时开始主动 spill 冷数据
        "distributed.worker.memory.spill": 0.70,   # 70% 时强制 spill 到磁盘
        "distributed.worker.memory.pause": 0.82,  # 82% 时暂停 worker（不再继续接收新 task）
        "distributed.worker.memory.terminate": 0.95,  # 95% 时杀死 worker
    }

    # 环境变量覆盖映射
    env_overrides = {
        "BRAINFLOW_DASK_TARGET": "distributed.worker.memory.target",
        "BRAINFLOW_DASK_SPILL": "distributed.worker.memory.spill",
        "BRAINFLOW_DASK_PAUSE": "distributed.worker.memory.pause",
        "BRAINFLOW_DASK_TERMINATE": "distributed.worker.memory.terminate",
    }

    result = defaults.copy()
    for env_var, config_key in env_overrides.items():
        env_val = os.getenv(env_var)
        if env_val is not None:
            result[config_key] = float(env_val)
            logger.warning(f"   -> [Override] {config_key}={env_val} (via {env_var})")

    return result


# ==========================================
# 计算 worker memory_limit
# ==========================================
def _compute_worker_memory_limit():
    """
    计算每个 Dask worker 的 memory_limit。

    策略：
    1. 用户显式配置（WORKER_MEMORY_LIMIT_GB > 0）优先
    2. 否则自动推导：
       - 系统内存 / worker 数 * 0.7（预留 30% 给 OS、scheduler、spill）
    """
    if config.WORKER_MEMORY_LIMIT_GB > 0:
        limit_str = f"{config.WORKER_MEMORY_LIMIT_GB}GB"
        logger.info(f"   -> Worker memory_limit: {limit_str} (用户显式配置)")
        return limit_str

    # 自动推导
    try:
        if platform.system() == "Windows":
            import ctypes
            kernel32 = ctypes.windll.kernel32
            c_ulong = ctypes.c_ulong
            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [
                    ("dwLength", c_ulong),
                    ("dwMemoryLoad", c_ulong),
                    ("ullTotalPhys", c_ulong),
                    ("ullAvailPhys", c_ulong),
                    ("ullTotalPageFile", c_ulong),
                    ("ullAvailPageFile", c_ulong),
                    ("ullTotalVirtual", c_ulong),
                    ("ullAvailVirtual", c_ulong),
                    ("sullAvailExtendedVirtual", c_ulong),
                ]
            mem_status = MEMORYSTATUSEX()
            mem_status.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
            kernel32.GlobalMemoryStatusEx(ctypes.byref(mem_status))
            sys_mem_gb = mem_status.ullTotalPhys / (1024 ** 3)
        else:
            # Linux: 读取 /proc/meminfo
            sys_mem_gb = None
            try:
                with open("/proc/meminfo", "r") as f:
                    for line in f:
                        if line.startswith("MemTotal:"):
                            sys_mem_gb = float(line.split()[1]) / (1024 * 1024)
                            break
            except Exception:
                pass

        if sys_mem_gb and config.N_WORKERS > 0:
            # 预留 30% 给 OS + scheduler + spill + 波动
            per_worker_mem_gb = (sys_mem_gb / config.N_WORKERS) * 0.7
            limit_str = f"{per_worker_mem_gb:.1f}GB"
            logger.info(f"   -> Worker memory_limit: {limit_str} "
                        f"(系统 {sys_mem_gb:.1f} GB / {config.N_WORKERS} workers * 0.7 预留)")
            return limit_str
    except Exception as e:
        logger.debug(f"Failed to compute auto memory limit: {e}")

    # Fallback: 使用 psutil（最可靠的跨平台方式）
    try:
        import psutil
        sys_mem_gb = psutil.virtual_memory().total / (1024 ** 3)
        per_worker_mem_gb = (sys_mem_gb / config.N_WORKERS) * 0.7
        limit_str = f"{per_worker_mem_gb:.1f}GB"
        logger.info(f"   -> Worker memory_limit: {limit_str} "
                    f"(系统 {sys_mem_gb:.1f} GB / {config.N_WORKERS} workers * 0.7, via psutil)")
        return limit_str
    except Exception as e2:
        logger.debug(f"psutil also failed: {e2}")

    # 最后 Fallback: 最小保证值
    MIN_MEMORY_GB = 2.0
    logger.warning(f"   -> Worker memory_limit: {MIN_MEMORY_GB:.1f}GB (最小保证 fallback，"
                  f"系统内存检测失败！建议设置 BRAINFLOW_WORKER_MEMORY_LIMIT_GB 环境变量)")
    return f"{MIN_MEMORY_GB}GB"


# ==========================================
# 获取/创建 Dask spill 目录
# ==========================================
def _get_dask_local_dir():
    """
    获取 Dask spill 目录，支持环境变量覆盖。
    """
    if config.DASK_LOCAL_DIR:
        dask_dir = config.DASK_LOCAL_DIR
    else:
        # 自动创建临时目录
        base_dir = tempfile.gettempdir()
        dask_dir = os.path.join(base_dir, "brainflow_dask_spill")
        os.makedirs(dask_dir, exist_ok=True)

    logger.info(f"   -> Dask spill directory: {dask_dir}")
    return dask_dir


# ==========================================
# Windows 多 GPU 绑定插件
# ==========================================
class WindowsMultiGPUPlugin(WorkerPlugin):
    def setup(self, worker):
        """当 Dask Worker 进程启动时执行"""
        try:
            import torch
            gpu_count = torch.cuda.device_count()
            if gpu_count > 0:
                worker_idx = int(worker.name)
                assigned_gpu = worker_idx % gpu_count
                worker.assigned_gpu = f"cuda:{assigned_gpu}"
                logger.info(f"Worker {worker.name} successfully bound to {worker.assigned_gpu}")
            else:
                worker.assigned_gpu = "cpu"
        except Exception as e:
            worker.assigned_gpu = "cuda:0"
            logger.debug(f"Failed to bind GPU for worker: {e}")


# ==========================================
# 配置 Dask 内存阈值
# ==========================================
_memory_thresholds = _get_dask_memory_thresholds()
dask.config.set({
    "optimization.fuse.active": True,
    "optimization.fuse.max_width": 2,
    "array.chunk-size": "256MB",
    # 内存阈值（不再设为 False）
    "distributed.worker.memory.target": _memory_thresholds["distributed.worker.memory.target"],
    "distributed.worker.memory.spill": _memory_thresholds["distributed.worker.memory.spill"],
    "distributed.worker.memory.pause": _memory_thresholds["distributed.worker.memory.pause"],
    "distributed.worker.memory.terminate": _memory_thresholds["distributed.worker.memory.terminate"],
})

logger.info(f"[DaskConfig] 内存阈值: target={_memory_thresholds['distributed.worker.memory.target']}, "
            f"spill={_memory_thresholds['distributed.worker.memory.spill']}, "
            f"pause={_memory_thresholds['distributed.worker.memory.pause']}, "
            f"terminate={_memory_thresholds['distributed.worker.memory.terminate']}")


class DaskService:
    _instance = None
    client = None
    cluster = None

    recommended_chunk_multiple = 1

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DaskService, cls).__new__(cls)
        return cls._instance

    def get_client(self):
        if self.client: return self.client
        try:
            return Client.current()
        except Exception as e:
            logger.debug(f"Failed to get current client: {e}")
            return None

    def start_cluster(self):
        """启动 Dask 集群"""
        if self.client: return self.client

        logger.info("Initializing Dask LocalCluster...")
        n_workers = config.N_WORKERS

        # 计算 worker memory_limit
        memory_limit = _compute_worker_memory_limit()
        # 获取 spill 目录
        dask_local_dir = _get_dask_local_dir()

        try:
            if HAS_TORCH and torch.cuda.is_available():
                gpu_count = torch.cuda.device_count()

                if gpu_count > 1 and n_workers > 1:
                    logger.info(f"启动 LocalCluster 多进程模式 (Windows {gpu_count}卡)")
                    self.cluster = LocalCluster(
                        n_workers=n_workers,
                        threads_per_worker=1,
                        processes=True,
                        dashboard_address=config.DASHBOARD_ADDRESS,
                        silence_logs=logging.WARNING,
                        memory_limit=memory_limit,
                        local_directory=dask_local_dir,
                    )
                    self.client = Client(self.cluster)
                    self.client.register_plugin(WindowsMultiGPUPlugin(), name="windows_gpu_pinning")
                else:
                    logger.info("启动 LocalCluster (单卡保护模式)")
                    self.cluster = LocalCluster(
                        n_workers=1,
                        threads_per_worker=1,
                        processes=False,
                        dashboard_address=config.DASHBOARD_ADDRESS,
                        silence_logs=logging.WARNING,
                        memory_limit=memory_limit,
                        local_directory=dask_local_dir,
                    )
                    self.client = Client(self.cluster)
            else:
                logger.info(f"启动 LocalCluster (纯 CPU 模式, {n_workers} workers)")
                self.cluster = LocalCluster(
                    n_workers=n_workers,
                    threads_per_worker=1,
                    dashboard_address=config.DASHBOARD_ADDRESS,
                    silence_logs=logging.WARNING,
                    memory_limit=memory_limit,
                    local_directory=dask_local_dir,
                )
                self.client = Client(self.cluster)

            # 预先导入 cellpose 到 workers 中
            def import_cellpose_on_workers():
                try:
                    from cellpose import models
                    try:
                        from cellpose import __version__
                        logger.info(f"Cellpose imported successfully in worker (version {__version__})")
                    except ImportError:
                        logger.info(f"Cellpose imported successfully in worker (version unknown)")
                except ImportError as e:
                    logger.warning(f"Cellpose not available in worker: {e}")
                except Exception as e:
                    logger.warning(f"Error importing cellpose: {e}")

            self.client.run(import_cellpose_on_workers)

            if platform.system() == "Linux":
                self.client.run_on_scheduler(
                    lambda dask_scheduler: dask_scheduler.loop.call_later(60, self._trim_memory))

            # 打印最终诊断信息
            logger.info(f"[DaskService] 集群启动成功:")
            logger.info(f"  - Dashboard: {self.client.dashboard_link}")
            logger.info(f"  - Workers: {n_workers}")
            logger.info(f"  - Worker memory_limit: {memory_limit}")
            logger.info(f"  - Spill directory: {dask_local_dir}")
            logger.info(f"  - 内存阈值: target={_memory_thresholds['distributed.worker.memory.target']}, "
                        f"spill={_memory_thresholds['distributed.worker.memory.spill']}, "
                        f"pause={_memory_thresholds['distributed.worker.memory.pause']}, "
                        f"terminate={_memory_thresholds['distributed.worker.memory.terminate']}")

            return self.client

        except Exception as e:
            logger.error(f" Dask Start Failed: {e}")
            return None

    def stop_cluster(self):
        # 清理所有 Worker 上的 Cellpose 模型缓存（释放 GPU 显存）
        if self.client:
            try:
                from nodes.cellpose_node import force_clear_cellpose_model_cache
                force_clear_cellpose_model_cache()
                logger.info("[DaskService] Cellpose model cache cleared on cluster stop")
            except Exception as e:
                logger.debug(f"Failed to clear cellpose cache on stop: {e}")

            try:
                self.client.close()
            except Exception as e:
                logger.warning(f"Error closing client: {e}")
            self.client = None
        if self.cluster:
            try:
                self.cluster.close()
            except Exception as e:
                logger.warning(f"Error closing cluster: {e}")
            self.cluster = None

    def _trim_memory(self):
        import gc
        import ctypes
        gc.collect()
        try:
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except Exception as e:
            logger.debug(f"Memory trim failed: {e}")


dask_service = DaskService()
