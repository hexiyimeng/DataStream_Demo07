import logging
import platform
import os
import time
import tempfile
import dask.config
from dask.distributed import Client, LocalCluster

# 引用统一�?logger
from core.logger import logger
from core.config import config
from distributed import WorkerPlugin

def _detect_cuda_for_cluster():
    """Lazily check CUDA when starting the cluster, not while importing this module."""
    try:
        import torch
    except Exception as e:
        logger.debug(f"PyTorch unavailable; GPU cluster mode disabled: {e}")
        return False, 0

    try:
        has_gpu = bool(torch.cuda.is_available())
        gpu_count = int(torch.cuda.device_count()) if has_gpu else 0
        return has_gpu, gpu_count
    except Exception as e:
        logger.debug(f"CUDA detection failed; GPU cluster mode disabled: {e}")
        return False, 0


# ==========================================
# Dask 内存阈值配置（可被环境变量覆盖�?# ==========================================
def _get_dask_memory_thresholds():
    """
    获取 Dask worker 内存阈值，支持环境变量覆盖�?    返回 dict，包�?target/spill/pause/terminate
    """
    # 默认值：保守策略，避�?worker 内存触发过晚
    defaults = {
        "distributed.worker.memory.target": 0.60,
        "distributed.worker.memory.spill": 0.70,
        "distributed.worker.memory.pause": 0.82,
        "distributed.worker.memory.terminate": 0.95,
    }

    # 环境变量覆盖映射
    env_overrides = {
        "WorkFlow_DASK_TARGET": "distributed.worker.memory.target",
        "WorkFlow_DASK_SPILL": "distributed.worker.memory.spill",
        "WorkFlow_DASK_PAUSE": "distributed.worker.memory.pause",
        "WorkFlow_DASK_TERMINATE": "distributed.worker.memory.terminate",
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
def _compute_worker_memory_limit(n_workers=None):
    """Compute the per-worker Dask memory limit."""
    worker_count = n_workers or config.N_WORKERS or 1

    if config.WORKER_MEMORY_LIMIT_GB > 0:
        limit_str = f"{config.WORKER_MEMORY_LIMIT_GB}GB"
        logger.info(f"   -> Worker memory_limit: {limit_str} (explicit config)")
        return limit_str

    sys_mem_gb = None
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
            try:
                with open("/proc/meminfo", "r") as f:
                    for line in f:
                        if line.startswith("MemTotal:"):
                            sys_mem_gb = float(line.split()[1]) / (1024 * 1024)
                            break
            except Exception:
                pass

        if sys_mem_gb is None:
            import psutil
            sys_mem_gb = psutil.virtual_memory().total / (1024 ** 3)

        per_worker_mem_gb = (sys_mem_gb / worker_count) * 0.7
        limit_str = f"{per_worker_mem_gb:.1f}GB"
        logger.info(
            f"   -> Worker memory_limit: {limit_str} "
            f"(system {sys_mem_gb:.1f} GB / {worker_count} workers * 0.7)"
        )
        return limit_str
    except Exception as e:
        logger.debug(f"Failed to compute auto memory limit: {e}")

    min_memory_gb = 2.0
    logger.warning(
        f"   -> Worker memory_limit: {min_memory_gb:.1f}GB fallback; "
        "set WorkFlow_WORKER_MEMORY_LIMIT_GB to override."
    )
    return f"{min_memory_gb}GB"


# ==========================================
# Dask spill directory
# ==========================================
def _get_dask_local_dir():
    """Return the Dask spill directory, honoring config/env overrides."""
    if config.DASK_LOCAL_DIR:
        dask_dir = config.DASK_LOCAL_DIR
    else:
        # 自动创建临时目录
        base_dir = tempfile.gettempdir()
        dask_dir = os.path.join(base_dir, "WorkFlow_dask_spill")
        os.makedirs(dask_dir, exist_ok=True)

    logger.info(f"   -> Dask spill directory: {dask_dir}")
    return dask_dir


# ==========================================
# Windows �?GPU 绑定插件
# ==========================================
class WindowsMultiGPUPlugin(WorkerPlugin):
    def setup(self, worker):
        """Bind a Dask worker process to a GPU when CUDA is available."""
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
            # Only default to cuda:0 if explicitly allowed; otherwise fall back to CPU
            # to prevent silent multi-worker contention on cuda:0.
            allow_implicit = os.getenv("WorkFlow_ALLOW_IMPLICIT_CUDA0", "").lower() in ("1", "true", "yes")
            worker.assigned_gpu = "cuda:0" if allow_implicit else "cpu"
            logger.debug(f"Failed to bind GPU for worker {worker.name}, assigned={worker.assigned_gpu}: {e}")


# ==========================================
# 配置 Dask 内存阈�?# ==========================================
_memory_thresholds = _get_dask_memory_thresholds()
_worker_ttl = os.getenv("WorkFlow_DASK_WORKER_TTL", "2h")
dask.config.set({
    "optimization.fuse.active": True,
    "optimization.fuse.max_width": 2,
    "array.chunk-size": "256MB",
    "distributed.worker.memory.target": _memory_thresholds["distributed.worker.memory.target"],
    "distributed.worker.memory.spill": _memory_thresholds["distributed.worker.memory.spill"],
    "distributed.worker.memory.pause": _memory_thresholds["distributed.worker.memory.pause"],
    "distributed.worker.memory.terminate": _memory_thresholds["distributed.worker.memory.terminate"],
    # Worker TTL �?prevents orphaned workers from holding resources forever
    "distributed.scheduler.worker-ttl": _worker_ttl,
})

logger.info(f"[DaskConfig] 内存阈�? target={_memory_thresholds['distributed.worker.memory.target']}, "
            f"spill={_memory_thresholds['distributed.worker.memory.spill']}, "
            f"pause={_memory_thresholds['distributed.worker.memory.pause']}, "
            f"terminate={_memory_thresholds['distributed.worker.memory.terminate']}, "
            f"worker-ttl={_worker_ttl}")


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
        memory_limit = None
        # 获取 spill 目录
        dask_local_dir = _get_dask_local_dir()

        try:
            has_gpu, gpu_count = _detect_cuda_for_cluster()
            if has_gpu and not os.getenv("WorkFlow_WORKERS"):
                n_workers = gpu_count if gpu_count > 1 else 1
            memory_limit = _compute_worker_memory_limit(n_workers)
            if has_gpu:

                if gpu_count > 1 and n_workers > 1:
                    logger.info(f"启动 LocalCluster 多进程模�?(Windows {gpu_count}�?")
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
                        processes=True,
                        dashboard_address=config.DASHBOARD_ADDRESS,
                        silence_logs=logging.WARNING,
                        memory_limit=memory_limit,
                        local_directory=dask_local_dir,
                    )
                    logger.info("Starting LocalCluster (single GPU protected process mode: 1 worker, 1 thread, processes=True)")
                    self.client = Client(self.cluster)
                    self.client.register_plugin(WindowsMultiGPUPlugin(), name="windows_gpu_pinning")
            else:
                logger.info(f"启动 LocalCluster (�?CPU 模式, {n_workers} workers)")
                self.cluster = LocalCluster(
                    n_workers=n_workers,
                    threads_per_worker=1,
                    dashboard_address=config.DASHBOARD_ADDRESS,
                    silence_logs=logging.WARNING,
                    memory_limit=memory_limit,
                    local_directory=dask_local_dir,
                )
                self.client = Client(self.cluster)

            if platform.system() == "Linux":
                self.client.run_on_scheduler(
                    lambda dask_scheduler: dask_scheduler.loop.call_later(60, self._trim_memory))

            # 打印最终诊断信�?            logger.info(f"[DaskService] 集群启动成功:")
            logger.info(f"  - Dashboard: {self.client.dashboard_link}")
            logger.info(f"  - Workers: {n_workers}")
            logger.info(f"  - Worker memory_limit: {memory_limit}")
            logger.info(f"  - Spill directory: {dask_local_dir}")
            logger.info(f"  - 内存阈�? target={_memory_thresholds['distributed.worker.memory.target']}, "
                        f"spill={_memory_thresholds['distributed.worker.memory.spill']}, "
                        f"pause={_memory_thresholds['distributed.worker.memory.pause']}, "
                        f"terminate={_memory_thresholds['distributed.worker.memory.terminate']}")

            return self.client

        except Exception as e:
            logger.error(f" Dask Start Failed: {e}")
            return None

    def stop_cluster(self):
        # Clear worker cache on all workers.
        if self.client:
            try:
                from core.worker_cache import force_clear_worker_cache

                stats = self.client.run(force_clear_worker_cache)
                logger.info(f"[DaskService] Worker cache cleared on cluster stop: {stats}")
            except Exception as e:
                logger.debug(f"Failed to clear worker cache on stop: {e}")

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
