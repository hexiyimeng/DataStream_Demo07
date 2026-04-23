import os
import socket
import multiprocessing
import logging
import platform
import torch

logger = logging.getLogger("BrainFlow.Config")
os.environ.setdefault("CELLPOSE_LOCAL_MODELS_PATH", os.path.join(os.path.dirname(os.path.dirname(__file__)), "models"))


def _get_system_memory_gb():
    """获取系统总内存（GB），跨平台兼容"""
    try:
        if platform.system() == "Windows":
            # Windows: 使用 ctypes 获取物理内存
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
            return mem_status.ullTotalPhys / (1024 ** 3)
        else:
            # Linux/macOS: 尝试读取 /proc/meminfo 或使用 psutil
            try:
                with open("/proc/meminfo", "r") as f:
                    for line in f:
                        if line.startswith("MemTotal:"):
                            # 单位是 kB
                            return float(line.split()[1]) / (1024 * 1024)
            except Exception:
                pass
            # Fallback: 使用 psutil（最可靠）
            try:
                import psutil
                return psutil.virtual_memory().total / (1024 ** 3)
            except Exception:
                pass
    except Exception as e:
        logger.debug(f"Failed to get system memory: {e}")
    return None  # 无法检测时返回 None


class AppConfig:
    """
    全局配置类：基于硬件资源的自适应配置

    设计原则：
    - 默认值保守，适合任意硬件
    - 所有配置都可通过环境变量覆盖
    - 资源检测结果写入日志，便于诊断
    """
    _instance = None

    # ---------- 基础配置（环境变量可覆盖） ----------
    N_WORKERS = 1
    CHUNK_MULTIPLE = 1
    DASHBOARD_ADDRESS = ":8787"
    DASHBOARD_HOST = None  # None 表示使用原始 dashboard_link

    # ---------- 新增：内存相关配置 ----------
    # Dask worker memory limit（单位：GB，0 表示 auto）
    WORKER_MEMORY_LIMIT_GB = 0  # 0 = 由 DaskService 自动推导
    # Dask spill 目录
    DASK_LOCAL_DIR = None  # None = 自动创建临时目录
    # 保守 chunk 模式阈值（当 chunk > 此值时启用警告）
    CHUNK_RISK_THRESHOLD_MB = 256

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AppConfig, cls).__new__(cls)
            cls._instance._detect_environment()
        return cls._instance

    def _detect_environment(self):
        hostname = socket.gethostname().lower()
        cpu_count = multiprocessing.cpu_count()
        sys_mem_gb = _get_system_memory_gb()

        # === GPU 检测 ===
        has_gpu = torch.cuda.is_available()
        gpu_count = torch.cuda.device_count() if has_gpu else 0
        gpu_vram_gb = 0

        if has_gpu:
            try:
                total_mem = torch.cuda.get_device_properties(0).total_memory
                gpu_vram_gb = total_mem / (1024 ** 3)
            except Exception as e:
                logger.debug(f"Failed to get GPU memory info: {e}")

        # === 打印硬件自检信息 ===
        gpu_info = f"GPU={has_gpu} (数量: {gpu_count}, 单卡VRAM: {gpu_vram_gb:.1f} GB)" if has_gpu else "GPU=False"
        mem_info = f"系统内存: {sys_mem_gb:.1f} GB" if sys_mem_gb else "系统内存: 未知"
        logger.info(f"  硬件自检: {gpu_info}, {mem_info}, CPU={cpu_count}")

        # === 模式识别（保守策略）===
        if gpu_count > 1:
            logger.info(f"   -> 识别模式: [多卡高并发模式]")
            self.N_WORKERS = gpu_count
            # 显存 > 20GB 时可适当增大 chunk，否则保守
            self.CHUNK_MULTIPLE = 4 if gpu_vram_gb > 20 else 2
        elif gpu_count == 1:
            logger.info(f"   -> 识别模式: [单卡保护模式]")
            self.N_WORKERS = 1
            self.CHUNK_MULTIPLE = 1
        else:
            logger.info(f"   -> 识别模式: [纯 CPU 模式]")
            self.N_WORKERS = max(1, cpu_count - 2)
            self.CHUNK_MULTIPLE = 1

        # === 自动推导 worker memory limit ===
        # 策略：总内存 / worker 数 * 0.7（预留 30% 给 OS + spill + 波动）
        if sys_mem_gb and self.N_WORKERS > 0:
            auto_memory_per_worker = (sys_mem_gb / self.N_WORKERS) * 0.7
            logger.info(f"   -> 自动推导: 每 worker memory_limit ≈ {auto_memory_per_worker:.1f} GB "
                        f"(系统 {sys_mem_gb:.1f} GB / {self.N_WORKERS} workers * 0.7 预留)")
        else:
            auto_memory_per_worker = None

        # === 环境变量覆盖（按优先级递增）===
        # BRAINFLOW_WORKERS
        if os.getenv("BRAINFLOW_WORKERS"):
            self.N_WORKERS = int(os.getenv("BRAINFLOW_WORKERS"))
            logger.warning(f"   -> [Override] BRAINFLOW_WORKERS={self.N_WORKERS}")

        # BRAINFLOW_CHUNK
        if os.getenv("BRAINFLOW_CHUNK"):
            self.CHUNK_MULTIPLE = int(os.getenv("BRAINFLOW_CHUNK"))
            logger.warning(f"   -> [Override] BRAINFLOW_CHUNK={self.CHUNK_MULTIPLE}")

        # BRAINFLOW_WORKER_MEMORY_LIMIT_GB（显式指定 worker 内存上限）
        if os.getenv("BRAINFLOW_WORKER_MEMORY_LIMIT_GB"):
            self.WORKER_MEMORY_LIMIT_GB = float(os.getenv("BRAINFLOW_WORKER_MEMORY_LIMIT_GB"))
            logger.warning(f"   -> [Override] BRAINFLOW_WORKER_MEMORY_LIMIT_GB={self.WORKER_MEMORY_LIMIT_GB}")

        # BRAINFLOW_DASK_LOCAL_DIR（spill 目录）
        if os.getenv("BRAINFLOW_DASK_LOCAL_DIR"):
            self.DASK_LOCAL_DIR = os.getenv("BRAINFLOW_DASK_LOCAL_DIR")
            logger.warning(f"   -> [Override] BRAINFLOW_DASK_LOCAL_DIR={self.DASK_LOCAL_DIR}")

        # BRAINFLOW_DASHBOARD_HOST
        if os.getenv("BRAINFLOW_DASHBOARD_HOST"):
            self.DASHBOARD_HOST = os.getenv("BRAINFLOW_DASHBOARD_HOST")

        # === 打印最终配置 ===
        mem_limit_str = f"{self.WORKER_MEMORY_LIMIT_GB:.1f} GB" if self.WORKER_MEMORY_LIMIT_GB else "auto"
        spill_str = self.DASK_LOCAL_DIR or "auto (临时目录)"
        logger.info(f" 最终生效配置: Workers={self.N_WORKERS}, ChunkMult={self.CHUNK_MULTIPLE}, "
                    f"WorkerMemLimit={mem_limit_str}, SpillDir={spill_str}")


config = AppConfig()


config = AppConfig()