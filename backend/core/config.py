import os
import socket
import multiprocessing
import logging
import torch

logger = logging.getLogger("BrainFlow.Config")


class AppConfig:
    """
    全局配置类：基于 显存(VRAM)容量 的配置
    """
    _instance = None

    # 默认兜底
    N_WORKERS = 1
    CHUNK_MULTIPLE = 1
    DASHBOARD_ADDRESS = ":8787"
    # Dashboard 外部访问地址（用于远程部署/反向代理）
    # 可通过 BRAINFLOW_DASHBOARD_HOST 环境变量覆盖
    DASHBOARD_HOST = None  # None 表示使用原始 dashboard_link

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AppConfig, cls).__new__(cls)
            cls._instance._detect_environment()
        return cls._instance

    def _detect_environment(self):
        hostname = socket.gethostname().lower()
        cpu_count = multiprocessing.cpu_count()

        # === 显存与显卡数量检测逻辑 ===
        has_gpu = torch.cuda.is_available()
        gpu_count = torch.cuda.device_count() if has_gpu else 0
        gpu_vram_gb = 0

        if has_gpu:
            try:
                # 获取第一块卡的显存大小 (单位: 字节 -> GB)
                total_mem = torch.cuda.get_device_properties(0).total_memory
                gpu_vram_gb = total_mem / (1024 ** 3)
            except Exception as e:
                logger.debug(f"Failed to get GPU memory info: {e}")
                gpu_vram_gb = 0

        logger.info(f"  硬件自检: GPU={has_gpu} (数量: {gpu_count}, 单卡VRAM: {gpu_vram_gb:.1f} GB), CPU={cpu_count}")

        # =========================================================
        # 策略 1: 多 GPU 分布式模式 (如 8x RTX 3090)
        # =========================================================
        if gpu_count > 1:
            logger.info(f"   -> 识别模式: [多卡高并发模式]")
            self.N_WORKERS = gpu_count  # 有几张卡就开几个 Worker
            # 显存大于 20GB (如 3090) 则拉高 Chunk，否则保守
            self.CHUNK_MULTIPLE = 4 if gpu_vram_gb > 20 else 2

        # =========================================================
        # 策略 2: 单 GPU 苟活模式 (如 本地 RTX 3050)
        # =========================================================
        elif gpu_count == 1:
            logger.info("   -> 识别模式: [单卡保护模式]")
            self.N_WORKERS = 1
            self.CHUNK_MULTIPLE = 1

        # =========================================================
        # 策略 3: 纯 CPU 模式
        # =========================================================
        else:
            logger.info("   -> 识别模式: [纯 CPU 模式]")
            self.N_WORKERS = max(1, cpu_count - 2)
            self.CHUNK_MULTIPLE = 1

        # ==== 环境变量强制覆盖逻辑保持不变 ====
        if os.getenv("BRAINFLOW_WORKERS"):
            self.N_WORKERS = int(os.getenv("BRAINFLOW_WORKERS"))
            logger.warning(f"   -> [Override] 环境变量强制 Workers={self.N_WORKERS}")

        if os.getenv("BRAINFLOW_CHUNK"):
            self.CHUNK_MULTIPLE = int(os.getenv("BRAINFLOW_CHUNK"))
            logger.warning(f"   -> [Override] 环境变量强制 ChunkMult={self.CHUNK_MULTIPLE}")

        if os.getenv("BRAINFLOW_DASHBOARD_HOST"):
            self.DASHBOARD_HOST = os.getenv("BRAINFLOW_DASHBOARD_HOST")

        logger.info(f" 最终生效配置: Workers={self.N_WORKERS}, Chunk={self.CHUNK_MULTIPLE}")


config = AppConfig()