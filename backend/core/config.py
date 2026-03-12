import os
import socket
import multiprocessing
import logging
import torch

# 配置日志
logging.basicConfig(level=logging.INFO)
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

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AppConfig, cls).__new__(cls)
            cls._instance._detect_environment()
        return cls._instance

    def _detect_environment(self):
        hostname = socket.gethostname().lower()
        cpu_count = multiprocessing.cpu_count()

        # === 显存检测逻辑 (核心修复) ===
        gpu_vram_gb = 0
        has_gpu = torch.cuda.is_available()

        if has_gpu:
            try:
                # 获取第一块卡的显存大小 (单位: 字节 -> GB)
                total_mem = torch.cuda.get_device_properties(0).total_memory
                gpu_vram_gb = total_mem / (1024 ** 3)
            except Exception as e:
                logger.debug(f"Failed to get GPU memory info: {e}")
                gpu_vram_gb = 0

        logger.info(f"  硬件自检: GPU={has_gpu} (VRAM: {gpu_vram_gb:.1f} GB), CPU={cpu_count}")

        # =========================================================
        # 策略 1: 生产服务器 (大显存/高延迟)
        # =========================================================
        if "server" in hostname or "202.38" in hostname:
            logger.info("   -> 识别模式: [生产服务器]")
            self.CHUNK_MULTIPLE = 4  # 必须加大块，防止 IO 卡死

            # 如果是服务器级别的卡 (A100/3090, VRAM > 20GB)，可以开 4 个
            if gpu_vram_gb > 20:
                self.N_WORKERS = 4
            # 普通服务器卡 (10-20GB)，保守开 2-3 个
            elif gpu_vram_gb > 10:
                self.N_WORKERS = 2
            else:
                self.N_WORKERS = 1

        # =========================================================
        # 策略 2: 本地开发机 (RTX 3050 等消费级显卡)
        # =========================================================
        elif "song" in hostname or "desktop" in hostname or has_gpu:
            logger.info("   -> 识别模式: [本地/消费级显卡]")
            self.CHUNK_MULTIPLE = 1  # 本地硬盘快，小块更灵活

            # --- 关键修复：根据显存决定 Worker 数量 ---
            if gpu_vram_gb < 6:
                # 3050 Laptop (4GB) -> 只能开 1 个！绝对不能多！
                logger.warning(f"   显存较小 ({gpu_vram_gb:.1f}GB)，强制单 Worker 以防 OOM")
                self.N_WORKERS = 1
            elif gpu_vram_gb < 10:
                # 3050/3060/3070 (8GB) -> 建议 1 个，最多 2 个
                # Cellpose 加载一次模型约 1-2GB，加上数据处理，2个都很勉强
                # 为了稳，默认 1 个，利用率低点总比崩了好
                self.N_WORKERS = 1
            elif gpu_vram_gb < 20:
                # 3080/4080 (12GB-16GB) -> 可以开 2-3 个
                self.N_WORKERS = 2
            else:
                # 3090/4090 (24GB) -> 可以开 4 个
                self.N_WORKERS = 4

            # 如果是纯 CPU 模式（没显卡），则拉满 CPU
            if not has_gpu:
                self.N_WORKERS = max(1, cpu_count - 2)

        # =========================================================
        # 策略 3: 环境变量强制覆盖 (给你留个后门)
        # =========================================================
        if os.getenv("BRAINFLOW_WORKERS"):
            self.N_WORKERS = int(os.getenv("BRAINFLOW_WORKERS"))
            logger.warning(f"   -> [Override] 环境变量强制 Workers={self.N_WORKERS}")

        if os.getenv("BRAINFLOW_CHUNK"):
            self.CHUNK_MULTIPLE = int(os.getenv("BRAINFLOW_CHUNK"))
            logger.warning(f"   -> [Override] 环境变量强制 ChunkMult={self.CHUNK_MULTIPLE}")

        logger.info(f" 最终生效配置: Workers={self.N_WORKERS}, Chunk={self.CHUNK_MULTIPLE}")


config = AppConfig()