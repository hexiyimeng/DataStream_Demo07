import logging
import platform
import os
import time
import dask.config
from dask.distributed import Client, LocalCluster

# 引用统一的 logger
from core.logger import logger
from core.config import config
from distributed import WorkerPlugin

# torch 延迟导入：避免 PyTorch 未安装时整个模块无法加载
try:
    import torch
    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False
    logger.warning("PyTorch 未安装, GPU features will be disabled")

# ==========================================
# 新增：Windows 专属的多 GPU 绑定插件
# ==========================================
class WindowsMultiGPUPlugin(WorkerPlugin):
    def setup(self, worker):
        """当 Dask Worker 进程启动时执行"""
        try:
            import torch
            gpu_count = torch.cuda.device_count()
            if gpu_count > 0:
                # LocalCluster 默认会将 worker 命名为 0, 1, 2, 3...
                worker_idx = int(worker.name)
                # 轮询分配：将 worker ID 映射到具体的显卡 ID
                assigned_gpu = worker_idx % gpu_count
                # 给这个 worker 对象打上一个标签
                worker.assigned_gpu = f"cuda:{assigned_gpu}"
                logger.info(f"Worker {worker.name} successfully bound to {worker.assigned_gpu}")
            else:
                worker.assigned_gpu = "cpu"
        except Exception as e:
            # 防御性回退
            worker.assigned_gpu = "cuda:0"
            logger.debug(f"Failed to bind GPU for worker: {e}")


# ==========================================
# 配置 Dask：启用适度优化
# ==========================================
dask.config.set({
    "optimization.fuse.active": True,
    "optimization.fuse.max_width": 2,  # 限制融合宽度防止过度优化
    "array.chunk-size": "256MB",  # 推荐分块大小
    "distributed.worker.memory.target": False,  # 不主动触发垃圾回收
    "distributed.worker.memory.spill": 0.8,  # 80% 内存使用时溢出到磁盘
})


class DaskService:
    _instance = None
    client = None
    cluster = None

    # 默认值
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

        try:
            if HAS_TORCH and torch.cuda.is_available():
                gpu_count = torch.cuda.device_count()

                # ==== 核心改动：Windows 8卡专属多进程模式 ====
                if gpu_count > 1 and n_workers > 1:
                    logger.info(f"启动标准 LocalCluster 多进程模式 (Windows {gpu_count}卡特供版)")
                    self.cluster = LocalCluster(
                        n_workers=n_workers,
                        threads_per_worker=1,
                        processes=True,  # 强制开启多进程！
                        dashboard_address=config.DASHBOARD_ADDRESS,
                        silence_logs=logging.WARNING,
                    )
                    self.client = Client(self.cluster)

                    # 注册我们的插件，给这 8 个进程分别分配 cuda:0 到 cuda:7
                    self.client.register_plugin(WindowsMultiGPUPlugin(), name="windows_gpu_pinning")
                else:
                    # 单卡苟活模式（本地笔记本）
                    logger.info("启动标准 LocalCluster (单卡保护模式)")
                    self.cluster = LocalCluster(
                        n_workers=1,
                        threads_per_worker=1,
                        processes=False,
                        dashboard_address=config.DASHBOARD_ADDRESS,
                        silence_logs=logging.WARNING,
                    )
                    self.client = Client(self.cluster)
            else:
                # 纯 CPU 模式
                self.cluster = LocalCluster(
                    n_workers=n_workers,
                    threads_per_worker=1,
                    dashboard_address=config.DASHBOARD_ADDRESS,
                    silence_logs=logging.WARNING,
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
                        # Some versions don't expose __version__
                        logger.info(f"Cellpose imported successfully in worker (version unknown)")
                except ImportError as e:
                    logger.warning(f"Cellpose not available in worker: {e}")
                except Exception as e:
                    logger.warning(f"Error importing cellpose: {e}")

            # 在所有 workers 上导入 cellpose
            self.client.run(import_cellpose_on_workers)

            if platform.system() == "Linux":
                self.client.run_on_scheduler(
                    lambda dask_scheduler: dask_scheduler.loop.call_later(60, self._trim_memory))

            logger.info(f" Dask Cluster Ready: {self.client.dashboard_link}")
            return self.client

        except Exception as e:
            logger.error(f" Dask Start Failed: {e}")
            return None

    def stop_cluster(self):
        if self.client:
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
