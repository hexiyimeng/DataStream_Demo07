import logging
import platform
import os
import time
import dask.config
import torch
from dask.distributed import Client, LocalCluster

# 引用统一的 logger
from core.logger import logger
from core.config import config


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

        # 读取配置
        n_workers = config.N_WORKERS
        self.recommended_chunk_multiple = config.CHUNK_MULTIPLE

        resources = {}
        env_vars = {}

        if torch.cuda.is_available():
            logger.info(f" GPU Mode: Enabled")
            resources = {"GPU": 1}
            env_vars["CUDA_VISIBLE_DEVICES"] = os.getenv("CUDA_VISIBLE_DEVICES", "0")

        try:
            # 使用 threads 模式（processes=False），以便 workers 可以访问相同的 Python 环境和包
            # 这样可以确保 Cellpose 等模块在 workers 中可用
            self.cluster = LocalCluster(
                n_workers=n_workers,
                threads_per_worker=1,
                processes=False,  # 使用 threads 而不是 processes
                resources=resources,
                dashboard_address=config.DASHBOARD_ADDRESS,
                silence_logs=logging.WARNING,
                memory_limit=0,  # 自动检测内存限制
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

    def bind_layers_to_node(self, layer_names, node_id):
        pass

    def monitor_task(self, future, callback=None, global_progress_callback=None):
        if not self.get_client(): return

        logger.info(f"Waiting for Dask task: {future.key}")

        while not future.done():
            if callback:
                callback(0, 100, "Cluster Processing (Writer)...")
            time.sleep(0.5)

        if future.status == 'error':
            future.result()

        return future.result()


dask_service = DaskService()