# backend/dask_manager.py
import logging
import platform
import ctypes
import re
import dask.config
import torch
from dask.distributed import Client, LocalCluster, SchedulerPlugin

# ==========================================
# [重要决策] 暂时强制禁用优化
# 这是让进度条 "立即复活" 的最稳妥方案。
# 等跑通后，我们可以再尝试开启它。
# ==========================================
dask.config.set({"optimization.fuse.active": False})

logger = logging.getLogger("BrainFlow.Dask")

_GLOBAL_CLIENT = None
_GLOBAL_CLUSTER = None


# =============================================================================
# [核心修复] 修正后的全局注册函数
# =============================================================================
def _global_register_mapping(names, nid, dask_scheduler=None):
    """
    运行在 Scheduler 进程中。建立 LayerName -> NodeID 的映射。
    """
    if not dask_scheduler: return

    try:
        plugin = dask_scheduler.plugins.get('brainflow_progress')
        if plugin:
            for name in names:
                plugin.register_manual_mapping(name, nid)
            # 打印日志证明绑定成功 (在控制台可见)
            print(f"✅ [Scheduler] Bound {names} -> {nid}")
    except Exception as e:
        print(f"❌ [Scheduler] Bind Error: {e}")


# ==========================================
# [调用入口] 全局绑定函数
# ==========================================
def bind_layers_to_node(layer_names, node_id):
    client = get_client()
    if not client: return

    try:
        if isinstance(layer_names, str): layer_names = [layer_names]
        # 发送指令给 Scheduler
        client.run_on_scheduler(_global_register_mapping, layer_names, node_id)
        # 本地也打印一下
        logger.info(f"🔗 Requesting Bind: {layer_names} -> {node_id}")
    except Exception as e:
        logger.warning(f"Binding failed: {e}")


class BrainFlowProgressPlugin(SchedulerPlugin):
    def __init__(self):
        self.node_stats = {}
        self.id_pattern = re.compile(r"([a-zA-Z0-9]+_\d{10,14})")
        self.layer_to_id = {}
        self.group_id_cache = {}

    def start(self, scheduler):
        self.scheduler = scheduler

    def register_manual_mapping(self, layer_name, node_id):
        self.layer_to_id[layer_name] = node_id

    def update_graph(self, scheduler, *args, **kwargs):
        pass

    def get_progress_snapshot(self):
        snapshot = {}

        # 遍历所有任务组
        for group_name, task_group in self.scheduler.task_groups.items():

            node_ids = set()

            # --- A. 查缓存 ---
            if group_name in self.group_id_cache:
                node_ids = self.group_id_cache[group_name]

            else:
                # --- B. 查映射表 (精确匹配) ---
                if group_name in self.layer_to_id:
                    node_ids.add(self.layer_to_id[group_name])

                # --- C. 查前缀 (融合任务) ---
                elif group_name.startswith("fused_"):
                    for layer_name, nid in self.layer_to_id.items():
                        if layer_name in group_name:
                            node_ids.add(nid)

                # --- D. 查正则 ---
                if not node_ids:
                    node_ids.update(self.id_pattern.findall(group_name))

                node_ids = list(node_ids)
                self.group_id_cache[group_name] = node_ids

                # [上帝视角] 打印未被识别的任务，帮我们找原因
                # 只有当它不是内部任务(如 finalize) 且没找到 ID 时才打印
                if not node_ids and "finalize" not in group_name:
                    # print(f"❓ [Unknown Task] {group_name}")
                    pass

            # --- 统计逻辑 ---
            if node_ids:
                states = task_group.states
                total = sum(states.values())
                finished = states.get('memory', 0) + states.get('released', 0) + states.get('errored', 0)

                if total > 0:
                    for nid in node_ids:
                        if nid not in snapshot: snapshot[nid] = {'total': 0, 'finished': 0}
                        snapshot[nid]['total'] += total
                        snapshot[nid]['finished'] += finished

                        # [调试] 只要有进度就打印，确保逻辑通了
                        # if finished > 0:
                        #    print(f"🚀 [Progress] {nid}: {finished}/{total}")

        return snapshot


# ==========================================
# 下面的代码保持原样 (内存清理与集群启动)
# ==========================================
def trim_memory():
    import gc
    import platform
    import ctypes
    gc.collect()
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
        return _GLOBAL_CLIENT

    logger.info("正在初始化 Dask 本地集群...")

    if torch.cuda.is_available():
        gpu_name = torch.cuda.get_device_name(0)
        logger.info(f"🟢 检测到 GPU: {gpu_name}")
        n_workers = 1
        threads_per_worker = 1
    else:
        import multiprocessing
        cpu_count = multiprocessing.cpu_count()
        n_workers = max(2, min(cpu_count - 1, 8))
        threads_per_worker = 1
        logger.info(f"🔵 CPU 模式: {n_workers} Workers")

    try:
        _GLOBAL_CLUSTER = LocalCluster(
            processes=True,
            n_workers=n_workers,
            threads_per_worker=threads_per_worker,
            dashboard_address=':8787'
        )
        _GLOBAL_CLIENT = Client(_GLOBAL_CLUSTER)

        plugin = BrainFlowProgressPlugin()
        _GLOBAL_CLIENT.register_scheduler_plugin(plugin, name='brainflow_progress')

        if platform.system() == "Linux":
            _GLOBAL_CLIENT.run_on_scheduler(
                lambda dask_scheduler: dask_scheduler.loop.call_later(
                    60, lambda: _GLOBAL_CLIENT.run(trim_memory)
                )
            )

        logger.info(f"Dask Ready: {_GLOBAL_CLIENT.dashboard_link}")
        return _GLOBAL_CLIENT

    except Exception as e:
        logger.error(f"Dask Start Failed: {e}")
        return None


def get_client():
    if _GLOBAL_CLIENT: return _GLOBAL_CLIENT
    try:
        return Client.current()
    except:
        return None


def stop_dask_cluster():
    global _GLOBAL_CLIENT, _GLOBAL_CLUSTER
    if _GLOBAL_CLIENT:
        _GLOBAL_CLIENT.close()
        _GLOBAL_CLIENT = None
    if _GLOBAL_CLUSTER:
        _GLOBAL_CLUSTER.close()
        _GLOBAL_CLUSTER = None