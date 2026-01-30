# backend/dask_monitor.py
import time
import logging

logger = logging.getLogger("BrainFlow.Monitor")


def monitor_dask_progress(client, future, callback=None, global_progress_callback=None):
    """
    通用监控工具：阻塞等待 Future 完成，期间持续高速轮询。
    """

    # 这里的函数跑在 Scheduler 上，用来获取数据
    def get_plugin_stats(dask_scheduler):
        if 'brainflow_progress' in dask_scheduler.plugins:
            return dask_scheduler.plugins['brainflow_progress'].get_progress_snapshot()
        return {}

    logger.info(f"Start monitoring Dask task: {future.key}")

    # 用来记录上一次的进度，防止重复发消息刷屏
    last_progress_cache = {}

    while not future.done():
        try:
            # 1. 远程调用获取快照
            stats = client.run_on_scheduler(get_plugin_stats)

            if stats:
                active_nodes_count = 0

                # 2. 遍历统计
                for nid, data in stats.items():
                    total = data['total']
                    finished = data['finished']

                    if total > 0:
                        active_nodes_count += 1

                        # [优化] 只在进度发生变化时才广播，减少 WebSocket 压力
                        cache_key = f"{nid}_{finished}_{total}"
                        if last_progress_cache.get(nid) != cache_key:

                            # 广播给全局 (更新上游节点进度条)
                            if global_progress_callback:
                                msg = f"Processing: {finished}/{total}"
                                # 如果完成了，改一下消息
                                if finished == total: msg = "Done"

                                global_progress_callback(nid, finished, total, msg)

                            last_progress_cache[nid] = cache_key

                # 3. 更新 Writer 自身 (显示有多少个上游节点在跑)
                if callback and active_nodes_count > 0:
                    callback(-1, 100, f"Syncing {active_nodes_count} active nodes...")

        except Exception as e:
            # 只有开发调试时才打开这个日志，不然太吵
            # logger.warning(f"Monitor polling warning: {e}")
            pass

        # [重要] 缩短轮询间隔到 0.1 秒 (100ms)
        # 对于小任务，0.5s 太慢了
        time.sleep(0.1)

    # 循环结束处理
    if future.status == 'error':
        future.result()  # 抛出异常

    logger.info("Dask task completed.")