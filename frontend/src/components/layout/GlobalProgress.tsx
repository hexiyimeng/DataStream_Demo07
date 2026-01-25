import { useFlow } from '../../hooks/useFlowContext';
import { useMemo } from 'react';

export default function GlobalProgress() {
  const { nodes, stopFlow, isConnected } = useFlow();

  // 1. 找到当前正在运行的节点
  // [核心改动] 增加了 n.data.progress === -1 的判断
  const activeNode = useMemo(() => {
    return nodes.find(n =>
        n.data.progress !== undefined &&
        ((n.data.progress > 0 && n.data.progress < 100) || n.data.progress === -1)
    );
  }, [nodes]);

  // 2. 只有当连接了后端，且有任务在跑时才显示
  if (!isConnected || !activeNode) return null;

  const progress = activeNode.data.progress || 0;
  // [核心改动] 判断是否为“不确定进度”模式
  const isIndeterminate = progress === -1;

  const label = activeNode.data.nodeSpec?.display_name || activeNode.data.opType;
  const message = activeNode.data.message || "Calculating...";

  return (
    <div className="absolute top-16 left-1/2 -translate-x-1/2 z-50 flex flex-col items-center gap-2 animate-in slide-in-from-top-4 fade-in duration-300">

      {/* 灵动岛主体 */}
      <div className="
        flex items-center gap-3 pl-3 pr-2 py-2
        bg-[var(--node-header)]/90 backdrop-blur-md
        border border-[var(--node-border)]
        rounded-full shadow-2xl
        text-[var(--text-head)]
        min-w-[300px] max-w-[500px]
      ">
        {/* 左侧：Loading Spinner */}
        <div className="relative w-5 h-5 flex items-center justify-center shrink-0">
            {/* [核心改动] 如果是不确定模式，让图标一直转；否则只有普通转动 */}
            <svg className={`w-full h-full text-blue-500 ${isIndeterminate ? 'animate-spin duration-700' : 'animate-spin'}`} viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none"></circle>
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
            </svg>
            {/* 只有在确定的进度下才显示数字 */}
            {!isIndeterminate && (
                <span className="absolute text-[8px] font-bold">{Math.floor(progress)}</span>
            )}
        </div>

        {/* 中间：文本信息 */}
        <div className="flex flex-col flex-1 min-w-0">
            <div className="flex justify-between items-baseline">
                <span className="text-xs font-bold truncate">{label}</span>
                <span className="text-[9px] font-mono opacity-50 ml-2">ID: {activeNode.id.split('_')[1]}</span>
            </div>
            <span className="text-[10px] text-[var(--text-sub)] truncate">{message}</span>

            {/* 细进度条 */}
            <div className="h-1 w-full bg-gray-200 dark:bg-gray-700 rounded-full mt-1 overflow-hidden relative">
                {isIndeterminate ? (
                    // [核心改动] 跑马灯动画 (Shimmer)
                    <div
                        className="absolute top-0 left-0 h-full w-1/2 bg-gradient-to-r from-transparent via-blue-500 to-transparent animate-shimmer opacity-80"
                    />
                ) : (
                    // 普通进度条
                    <div
                        className="h-full bg-blue-500 transition-all duration-300"
                        style={{ width: `${progress}%` }}
                    />
                )}
            </div>
        </div>

        {/* 右侧：停止按钮 */}
        <button
            onClick={stopFlow}
            className="w-7 h-7 flex items-center justify-center rounded-full bg-red-500/10 hover:bg-red-500 text-red-500 hover:text-white transition-all shrink-0"
            title="Stop Execution"
        >
            <svg className="w-3.5 h-3.5" fill="currentColor" viewBox="0 0 24 24"><path d="M6 18L18 6M6 6l12 12" stroke="currentColor" strokeWidth="3" strokeLinecap="round" /></svg>
        </button>
      </div>
    </div>
  );
}