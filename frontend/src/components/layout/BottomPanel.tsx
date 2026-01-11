// src/components/layout/BottomPanel.tsx
import { useEffect, useRef, useState, useCallback } from 'react';
import { useFlow } from '../../hooks/useFlowContext';

export default function BottomPanel() {
  const { logs, clearLogs, isConsoleOpen, toggleConsole } = useFlow();
  const endRef = useRef<HTMLDivElement>(null);

  // 高度状态 + 拖拽逻辑
  const [height, setHeight] = useState(192); // 默认高度 (h-48 = 192px)
  const isDragging = useRef(false);

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [logs, isConsoleOpen]);

  // 拖拽逻辑
  const startResizing = useCallback((e: React.MouseEvent) => {
    e.preventDefault(); // 防止选中文字
    isDragging.current = true;
    document.body.style.cursor = 'row-resize';
    document.body.style.userSelect = 'none';
  }, []);

  const stopResizing = useCallback(() => {
    isDragging.current = false;
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
  }, []);

  const resize = useCallback((e: MouseEvent) => {
    if (isDragging.current) {
      // 计算新高度：窗口总高度 - 鼠标Y坐标
      const newHeight = window.innerHeight - e.clientY;
      // 限制：最小 32px (Header高度)，最大 80% 屏幕
      setHeight(Math.max(32, Math.min(newHeight, window.innerHeight * 0.8)));
    }
  }, []);

  useEffect(() => {
    window.addEventListener('mousemove', resize);
    window.addEventListener('mouseup', stopResizing);
    return () => {
      window.removeEventListener('mousemove', resize);
      window.removeEventListener('mouseup', stopResizing);
    };
  }, [resize, stopResizing]);


  const formatTime = (ts: string | number) => {
    if (!ts) return "--:--:--";
    const date = new Date(ts);
    if (isNaN(date.getTime())) return "--:--:--";
    return date.toLocaleTimeString([], { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" });
  };

  return (
    <div
      className={`
        absolute bottom-0 left-0 w-full z-40 flex flex-col transition-none ease-in-out
        border-t border-[var(--console-border)] 
        bg-[var(--console-bg)] text-[var(--console-text)]
        shadow-[0_-4px_20px_rgba(0,0,0,0.1)]
      `}
      // 动态样式：如果打开就用 state 高度，否则固定 32px (h-8)
      style={{ height: isConsoleOpen ? height : 32 }}
    >
      {/* 拖拽手柄 (仅在打开时可用) */}
      {isConsoleOpen && (
        <div
            onMouseDown={startResizing}
            className="absolute top-0 left-0 w-full h-1 cursor-row-resize hover:bg-blue-500/50 z-50 -mt-0.5"
        />
      )}

      {/* Toolbar */}
      <div
        className="h-8 border-b border-[var(--console-border)] flex items-center justify-between px-4 select-none cursor-pointer hover:bg-[var(--widget-bg)] transition-colors shrink-0"
        onClick={toggleConsole}
      >
        <div className="flex items-center gap-3">
          <div className={`transition-transform duration-200 opacity-60 ${isConsoleOpen ? 'rotate-0' : '-rotate-90'}`}>
             ▼
          </div>
          <span className="text-[11px] font-bold uppercase tracking-wider font-mono opacity-80">
            Console Terminal
          </span>
          {logs.length > 0 && (
             <span className="text-[10px] px-1.5 py-0.5 rounded font-mono border border-[var(--console-border)] bg-[var(--widget-bg)] opacity-70">
               {logs.length} events
             </span>
          )}
        </div>
        <button
          onClick={(e) => { e.stopPropagation(); clearLogs(); }}
          className="text-[10px] px-2 py-1 rounded hover:bg-[var(--widget-bg)] opacity-60 hover:opacity-100 transition-all uppercase font-semibold border border-transparent hover:border-[var(--console-border)]"
        >
          Clear
        </button>
      </div>

      {/* Log Content */}
      {isConsoleOpen && (
        <div className="flex-1 overflow-y-auto p-2 font-mono text-[11px] leading-relaxed custom-scrollbar select-text bg-[var(--console-bg)]">
          {logs.length === 0 ? (
            <div className="opacity-40 p-2 italic">{">"} System ready... Waiting for workflow execution.</div>
          ) : (
            logs.map((log) => (
              <div key={log.id} className="flex gap-3 py-0.5 hover:bg-[var(--widget-bg)] px-2 rounded-sm border-l-2 border-transparent hover:border-blue-500/30 transition-colors">
                <span className="opacity-40 w-16 shrink-0 select-none text-right">
                  {formatTime(log.timestamp)}
                </span>
                <span className={`flex-1 break-all tracking-tight
                  ${log.type === 'error' ? 'text-red-500 font-bold' : ''}
                  ${log.type === 'success' ? 'text-green-600 dark:text-green-400 font-bold' : ''}
                  ${log.type === 'warning' ? 'text-amber-500' : ''}
                  ${log.type === 'info' ? 'text-[var(--console-text)]' : ''}
                `}>
                  <span className="opacity-50 mr-2 select-none">
                    {log.type === 'error' && '❌'}
                    {log.type === 'success' && '✅'}
                    {log.type === 'warning' && '⚠️'}
                    {log.type === 'info' && '>'}
                  </span>
                  {log.message}
                </span>
              </div>
            ))
          )}
          <div ref={endRef} />
        </div>
      )}
    </div>
  );
}