// src/components/layout/Sidebar.tsx
import { useState, useCallback, useRef, useEffect } from 'react';
import { useFlow } from '../../hooks/useFlowContext';

const NODE_ICON = (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19.428 15.428a2 2 0 00-1.022-.547l-2.384-.477a6 6 0 00-3.86.517l-.318.158a6 6 0 01-3.86.517L6.05 15.21a2 2 0 00-1.806.547M8 4h8l-1 1v5.172a2 2 0 00.586 1.414l5 5c1.26 1.26.367 3.414-1.415 3.414H4.828c-1.782 0-2.674-2.154-1.414-3.414l5-5A2 2 0 009 10.172V5L8 4z" />
  </svg>
);

export default function Sidebar() {
  const { addNode, nodeDefs } = useFlow();
  const [activeTab, setActiveTab] = useState<'nodes' | null>('nodes');

  // 🔥 新增：宽度状态 + 拖拽逻辑
  const [width, setWidth] = useState(240);
  const isDragging = useRef(false);

  // 开始拖拽
  const startResizing = useCallback(() => {
    isDragging.current = true;
    document.body.style.cursor = 'col-resize'; // 强制全局鼠标样式
    document.body.style.userSelect = 'none';   // 防止拖拽时选中文字
  }, []);

  // 结束拖拽
  const stopResizing = useCallback(() => {
    isDragging.current = false;
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
  }, []);

  // 拖拽中
  const resize = useCallback((e: MouseEvent) => {
    if (isDragging.current) {
      // 限制最小 150px，最大 600px
      const newWidth = Math.max(150, Math.min(e.clientX - 48, 600));
      setWidth(newWidth);
    }
  }, []);

  // 绑定全局事件
  useEffect(() => {
    window.addEventListener('mousemove', resize);
    window.addEventListener('mouseup', stopResizing);
    return () => {
      window.removeEventListener('mousemove', resize);
      window.removeEventListener('mouseup', stopResizing);
    };
  }, [resize, stopResizing]);

  return (
    <div className="flex h-full z-20 pointer-events-none relative">
      {/* 极窄工具栏 (固定宽度) */}
      <aside className="w-12 h-full bg-[var(--node-header)] border-r border-[var(--node-border)] flex flex-col items-center py-4 gap-4 select-none shadow-xl pointer-events-auto shrink-0">
        <div
          onClick={() => setActiveTab(activeTab === 'nodes' ? null : 'nodes')}
          className={`
            flex flex-col items-center justify-center gap-1 cursor-pointer transition-all w-8 h-8 rounded-md
            ${activeTab === 'nodes' 
              ? 'bg-blue-600 text-white shadow-md' 
              : 'text-[var(--text-sub)] hover:bg-[var(--node-body)] hover:text-[var(--text-head)]'}
          `}
          title="Node Library"
        >
          {NODE_ICON}
        </div>
      </aside>

      {/* 扩展抽屉 (可变宽度) */}
      {activeTab === 'nodes' && (
        <div
            className="h-full bg-[var(--node-body)] border-r border-[var(--node-border)] flex flex-col shadow-2xl pointer-events-auto relative"
            style={{ width: width }} // 🔥 动态宽度
        >
          <div className="p-3 border-b border-[var(--node-border)]">
             <input
                type="text"
                placeholder="Search nodes..."
                className="w-full bg-[var(--widget-bg)] border border-[var(--node-border)] rounded px-2 py-1.5 text-xs text-[var(--text-val)] focus:border-blue-500 outline-none transition-colors"
             />
          </div>
          <div className="flex-1 overflow-y-auto custom-scrollbar p-2">
            <div className="flex flex-col gap-1">
              {Object.keys(nodeDefs).map(name => (
                <button
                  key={name}
                  onClick={() => addNode(name)}
                  className="text-left px-3 py-2 rounded hover:bg-[var(--node-header)] text-[11px] text-[var(--text-head)] transition-colors border border-transparent hover:border-[var(--node-border)] truncate"
                >
                  {nodeDefs[name].display_name}
                </button>
              ))}
            </div>
          </div>

          {/* 🔥 拖拽手柄 */}
          <div
            onMouseDown={startResizing}
            className="absolute top-0 right-0 w-1.5 h-full cursor-col-resize hover:bg-blue-500/50 transition-colors z-50"
            style={{ marginRight: '-1px' }} // 略微突出一点便于抓取
          />
        </div>
      )}
    </div>
  );
}