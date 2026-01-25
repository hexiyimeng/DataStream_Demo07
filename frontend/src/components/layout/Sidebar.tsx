// src/components/layout/Sidebar.tsx
import { useState, useCallback, useRef, useEffect, useMemo } from 'react';
import { useFlow } from '../../hooks/useFlowContext';

// === 图标组件 ===
const NODE_ICON = (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19.428 15.428a2 2 0 00-1.022-.547l-2.384-.477a6 6 0 00-3.86.517l-.318.158a6 6 0 01-3.86.517L6.05 15.21a2 2 0 00-1.806.547M8 4h8l-1 1v5.172a2 2 0 00.586 1.414l5 5c1.26 1.26.367 3.414-1.415 3.414H4.828c-1.782 0-2.674-2.154-1.414-3.414l5-5A2 2 0 009 10.172V5L8 4z" />
  </svg>
);

const FOLDER_ICON = (
  <svg className="w-3.5 h-3.5 text-blue-400 mr-1.5" fill="currentColor" viewBox="0 0 20 20">
    <path d="M2 6a2 2 0 012-2h5l2 2h5a2 2 0 012 2v6a2 2 0 01-2 2H4a2 2 0 01-2-2V6z" />
  </svg>
);

const FOLDER_OPEN_ICON = (
  <svg className="w-3.5 h-3.5 text-blue-500 mr-1.5" fill="currentColor" viewBox="0 0 20 20">
    <path fillRule="evenodd" d="M2 6a2 2 0 012-2h4l2 2h6a2 2 0 012 2v8a2 2 0 01-2 2H4a2 2 0 01-2-2V6z" clipRule="evenodd" />
  </svg>
);

const CHEVRON_RIGHT = <svg className="w-3 h-3 text-[var(--text-sub)]" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" /></svg>;
const CHEVRON_DOWN = <svg className="w-3 h-3 text-[var(--text-sub)]" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" /></svg>;

// === 类型定义 ===
interface TreeNode {
  label: string;
  value?: string; // 如果是节点，这里存 type
  children?: TreeNode[]; // 如果是文件夹，这里存子项
}

// === 1. 递归文件夹组件 ===
const SidebarCategory = ({
  item,
  onDragStart,
  onAddNode, // <--- 1. 新增接收这个参数
  level = 0
}: {
  item: TreeNode;
  onDragStart: (e: React.DragEvent, type: string) => void;
  onAddNode: (type: string) => void; // <--- 类型定义
  level?: number
}) => {
  const [isOpen, setIsOpen] = useState(false);
  const isNode = !item.children;

  // 如果是节点（叶子）
  if (isNode) {
    return (
      <div
        draggable
        onDragStart={(e) => item.value && onDragStart(e, item.value)}
        // 2. 新增点击事件
        onClick={() => item.value && onAddNode(item.value)}
        className="
          flex items-center group cursor-grab active:cursor-grabbing
          px-2 py-1.5 ml-3 mr-1 rounded-sm
          hover:bg-[var(--widget-hover)] border border-transparent hover:border-[var(--node-border)]
          transition-all
          hover:text-blue-500 hover:font-medium {/* 增加一点点击反馈样式 */}
        "
        style={{ paddingLeft: `${level * 12 + 12}px` }}
      >
        <div className="w-1.5 h-1.5 rounded-full bg-blue-500/50 mr-2 group-hover:bg-blue-500 transition-colors" />
        <span className="text-[11px] text-[var(--text-head)] truncate select-none">{item.label}</span>
      </div>
    );
  }

  // 如果是文件夹
  return (
    <div className="select-none">
      <div
        onClick={() => setIsOpen(!isOpen)}
        className="
          flex items-center cursor-pointer px-2 py-1.5 rounded-sm
          hover:bg-[var(--node-header)] transition-colors
          text-[var(--text-label)] hover:text-[var(--text-head)]
        "
        style={{ paddingLeft: `${level * 12 + 4}px` }}
      >
        <div className="mr-1 opacity-70">
          {isOpen ? CHEVRON_DOWN : CHEVRON_RIGHT}
        </div>
        {isOpen ? FOLDER_OPEN_ICON : FOLDER_ICON}
        <span className="text-[11px] font-semibold tracking-wide truncate">{item.label}</span>
      </div>

      {isOpen && item.children && (
        <div className="border-l border-[var(--node-border)] ml-[11px]">
            {item.children.map((child, idx) => (
                <SidebarCategory
                  key={idx}
                  item={child}
                  onDragStart={onDragStart}
                  onAddNode={onAddNode} // <--- 3. 递归时别忘了继续传下去！
                  level={level + 1}
                />
            ))}
        </div>
      )}
    </div>
  );
};
// === 主组件 ===
export default function Sidebar() {
  const { addNode, nodeDefs } = useFlow();
  const [activeTab, setActiveTab] = useState<'nodes' | null>('nodes');
  const [searchText, setSearchText] = useState("");

  // 宽度拖拽
  const [width, setWidth] = useState(240);
  const isDragging = useRef(false);

  // === 构建树形结构 (Memo以优化性能) ===
  const treeData = useMemo(() => {
    const root: TreeNode[] = [];

    // 遍历所有节点定义
    Object.entries(nodeDefs).forEach(([type, def]) => {
      const category = def.category || 'Utils';
      const parts = category.split('/'); // 例如 ["Image", "Loaders"]

      let currentLevel = root;

      // 遍历路径，创建文件夹
      parts.forEach((part) => {
        let existing = currentLevel.find(i => i.label === part && i.children);
        if (!existing) {
          existing = { label: part, children: [] };
          currentLevel.push(existing);
        }
        currentLevel = existing.children!;
      });

      // 添加具体的节点
      currentLevel.push({ label: def.display_name, value: type });
    });

    // 排序：文件夹优先，然后按字母排序
    const sortNodes = (nodes: TreeNode[]) => {
      nodes.sort((a, b) => {
        const aIsFolder = !!a.children;
        const bIsFolder = !!b.children;
        if (aIsFolder && !bIsFolder) return -1;
        if (!aIsFolder && bIsFolder) return 1;
        return a.label.localeCompare(b.label);
      });
      nodes.forEach(n => n.children && sortNodes(n.children));
    };

    sortNodes(root);
    return root;
  }, [nodeDefs]);

  // === 搜索过滤结果 (扁平化) ===
  const filteredFlatList = useMemo(() => {
    if (!searchText) return [];
    return Object.entries(nodeDefs)
      .filter(([, def]) => def.display_name.toLowerCase().includes(searchText.toLowerCase()))
      .map(([nodeType, def]) => ({ ...def, type: nodeType }));
  }, [searchText, nodeDefs]);

  // === 拖拽逻辑 ===
  const onDragStart = (event: React.DragEvent, nodeType: string) => {
    event.dataTransfer.setData('application/reactflow', nodeType);
    event.dataTransfer.effectAllowed = 'move';
  };

  const startResizing = useCallback(() => {
    isDragging.current = true;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
  }, []);

  const stopResizing = useCallback(() => {
    isDragging.current = false;
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
  }, []);

  const resize = useCallback((e: MouseEvent) => {
    if (isDragging.current) {
      setWidth(Math.max(180, Math.min(e.clientX - 48, 600)));
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

  return (
    <div className="flex h-full z-20 pointer-events-none relative">
      {/* 固定侧边栏 */}
      <aside className="w-12 h-full bg-[var(--node-header)] border-r border-[var(--node-border)] flex flex-col items-center py-4 gap-4 select-none shadow-xl pointer-events-auto shrink-0 z-30">
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

      {/* 扩展抽屉 */}
      {activeTab === 'nodes' && (
        <div
            className="h-full bg-[var(--node-body)] border-r border-[var(--node-border)] flex flex-col shadow-2xl pointer-events-auto relative animate-in slide-in-from-left-2 duration-200"
            style={{ width: width }}
        >
          {/* 搜索框 */}
          <div className="p-3 border-b border-[var(--node-border)] bg-[var(--node-header)]">
             <div className="relative">
                <input
                    type="text"
                    placeholder="Search nodes..."
                    value={searchText}
                    onChange={e => setSearchText(e.target.value)}
                    className="w-full bg-[var(--widget-bg)] border border-[var(--node-border)] rounded px-2 pl-7 py-1.5 text-xs text-[var(--text-val)] focus:border-blue-500 outline-none transition-colors placeholder-[var(--text-sub)]"
                />
                <svg className="w-3 h-3 text-[var(--text-sub)] absolute left-2 top-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" /></svg>
                {searchText && (
                    <button onClick={() => setSearchText("")} className="absolute right-2 top-2 text-[var(--text-sub)] hover:text-[var(--text-head)]">
                        <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" /></svg>
                    </button>
                )}
             </div>
          </div>

          {/* 列表内容 */}
          <div className="flex-1 overflow-y-auto custom-scrollbar p-2">

            {/* 模式 A: 搜索时显示扁平列表 (更直观) */}
            {searchText ? (
               <div className="flex flex-col gap-1">
                  <div className="text-[10px] text-[var(--text-sub)] mb-2 uppercase font-bold px-1">
                    Search Results ({filteredFlatList.length})
                  </div>
                  {filteredFlatList.map((item) => (
                    <button
                      key={item.type}
                      draggable
                      onDragStart={(event) => onDragStart(event, item.type)}
                      onClick={() => addNode(item.type)}
                      className="text-left px-3 py-2 rounded hover:bg-[var(--node-header)] text-[11px] text-[var(--text-head)] transition-colors border border-transparent hover:border-[var(--node-border)] truncate flex items-center"
                    >
                      <div className="w-1.5 h-1.5 rounded-full bg-blue-500 mr-2 shrink-0" />
                      {item.display_name}
                      <span className="ml-auto text-[9px] text-[var(--text-sub)] opacity-50">{item.category?.split('/').pop()}</span>
                    </button>
                  ))}
                  {filteredFlatList.length === 0 && (
                      <div className="text-center text-[var(--text-sub)] text-xs mt-4">No nodes found</div>
                  )}
               </div>
            ) : (
            // 模式 B: 无搜索时显示树形结构
               <div className="flex flex-col gap-0.5 pb-10">
                  {treeData.map((item, idx) => (
                     <SidebarCategory
                        key={idx}
                        item={item}
                        onDragStart={onDragStart}
                        onAddNode={addNode}
                     />
                  ))}
               </div>
            )}
          </div>

          {/* 拖拽手柄 */}
          <div
            onMouseDown={startResizing}
            className="absolute top-0 right-0 w-1 h-full cursor-col-resize hover:bg-blue-500 transition-colors z-50 group"
          >
             <div className="w-[1px] h-full bg-[var(--node-border)] group-hover:bg-blue-500 transition-colors mx-auto" />
          </div>
        </div>
      )}
    </div>
  );
}