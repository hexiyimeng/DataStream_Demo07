import { useMemo, useState, useEffect, useRef } from 'react';
import { useFlow } from '../../hooks/useFlowContext';
import { createPortal } from 'react-dom';
import type { NodeSpec } from '../../types';

// === 类型定义 ===
interface MenuItem {
  label: string;
  value?: string;
  children?: MenuItem[];
}

// === 1. 子菜单列表组件 (递归核心) ===
// 只有列表自己知道哪一行被选中了 (Active)，而不是每一行自己管自己
const MenuList = ({ items, onSelect }: { items: MenuItem[], onSelect: (v: string) => void }) => {
  // 当前这一层级，哪一个 label 是展开的？
  const [activeLabel, setActiveLabel] = useState<string | null>(null);

  return (
    <div className="bg-[#222] border border-[#444] shadow-xl flex flex-col py-1 min-w-[140px]">
      {items.map((item) => {
        const hasChildren = !!item.children && item.children.length > 0;
        const isActive = activeLabel === item.label;

        return (
          <div
            key={item.label}
            className="relative group"
            // === 交互逻辑复刻 ===
            // 1. 鼠标移入：自动展开当前项 (ComfyUI 体验)
            onMouseEnter={() => setActiveLabel(item.label)}
            // 2. 点击：如果是目录，强制展开；如果是文件，直接选中
            onClick={(e) => {
              e.stopPropagation();
              if (hasChildren) {
                setActiveLabel(item.label); // 【关键修复】点击目录现在会强制触发展开
              } else if (item.value) {
                onSelect(item.value);
              }
            }}
          >
            {/* 菜单行 */}
            <div
              className={`
                px-3 py-1 cursor-pointer flex justify-between items-center select-none
                text-[12px] font-sans transition-colors duration-75
                ${isActive ? 'bg-[#235a9f] text-white' : 'text-[#ccc] hover:bg-[#235a9f] hover:text-white'}
              `}
            >
              <span className="truncate mr-4">{item.label}</span>
              {hasChildren && <span className="text-[10px] opacity-80">▶</span>}
            </div>

            {/* 子菜单渲染 (绝对定位) */}
            {hasChildren && isActive && (
              <div
                className="absolute top-0 left-full z-50"
                style={{ marginLeft: '-1px' }} // 物理重叠，防止鼠标断触
              >
                {/* 递归调用 MenuList */}
                <MenuList items={item.children!} onSelect={onSelect} />
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
};

// === 2. 主入口组件 ===
interface ContextMenuProps {
  top: number;
  left: number;
  onClose: () => void;
  onAddNode: (type: string) => void;
}

export default function ContextMenu({ top, left, onClose, onAddNode }: ContextMenuProps) {
  const { nodeDefs } = useFlow();
  const [search, setSearch] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => inputRef.current?.focus(), []);

  // 构建菜单树
  const menuTree = useMemo(() => {
    const root: MenuItem[] = [];
    Object.entries(nodeDefs).forEach(([type, def]: [string, NodeSpec]) => {
      // 默认分类兼容
      const category = def.category || 'Utils';
      const parts = category.split('/');

      let current = root;
      parts.forEach((part: string) => {
        let existing = current.find(i => i.label === part);
        if (!existing) {
          existing = { label: part, children: [] };
          current.push(existing);
        }
        current = existing.children!;
      });
      current.push({ label: def.display_name, value: type });
    });

    // 排序：文件夹优先，然后按字母
    const sortNodes = (nodes: MenuItem[]) => {
      nodes.sort((a, b) => {
        const aDir = !!a.children;
        const bDir = !!b.children;
        if (aDir && !bDir) return -1;
        if (!aDir && bDir) return 1;
        return a.label.localeCompare(b.label);
      });
      nodes.forEach(n => n.children && sortNodes(n.children));
    };
    sortNodes(root);
    return root;
  }, [nodeDefs]);

  // 搜索扁平化
  const flatResults = useMemo(() => {
    if (!search) return null;
    const res: MenuItem[] = [];
    Object.entries(nodeDefs).forEach(([type, def]: [string, NodeSpec]) => {
      if (def.display_name.toLowerCase().includes(search.toLowerCase())) {
        res.push({ label: def.display_name, value: type });
      }
    });
    return res;
  }, [search, nodeDefs]);

  return createPortal(
    <div
      className="fixed inset-0 z-[9999]"
      onContextMenu={(e) => e.preventDefault()}
      onClick={onClose}
    >
      <div
        className="fixed w-48 bg-[#222] border border-[#555] shadow-[0_10px_30px_rgba(0,0,0,0.5)] flex flex-col text-[#ccc]"
        style={{ top, left }}
        onClick={e => e.stopPropagation()}
      >
        {/* Search Header */}
        <div className="p-1 border-b border-[#444] bg-[#222]">
          <input
            ref={inputRef}
            className="w-full bg-[#111] text-[#fff] px-2 py-1 text-[12px] border border-[#333] outline-none placeholder-[#666] focus:border-[#235a9f]"
            placeholder="Search nodes..."
            value={search}
            onChange={e => setSearch(e.target.value)}
            onKeyDown={e => {
              if (e.key === 'Enter' && flatResults && flatResults[0] && flatResults[0].value) {
                 onAddNode(flatResults[0].value);
                 onClose();
              }
            }}
          />
        </div>

        {/* Content Area */}
        <div className="max-h-[500px] overflow-y-visible custom-scrollbar">
          {search ? (
            // Search Mode (Flat)
            <div className="py-1">
                {flatResults?.length ? flatResults.map(item => (
                <div
                    key={item.value}
                    onClick={() => item.value && onAddNode(item.value) && onClose()}
                    className="px-3 py-1 cursor-pointer hover:bg-[#235a9f] hover:text-white text-[12px] truncate"
                >
                    {item.label}
                </div>
                )) : <div className="p-2 text-center italic text-[#666] text-xs">No results</div>}
            </div>
          ) : (
            // Directory Mode (Recursive)
            // 直接渲染第一层 MenuList
            <MenuList items={menuTree} onSelect={(val) => { onAddNode(val); onClose(); }} />
          )}
        </div>
      </div>
    </div>,
    document.body
  );
}