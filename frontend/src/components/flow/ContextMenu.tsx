// src/components/flow/ContextMenu.tsx
import { useMemo, useState, useEffect, useRef } from 'react';
import { useFlow } from '../../hooks/useFlowContext';
import { createPortal } from 'react-dom';
import type { NodeSpec } from '../../types';

interface MenuItem {
  label: string;
  value?: string;
  children?: MenuItem[];
}

// 1. 子菜单组件
const MenuList = ({ items, onSelect }: { items: MenuItem[], onSelect: (v: string) => void }) => {
  const [activeLabel, setActiveLabel] = useState<string | null>(null);

  return (
    //  主容器颜色变量化
    <div className="bg-[var(--node-body)] border border-[var(--node-border)] shadow-xl flex flex-col py-1 min-w-[140px] rounded-sm">
      {items.map((item) => {
        const hasChildren = !!item.children && item.children.length > 0;
        const isActive = activeLabel === item.label;

        return (
          <div
            key={item.label}
            className="relative group"
            onMouseEnter={() => setActiveLabel(item.label)}
            onClick={(e) => {
              e.stopPropagation();
              if (hasChildren) {
                setActiveLabel(item.label);
              } else if (item.value) {
                onSelect(item.value);
              }
            }}
          >
            <div
              className={`
                px-3 py-1.5 cursor-pointer flex justify-between items-center select-none
                text-[12px] font-sans transition-colors duration-75
                ${isActive 
                  ? 'bg-blue-600 text-white'  // 选中态保持蓝色高亮
                  : 'text-[var(--text-head)] hover:bg-blue-600 hover:text-white'} // 默认态使用主题字色
              `}
            >
              <span className="truncate mr-4">{item.label}</span>
              {hasChildren && <span className="text-[10px] opacity-80">▶</span>}
            </div>

            {hasChildren && isActive && (
              <div
                className="absolute top-0 left-full z-50"
                style={{ marginLeft: '-2px' }} // 微调重叠
              >
                <MenuList items={item.children!} onSelect={onSelect} />
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
};

// 2. 主组件
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

  // 简单的边界检测，防止菜单超出屏幕右下角
  const adjustedX = left + 200 > window.innerWidth ? left - 200 : left;
  const adjustedY = top + 400 > window.innerHeight ? top - 300 : top;

  const menuTree = useMemo(() => {
    const root: MenuItem[] = [];
    Object.entries(nodeDefs).forEach(([type, def]: [string, NodeSpec]) => {
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

    // 排序逻辑
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
        // 这样子菜单 (absolute left-full) 才能显示在父容器外面
        className="fixed w-48 bg-[var(--node-body)] border border-[var(--node-border)] shadow-2xl flex flex-col text-[var(--text-head)] rounded-md"
        style={{ top: adjustedY, left: adjustedX }}
        onClick={e => e.stopPropagation()}
      >
        {/* Search Header */}
        <div className="p-1 border-b border-[var(--node-border)] bg-[var(--node-header)] rounded-t-md">
          <input
            ref={inputRef}
            className="w-full bg-[var(--widget-bg)] text-[var(--text-val)] px-2 py-1 text-[12px] border border-[var(--node-border)] rounded-sm outline-none placeholder-[var(--text-sub)] focus:border-blue-500"
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
        <div className="py-1">
          {search ? (
            <div className="py-1 max-h-[400px] overflow-y-auto custom-scrollbar">
                {/* 搜索结果是平铺的，这里可以加 overflow，因为没有向右弹出的子菜单 */}
                {flatResults?.length ? flatResults.map(item => (
                <div
                    key={item.value}
                    onClick={() => item.value && onAddNode(item.value) && onClose()}
                    className="px-3 py-1.5 cursor-pointer hover:bg-blue-600 hover:text-white text-[12px] truncate text-[var(--text-head)]"
                >
                    {item.label}
                </div>
                )) : <div className="p-2 text-center italic text-[var(--text-sub)] text-xs">No results</div>}
            </div>
          ) : (
            // 目录模式：千万不能加 overflow-hidden，否则子菜单会被切掉
            <MenuList items={menuTree} onSelect={(val) => { onAddNode(val); onClose(); }} />
          )}
        </div>
      </div>
    </div>,
    document.body
  );
}