import { useMemo, useState, useEffect, useRef } from 'react';
import { useFlow } from '../../hooks/useFlowContext';
import { createPortal } from 'react-dom';
import type { NodeSpec } from '../../types';

interface MenuItem {
  label: string;
  value?: string;
  children?: MenuItem[];
}

const CATEGORY_COLORS: Record<string, string> = {
  'Image': '#3b82f6', 'Loader': '#22c55e', 'Output': '#a78bfa',
  'Processing': '#f59e0b', 'Transform': '#06b6d4', 'Utils': '#94a3b8',
  'BrainFlow/IO': '#22c55e', 'BrainFlow/Processing': '#3b82f6',
  'BrainFlow/Segmentation': '#f59e0b', 'BrainFlow/Output': '#a78bfa',
};
function getCategoryColor(cat?: string): string {
  if (!cat) return '#94a3b8';
  return CATEGORY_COLORS[cat.split('/')[0]] ?? '#94a3b8';
}

const MenuList = ({ items, onSelect }: { items: MenuItem[], onSelect: (v: string) => void }) => {
  const [active, setActive] = useState<string | null>(null);
  return (
    <div
      className="flex flex-col rounded-[var(--radius-md)] overflow-hidden"
      style={{ backgroundColor: 'var(--color-bg-surface)', border: '1px solid var(--color-border-default)', boxShadow: 'var(--shadow-dropdown)' }}
    >
      {items.map(item => {
        const hasChildren = !!(item.children && item.children.length > 0);
        const isActive = active === item.label;
        return (
          <div
            key={item.label}
            className="relative"
            onMouseEnter={() => hasChildren && setActive(item.label)}
            onMouseLeave={() => setActive(null)}
          >
            <div
              onClick={(e) => {
                e.stopPropagation();
                if (hasChildren) setActive(item.label);
                else if (item.value) onSelect(item.value);
              }}
              className={[
                'px-3 py-1.5 flex items-center justify-between cursor-pointer select-none transition-colors duration-75',
                'text-[12px]',
                isActive ? 'bg-[var(--color-accent)] text-white' : 'text-[var(--color-text-primary)] hover:bg-[var(--color-bg-field-hover)]',
              ].join(' ')}
            >
              <span className="truncate mr-6">{item.label}</span>
              {hasChildren && <span className="text-[10px] opacity-70 shrink-0">▶</span>}
            </div>
            {hasChildren && isActive && (
              <div className="absolute top-0 left-full z-[100]" style={{ marginLeft: '2px' }}>
                <MenuList items={item.children!} onSelect={onSelect} />
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
};

interface ContextMenuProps {
  top: number;
  left: number;
  onClose: () => void;
  onAddNode: (type: string) => void;
}

export default function ContextMenu({ top, left, onClose, onAddNode }: ContextMenuProps) {
  const { nodeDefs } = useFlow();
  const [search, setSearch] = useState('');
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => { inputRef.current?.focus(); }, []);
  useEffect(() => {
    const handler = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose]);

  const adjustedX = Math.min(left, window.innerWidth - 320);
  const adjustedY = Math.min(top, window.innerHeight - 480);

  const menuTree = useMemo(() => {
    const root: MenuItem[] = [];
    Object.entries(nodeDefs).forEach(([type, def]: [string, NodeSpec]) => {
      const cat = def.category || 'Utils';
      const parts = cat.split('/');
      let cur = root;
      parts.forEach(p => {
        let ex = cur.find(i => i.label === p);
        if (!ex) { ex = { label: p, children: [] }; cur.push(ex); }
        cur = ex.children!;
      });
      cur.push({ label: def.display_name, value: type });
    });
    const sort = (ns: MenuItem[]) => {
      ns.sort((a, b) => {
        const af = !!a.children; const bf = !!b.children;
        if (af !== bf) return af ? -1 : 1;
        return a.label.localeCompare(b.label);
      });
      ns.forEach(n => n.children && sort(n.children));
    };
    sort(root);
    return root;
  }, [nodeDefs]);

  const flatResults = useMemo(() => {
    if (!search) return null;
    const q = search.toLowerCase();
    return Object.entries(nodeDefs)
      .filter(([, d]) => d.display_name.toLowerCase().includes(q))
      .map(([type, d]) => ({ label: d.display_name, value: type, category: d.category, output: d.output }));
  }, [search, nodeDefs]);

  return createPortal(
    <div className="fixed inset-0 z-[9999]" onContextMenu={e => e.preventDefault()} onClick={onClose}>
      <div
        className="fixed flex flex-col rounded-[var(--radius-lg)] overflow-hidden"
        style={{
          top: adjustedY, left: adjustedX, width: 300,
          backgroundColor: 'var(--color-bg-surface)',
          border: '1px solid var(--color-border-default)',
          boxShadow: 'var(--shadow-floating)',
        }}
        onClick={e => e.stopPropagation()}
      >
        {/* Search header */}
        <div className="px-3 py-2.5 border-b" style={{ backgroundColor: 'var(--color-bg-surface-2)', borderColor: 'var(--color-border-subtle)' }}>
          <div className="relative flex items-center">
            <svg className="absolute left-2.5 w-3.5 h-3.5 pointer-events-none" style={{ color: 'var(--color-text-muted)' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
            <input
              ref={inputRef}
              className="w-full h-8 rounded-[var(--radius-md)] pl-8 pr-3 text-[12px] outline-none transition-colors"
              style={{
                backgroundColor: 'var(--color-bg-field)',
                border: '1px solid var(--color-border-default)',
                color: 'var(--color-text-primary)',
              }}
              placeholder="Search nodes..."
              value={search}
              onChange={e => setSearch(e.target.value)}
              onKeyDown={e => {
                if (e.key === 'Enter' && flatResults && flatResults[0]?.value) {
                  onAddNode(flatResults[0].value); onClose();
                }
              }}
              onFocus={e => (e.currentTarget.style.borderColor = 'var(--color-border-focus)')}
              onBlur={e => (e.currentTarget.style.borderColor = 'var(--color-border-default)')}
            />
          </div>
        </div>

        {/* Content */}
        <div className="py-1 max-h-80 overflow-y-auto custom-scrollbar">
          {search ? (
            <div>
              {flatResults && flatResults.length > 0 ? (
                <>
                  <div className="px-3 py-1 text-[9px] font-bold uppercase tracking-wider" style={{ color: 'var(--color-text-muted)' }}>
                    {flatResults.length} result{flatResults.length !== 1 ? 's' : ''}
                  </div>
                  {flatResults.map(item => {
                    const color = getCategoryColor(item.category);
                    return (
                      <div
                        key={item.value}
                        onClick={() => { if (item.value) { onAddNode(item.value); onClose(); } }}
                        className="group flex items-center gap-2.5 px-3 py-2 cursor-pointer transition-colors"
                        style={{ color: 'var(--color-text-primary)' }}
                        onMouseEnter={e => (e.currentTarget.style.backgroundColor = 'var(--color-bg-field-hover)')}
                        onMouseLeave={e => (e.currentTarget.style.backgroundColor = 'transparent')}
                      >
                        <div className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: color }} />
                        <div className="flex-1 min-w-0">
                          <div className="text-[12px] truncate">{item.label}</div>
                          <div className="text-[10px]" style={{ color: 'var(--color-text-muted)' }}>{item.category}</div>
                        </div>
                        {item.output && item.output.length > 0 && (
                          <span className="text-[9px] px-1 py-px rounded shrink-0" style={{ backgroundColor: 'var(--color-bg-field)', color: 'var(--color-text-muted)' }}>
                            {item.output[0]}
                          </span>
                        )}
                      </div>
                    );
                  })}
                </>
              ) : (
                <div className="px-4 py-6 text-center text-[11px] italic" style={{ color: 'var(--color-text-muted)' }}>
                  No nodes match "{search}"
                </div>
              )}
            </div>
          ) : (
            <MenuList
              items={menuTree}
              onSelect={(val) => { onAddNode(val); onClose(); }}
            />
          )}
        </div>

        {/* Footer hint */}
        <div className="px-3 py-1.5 border-t text-[9px]" style={{ backgroundColor: 'var(--color-bg-surface-2)', borderColor: 'var(--color-border-subtle)', color: 'var(--color-text-muted)' }}>
          <span>↵ Add node</span>
          <span className="mx-2">·</span>
          <span>Esc close</span>
          <span className="mx-2">·</span>
          <span>▶ Submenu</span>
        </div>
      </div>
    </div>,
    document.body
  );
}
