import { useState, useCallback, useRef, useEffect, useMemo } from 'react';
import { useFlow } from '../../hooks/useFlowContext';
import { TextField } from '../ui/TextField';

const CATEGORY_COLORS: Record<string, string> = {
  'Image': '#3b82f6',
  'Loader': '#22c55e',
  'Output': '#a78bfa',
  'Processing': '#f59e0b',
  'Transform': '#06b6d4',
  'Utils': '#94a3b8',
  'BrainFlow/IO': '#22c55e',
  'BrainFlow/Processing': '#3b82f6',
  'BrainFlow/Segmentation': '#f59e0b',
  'BrainFlow/Output': '#a78bfa',
};

function getCategoryColor(category?: string): string {
  if (!category) return '#94a3b8';
  const top = category.split('/')[0];
  return CATEGORY_COLORS[top] ?? '#94a3b8';
}


interface TreeNode {
  label: string;
  value?: string;
  children?: TreeNode[];
}

const CategoryGroup = ({
  item,
  onDragStart,
  onAddNode,
  level = 0,
}: {
  item: TreeNode;
  onDragStart: (e: React.DragEvent, type: string) => void;
  onAddNode: (type: string) => void;
  level?: number;
}) => {
  const [open, setOpen] = useState(false);
  const isNode = !item.children;
  const color = getCategoryColor(item.label);

  if (isNode) {
    return (
      <div
        draggable
        onDragStart={(e) => item.value && onDragStart(e, item.value)}
        onClick={() => item.value && onAddNode(item.value)}
        className="group flex items-center gap-2 px-2 py-1.5 mx-1 rounded-[var(--radius-sm)] cursor-grab active:cursor-grabbing transition-all duration-100"
        style={{
          paddingLeft: `${level * 12 + 10}px`,
          backgroundColor: 'transparent',
        }}
        onMouseEnter={e => (e.currentTarget.style.backgroundColor = 'var(--color-bg-field-hover)')}
        onMouseLeave={e => (e.currentTarget.style.backgroundColor = 'transparent')}
      >
        <div className="w-1.5 h-1.5 rounded-full shrink-0" style={{ backgroundColor: color, opacity: 0.7 }} />
        <span className="text-[11px] truncate select-none" style={{ color: 'var(--color-text-secondary)' }}>
          {item.label}
        </span>
        <svg className="w-3 h-3 ml-auto shrink-0 opacity-0 group-hover:opacity-60 transition-opacity" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M12 4v16m8-8H4" />
        </svg>
      </div>
    );
  }

  return (
    <div>
      <div
        onClick={() => setOpen(!open)}
        className="flex items-center gap-1.5 px-2 py-1.5 mx-1 rounded-[var(--radius-sm)] cursor-pointer transition-all duration-100"
        style={{ paddingLeft: `${level * 12 + 6}px`, color: 'var(--color-text-secondary)' }}
        onMouseEnter={e => (e.currentTarget.style.backgroundColor = 'var(--color-bg-field-hover)')}
        onMouseLeave={e => (e.currentTarget.style.backgroundColor = 'transparent')}
      >
        <svg className="w-3 h-3 shrink-0 transition-transform duration-100" style={{ transform: open ? 'rotate(90deg)' : 'rotate(0deg)' }} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
        </svg>
        <div className="w-1.5 h-1.5 rounded-full shrink-0" style={{ backgroundColor: color, opacity: 0.6 }} />
        <span className="text-[11px] font-semibold tracking-wide truncate">{item.label}</span>
      </div>
      {open && item.children && (
        <div className="ml-2 border-l" style={{ borderColor: 'var(--color-border-subtle)' }}>
          {item.children.map((child, i) => (
            <CategoryGroup
              key={i}
              item={child}
              onDragStart={onDragStart}
              onAddNode={onAddNode}
              level={level + 1}
            />
          ))}
        </div>
      )}
    </div>
  );
};

export default function Sidebar() {
  const { addNode, nodeDefs } = useFlow();
  const [activeTab, setActiveTab] = useState<'nodes' | null>('nodes');
  const [search, setSearch] = useState('');
  const [width, setWidth] = useState(260);
  const isDragging = useRef(false);

  const treeData = useMemo(() => {
    const root: TreeNode[] = [];
    Object.entries(nodeDefs).forEach(([type, def]) => {
      const cat = def.category || 'Utils';
      const parts = cat.split('/');
      let cur = root;
      parts.forEach(p => {
        let ex = cur.find(i => i.label === p && i.children);
        if (!ex) { ex = { label: p, children: [] }; cur.push(ex); }
        cur = ex.children!;
      });
      cur.push({ label: def.display_name, value: type });
    });
    const sort = (ns: TreeNode[]) => {
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
    if (!search) return [];
    const q = search.toLowerCase();
    return Object.entries(nodeDefs)
      .filter(([, d]) => d.display_name.toLowerCase().includes(q))
      .map(([type, d]) => ({ ...d, type }));
  }, [search, nodeDefs]);

  const totalNodes = Object.keys(nodeDefs).length;

  const onDragStart = (e: React.DragEvent, nodeType: string) => {
    e.dataTransfer.setData('application/reactflow', nodeType);
    e.dataTransfer.effectAllowed = 'move';
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
    if (isDragging.current) setWidth(Math.max(200, Math.min(e.clientX - 48, 480)));
  }, []);
  useEffect(() => {
    window.addEventListener('mousemove', resize);
    window.addEventListener('mouseup', stopResizing);
    return () => { window.removeEventListener('mousemove', resize); window.removeEventListener('mouseup', stopResizing); };
  }, [resize, stopResizing]);

  return (
    <div className="flex h-full z-20 pointer-events-none relative">
      {/* Tab strip */}
      <aside
        className="w-12 h-full flex flex-col items-center py-3 gap-2 select-none shrink-0 z-30"
        style={{ backgroundColor: 'var(--color-bg-surface)', borderRight: '1px solid var(--color-border-subtle)', boxShadow: 'var(--shadow-panel)' }}
      >
        <button
          onClick={() => setActiveTab(activeTab === 'nodes' ? null : 'nodes')}
          className={[
            'flex flex-col items-center justify-center gap-0.5 cursor-pointer transition-all w-9 h-9 rounded-[var(--radius-md)]',
          ].join(' ')}
          style={activeTab === 'nodes'
            ? { backgroundColor: 'var(--color-accent)', color: 'white', boxShadow: 'var(--shadow-node)' }
            : { color: 'var(--color-text-muted)' }}
          title="Node Library"
        >
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M19.428 15.428a2 2 0 00-1.022-.547l-2.384-.477a6 6 0 00-3.86.517l-.318.158a6 6 0 01-3.86.517L6.05 15.21a2 2 0 00-1.806.547M8 4h8l-1 1v5.172a2 2 0 00.586 1.414l5 5c1.26 1.26.367 3.414-1.415 3.414H4.828c-1.782 0-2.674-2.154-1.414-3.414l5-5A2 2 0 009 10.172V5L8 4z" />
          </svg>
        </button>
      </aside>

      {/* Drawer */}
      {activeTab === 'nodes' && (
        <div
          className="h-full flex flex-col pointer-events-auto relative"
          style={{ width, backgroundColor: 'var(--color-bg-surface)', borderRight: '1px solid var(--color-border-subtle)', boxShadow: 'var(--shadow-panel)' }}
        >
          {/* Header */}
          <div className="px-3 pt-3 pb-2 border-b" style={{ borderColor: 'var(--color-border-subtle)' }}>
            <div className="flex items-center justify-between mb-2">
              <div>
                <div className="text-[12px] font-semibold" style={{ color: 'var(--color-text-primary)' }}>Node Library</div>
                <div className="text-[10px]" style={{ color: 'var(--color-text-muted)' }}>{totalNodes} operators</div>
              </div>
            </div>
            <TextField
              placeholder="Search nodes..."
              value={search}
              onChange={e => setSearch(e.target.value)}
              onClear={() => setSearch('')}
              size="sm"
              icon={
                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                </svg>
              }
            />
          </div>

          {/* Node list */}
          <div className="flex-1 overflow-y-auto custom-scrollbar py-1">
            {search ? (
              <div>
                <div className="text-[10px] font-bold uppercase tracking-wider px-3 py-1.5" style={{ color: 'var(--color-text-muted)' }}>
                  {flatResults.length} result{flatResults.length !== 1 ? 's' : ''}
                </div>
                {flatResults.map(item => {
                  const color = getCategoryColor(item.category);
                  return (
                    <div
                      key={item.type}
                      draggable
                      onDragStart={(e) => onDragStart(e, item.type)}
                      onClick={() => { addNode(item.type); }}
                      className="group flex items-center gap-2 px-3 py-2 cursor-grab active:cursor-grabbing transition-colors"
                      onMouseEnter={e => (e.currentTarget.style.backgroundColor = 'var(--color-bg-field-hover)')}
                      onMouseLeave={e => (e.currentTarget.style.backgroundColor = 'transparent')}
                    >
                      <div className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: color }} />
                      <div className="flex-1 min-w-0">
                        <div className="text-[11px] truncate" style={{ color: 'var(--color-text-primary)' }}>{item.display_name}</div>
                        <div className="text-[9px] truncate" style={{ color: 'var(--color-text-muted)' }}>{item.category}</div>
                      </div>
                      {item.output && item.output.length > 0 && (
                        <span className="text-[9px] px-1 py-px rounded shrink-0" style={{ backgroundColor: 'var(--color-bg-field)', color: 'var(--color-text-muted)' }}>
                          {item.output[0]}
                        </span>
                      )}
                    </div>
                  );
                })}
                {flatResults.length === 0 && (
                  <div className="px-4 py-8 text-center text-[11px] italic" style={{ color: 'var(--color-text-muted)' }}>No nodes found</div>
                )}
              </div>
            ) : (
              <div className="pb-12">
                {treeData.map((item, i) => (
                  <CategoryGroup
                    key={i}
                    item={item}
                    onDragStart={onDragStart}
                    onAddNode={addNode}
                  />
                ))}
              </div>
            )}
          </div>

          {/* Resize handle */}
          <div
            onMouseDown={startResizing}
            className="absolute top-0 right-0 w-1 h-full cursor-col-resize group z-50"
          >
            <div className="w-px h-full mx-auto transition-colors" style={{ backgroundColor: 'var(--color-border-subtle)' }} />
          </div>
        </div>
      )}
    </div>
  );
}
