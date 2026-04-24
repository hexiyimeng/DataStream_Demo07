import { memo, useCallback, useMemo, useState, useRef, useEffect, useLayoutEffect } from 'react';
import { Handle, Position, NodeResizer, type NodeProps, type Node } from '@xyflow/react';
import type { NodeData } from '../types';
import { useFlow } from '../hooks/useFlowContext';
import { createPortal } from 'react-dom';
import { Pill } from './ui/Pill';

// === Handle type color registry ===
const TYPE_COLORS: Record<string, string> = {
  ZARR_HANDLE: '#fbbf24',
  IMAGE: '#3b82f6',
  INT: '#34d399',
  FLOAT: '#f472b6',
  STRING: '#94a3b8',
  DATA_STREAM: '#a78bfa',
  METADATA: '#fb7185',
  DASK_ARRAY: '#22d3ee',
  default: '#6b7280',
};

// Category → accent strip color (low saturation)
const CATEGORY_COLORS: Record<string, string> = {
  'BrainFlow/IO': '#22c55e',
  'BrainFlow/Processing': '#3b82f6',
  'BrainFlow/Segmentation': '#f59e0b',
  'BrainFlow/Output': '#a78bfa',
  'Image': '#3b82f6',
  'Loader': '#22c55e',
  'Output': '#a78bfa',
  'Processing': '#f59e0b',
  'Transform': '#06b6d4',
  'Utils': '#94a3b8',
};

function getCategoryAccent(category?: string): string {
  if (!category) return '#6b7280';
  const top = category.split('/')[0];
  return CATEGORY_COLORS[top] ?? '#6b7280';
}

// === NodeStatus map ===
type NodeStatus = 'idle' | 'ready' | 'submitted' | 'running' | 'done' | 'failed' | 'cancelled' | 'error';

const STATUS_PILL_VARIANT: Record<NodeStatus, 'idle' | 'info' | 'running' | 'success' | 'danger' | 'warning' | 'muted'> = {
  idle: 'idle',
  ready: 'info',
  submitted: 'info',
  running: 'running',
  done: 'success',
  failed: 'danger',
  cancelled: 'warning',
  error: 'danger',
};

const STATUS_LABEL: Record<NodeStatus, string> = {
  idle: '',
  ready: 'Ready',
  submitted: 'Queued',
  running: 'Running',
  done: 'Done',
  failed: 'Failed',
  cancelled: 'Cancelled',
  error: 'Error',
};

// ==========================================
// 1. ValuePopup — string editor popup
// ==========================================
interface ValuePopupProps {
  initialValue: string;
  onSave: (value: string) => void;
  onClose: () => void;
  anchorRect: DOMRect | null;
}

const ValuePopup = ({ initialValue, onSave, onClose, anchorRect }: ValuePopupProps) => {
  const [val, setVal] = useState(initialValue);
  const inputRef = useRef<HTMLInputElement>(null);
  useEffect(() => { inputRef.current?.focus(); inputRef.current?.select(); }, []);

  const style: React.CSSProperties = useMemo(() => {
    if (!anchorRect) return { top: '50%', left: '50%', transform: 'translate(-50%, -50%)' };
    let top = anchorRect.bottom + 6;
    let left = anchorRect.left + anchorRect.width / 2 - 160;
    if (left < 10) left = 10;
    if (left + 320 > window.innerWidth) left = window.innerWidth - 330;
    if (top + 50 > window.innerHeight) top = anchorRect.top - 55;
    return { top, left, position: 'fixed' as const };
  }, [anchorRect]);

  return createPortal(
    <div className="fixed inset-0 z-[9999]" onClick={onClose}>
      <div
        className="bg-[var(--color-bg-surface)] border border-[var(--color-border-default)] rounded-[var(--radius-lg)] shadow-[var(--shadow-floating)] px-3 py-2.5 flex items-center gap-3 w-[320px]"
        style={style}
        onClick={e => e.stopPropagation()}
      >
        <span className="text-[10px] text-[var(--color-text-muted)] font-semibold uppercase tracking-wider shrink-0">Value</span>
        <input
          ref={inputRef}
          className="flex-1 bg-[var(--color-bg-field)] text-[var(--color-text-primary)] text-[11px] font-mono px-2.5 py-1.5 rounded-[var(--radius-md)] border border-[var(--color-border-default)] focus:border-[var(--color-border-focus)] outline-none transition-colors placeholder-[var(--color-text-muted)]"
          value={val}
          onChange={e => setVal(e.target.value)}
          onKeyDown={e => {
            if (e.key === 'Enter') { e.preventDefault(); onSave(val); }
            if (e.key === 'Escape') onClose();
          }}
        />
        <button
          onClick={() => onSave(val)}
          className="shrink-0 px-2.5 py-1.5 text-[10px] font-bold text-white bg-[var(--color-accent)] hover:bg-[var(--color-accent-hover)] rounded-[var(--radius-md)] transition-colors duration-[var(--motion-fast)]"
        >
          OK
        </button>
      </div>
    </div>, document.body
  );
};

// ==========================================
// 2. TypeBadge — small inline type label
// ==========================================
function TypeBadge({ type }: { type: string }) {
  const color = TYPE_COLORS[type] ?? TYPE_COLORS.default;
  return (
    <span
      className="text-[9px] font-mono px-1 py-px rounded-full uppercase tracking-wider font-bold"
      style={{ color, backgroundColor: `${color}18`, border: `1px solid ${color}30` }}
    >
      {type}
    </span>
  );
}

// ==========================================
// 3. ControlWidget
// ==========================================
interface ControlWidgetProps {
  name: string;
  config: [string, Record<string, unknown>?];
  value: unknown;
  onChange: (name: string, value: unknown) => void;
  disabled?: boolean;
}

const ControlWidget = ({ name, config, value, onChange, disabled = false }: ControlWidgetProps) => {
  const [type, options] = config;
  const [showPopup, setShowPopup] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const [anchorRect, setAnchorRect] = useState<DOMRect | null>(null);
  const stopProp = (e: React.MouseEvent | React.PointerEvent) => e.stopPropagation();
  const stopPropKey = (e: React.KeyboardEvent) => e.stopPropagation();

  // --- Boolean ---
  if (type === 'BOOLEAN') {
    const boolVal = value === true || String(value).toLowerCase() === 'true';
    return (
      <div
        className={[
          'nodrag group flex items-center justify-between',
          'bg-[var(--color-bg-field)] hover:bg-[var(--color-bg-field-hover)]',
          'rounded-[var(--radius-sm)] px-2 h-7 mb-[2px]',
          'border border-transparent hover:border-[var(--color-border-subtle)]',
          'transition-all duration-[var(--motion-fast)] select-none',
          disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer',
        ].join(' ')}
        onClick={disabled ? undefined : () => onChange(name, !boolVal)}
        onDoubleClick={stopProp}
      >
        <span className="text-[11px] text-[var(--color-text-secondary)] font-medium truncate max-w-[55%]">{name}</span>
        <div
          className={[
            'relative w-8 h-4 rounded-full transition-colors duration-200 shrink-0',
            boolVal ? 'bg-[var(--color-accent)]' : 'bg-[var(--color-border-default)]',
          ].join(' ')}
        >
          <div
            className={[
              'absolute top-[2px] left-[2px] w-3 h-3 bg-white rounded-full shadow-sm',
              'transition-transform duration-200',
              boolVal ? 'translate-x-4' : 'translate-x-0',
            ].join(' ')}
          />
        </div>
      </div>
    );
  }

  // --- String ---
  if (type === 'STRING') {
    const displayStr = value != null && String(value) !== '' ? String(value) : '';
    return (
      <>
        <div
          ref={containerRef}
          className={[
            'nodrag group flex items-center justify-between',
            'bg-[var(--color-bg-field)] hover:bg-[var(--color-bg-field-hover)]',
            'rounded-[var(--radius-sm)] px-2 h-7 mb-[2px]',
            'border border-transparent hover:border-[var(--color-border-subtle)]',
            'transition-all duration-[var(--motion-fast)] select-none',
            disabled ? '' : 'cursor-pointer',
          ].join(' ')}
          onClick={disabled ? stopProp : () => { setAnchorRect(containerRef.current?.getBoundingClientRect() ?? null); setShowPopup(true); }}
          onDoubleClick={stopProp}
        >
          <span className="text-[11px] text-[var(--color-text-secondary)] font-medium truncate max-w-[55%]">{name}</span>
          <div className="flex-1 min-w-0 flex items-center justify-end ml-2">
            <span className={[
              'text-[11px] font-mono text-right truncate max-w-[120px]',
              displayStr ? 'text-[var(--color-text-primary)]' : 'text-[var(--color-text-muted)] italic',
            ].join(' ')}>
              {displayStr || '—'}
            </span>
          </div>
        </div>
        {showPopup && !disabled && (
          <ValuePopup
            initialValue={String(value ?? '')}
            anchorRect={anchorRect}
            onSave={v => { onChange(name, v); setShowPopup(false); }}
            onClose={() => setShowPopup(false)}
          />
        )}
      </>
    );
  }

  // --- Number (INT / FLOAT / LONG) ---
  if (type === 'INT' || type === 'FLOAT' || type === 'LONG') {
    const isFloat = type === 'FLOAT';
    const step = Number(options?.step) || (isFloat ? 0.01 : 1);
    const val = Number(value ?? options?.default ?? 0);
    const min = options?.min as number | undefined;
    const max = options?.max as number | undefined;

    const stepVal = (direction: 1 | -1, shift: boolean) => {
      const mult = shift ? 10 : 1;
      let v = val + step * direction * mult;
      if (isFloat) v = parseFloat(v.toFixed(5));
      else v = Math.round(v);
      if (min !== undefined) v = Math.max(v, min);
      if (max !== undefined) v = Math.min(v, max);
      onChange(name, v);
    };

    return (
      <div
        className={[
          'nodrag flex items-center justify-between',
          'bg-[var(--color-bg-field)] hover:bg-[var(--color-bg-field-hover)]',
          'rounded-[var(--radius-sm)] px-2 h-7 mb-[2px]',
          'border border-transparent hover:border-[var(--color-border-subtle)]',
          'transition-all duration-[var(--motion-fast)] select-none',
        ].join(' ')}
        onDoubleClick={stopProp}
      >
        <span className="text-[11px] text-[var(--color-text-secondary)] font-medium truncate max-w-[55%]">{name}</span>
        <div className="flex items-center gap-0.5 shrink-0">
          <button
            className={[
              'w-5 h-5 rounded flex items-center justify-center',
              'text-[var(--color-text-muted)] hover:text-[var(--color-text-primary)] hover:bg-[var(--color-bg-surface-3)]',
              'transition-colors duration-[var(--motion-fast)]',
              disabled ? 'opacity-40 cursor-not-allowed' : 'cursor-pointer',
            ].join(' ')}
            onClick={disabled ? undefined : (e) => { e.stopPropagation(); stepVal(-1, e.shiftKey); }}
            disabled={disabled}
          >
            <svg className="w-2.5 h-2.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={3}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M5 12h14" />
            </svg>
          </button>
          <div className={[
            'w-[68px] h-[22px] flex items-center justify-center',
            'bg-[var(--color-bg-surface)] rounded-[var(--radius-sm)]',
            'border border-[var(--color-border-subtle)]',
            'focus-within:border-[var(--color-border-focus)]',
            'transition-colors duration-[var(--motion-fast)]',
          ].join(' ')}>
            <input
              type="number"
              className="w-full h-full text-[11px] text-center font-mono text-[var(--color-text-primary)] bg-transparent outline-none border-none no-spinners"
              value={val}
              disabled={disabled}
              onChange={e => {
                let v = Number(e.target.value);
                if (!isFloat) v = Math.round(v);
                onChange(name, v);
              }}
              step={step}
              onPointerDown={stopProp}
              onKeyDown={stopPropKey}
            />
          </div>
          <button
            className={[
              'w-5 h-5 rounded flex items-center justify-center',
              'text-[var(--color-text-muted)] hover:text-[var(--color-text-primary)] hover:bg-[var(--color-bg-surface-3)]',
              'transition-colors duration-[var(--motion-fast)]',
              disabled ? 'opacity-40 cursor-not-allowed' : 'cursor-pointer',
            ].join(' ')}
            onClick={disabled ? undefined : (e) => { e.stopPropagation(); stepVal(1, e.shiftKey); }}
            disabled={disabled}
          >
            <svg className="w-2.5 h-2.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={3}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 5v14M5 12h14" />
            </svg>
          </button>
        </div>
      </div>
    );
  }

  // --- Dropdown (array type) ---
  if (Array.isArray(type)) {
    return (
      <div
        className={[
          'nodrag flex items-center justify-between',
          'bg-[var(--color-bg-field)] hover:bg-[var(--color-bg-field-hover)]',
          'rounded-[var(--radius-sm)] px-2 h-7 mb-[2px]',
          'border border-transparent hover:border-[var(--color-border-subtle)]',
          'transition-all duration-[var(--motion-fast)] select-none',
        ].join(' ')}
        onDoubleClick={stopProp}
      >
        <span className="text-[11px] text-[var(--color-text-secondary)] font-medium truncate max-w-[55%]">{name}</span>
        <div className="flex items-center gap-1 shrink-0 ml-2">
          <div className={[
            'bg-[var(--color-bg-surface)] rounded-[var(--radius-sm)] px-1.5 h-[22px]',
            'border border-[var(--color-border-subtle)]',
            'flex items-center gap-1',
            'transition-colors duration-[var(--motion-fast)]',
          ].join(' ')}>
            <select
              className="text-[11px] font-mono text-right text-[var(--color-text-primary)] bg-transparent outline-none border-none appearance-none cursor-pointer pr-1 max-w-[100px]"
              value={String(value ?? type[0])}
              onChange={e => onChange(name, e.target.value)}
              onPointerDown={stopProp}
              disabled={disabled}
            >
              {type.map((o: string) => (
                <option key={o} value={o} className="bg-[var(--color-bg-surface)]">{o}</option>
              ))}
            </select>
            <svg className="w-2.5 h-2.5 text-[var(--color-text-muted)] shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
            </svg>
          </div>
        </div>
      </div>
    );
  }

  return null;
};

// ==========================================
// 4. DynamicNode — main component
// ==========================================
const DynamicNode = ({ id, data, selected }: NodeProps<Node<NodeData>>) => {
  const { nodeSpec, values = {}, progress, message, _invalid, _warning, runState, waitingFor } = data || {};
  const { isExecutionLocked, updateNodeData } = useFlow();
  const locked = isExecutionLocked ?? false;
  const [collapsed, setCollapsed] = useState(false);
  const valuesRef = useRef(values);
  useLayoutEffect(() => { valuesRef.current = values; }, [values]);

  const handleUpdate = useCallback((key: string, v: unknown) => {
    updateNodeData(id, { values: { ...valuesRef.current, [key]: v } });
  }, [id, updateNodeData]);

  // --- Status ---
  const status: NodeStatus = runState ?? 'idle';
  const progressType = nodeSpec?.progress_type || 'state_only';
  const progressValue = progress ?? 0;
  const showPercentage = progressType === 'chunk_count' && progress !== null && !nodeSpec?.output_node;

  const hasFooter = !collapsed && status !== 'idle' && status !== 'ready';
  const accentColor = getCategoryAccent(nodeSpec?.category);
  const isOutputNode = nodeSpec?.output_node === true;

  // --- Footer message ---
  const footerMessage = useMemo(() => {
    if (status === 'submitted') return 'Queued';
    if (status === 'running' && waitingFor?.length) return `Waiting for ${waitingFor[0]}${waitingFor.length > 1 ? ` +${waitingFor.length - 1}` : ''}`;
    if (status === 'running' && progress !== null) return message || 'Running...';
    if (status === 'done') return 'Completed';
    if (status === 'failed') return message || 'Failed';
    if (status === 'cancelled') return 'Cancelled';
    if (showPercentage) return message || 'Running...';
    return message || '';
  }, [status, waitingFor, progress, message, showPercentage]);

  // --- Parse IO & widgets ---
  const { linkInputs, widgets, outputs } = useMemo(() => {
    if (!nodeSpec) return { linkInputs: [], outputs: [], widgets: [] };
    const links: { name: string; type: string; color: string }[] = [];
    const wids: { name: string; config: [string, Record<string, unknown>?] }[] = [];
    const outs: { name: string; type: string; color: string }[] = [];

    const allInputs = { ...(nodeSpec.input?.required || {}), ...(nodeSpec.input?.optional || {}) };
    Object.entries(allInputs).forEach(([name, config]) => {
      if (!config) return;
      const [rawType, rawOptions] = config as [string, Record<string, unknown>?];
      const isDropdown = Array.isArray(rawType);
      const hasOptions = rawOptions && typeof rawOptions === 'object';
      const isPrimitive = ['INT', 'FLOAT', 'STRING', 'BOOLEAN', 'LONG'].includes(rawType);
      if (isDropdown || hasOptions || isPrimitive) {
        wids.push({ name, config: [rawType, rawOptions] });
      } else {
        links.push({ name, type: rawType, color: TYPE_COLORS[rawType] ?? TYPE_COLORS.default });
      }
    });

    if (nodeSpec.output && Array.isArray(nodeSpec.output)) {
      nodeSpec.output.forEach((outType: string) => {
        outs.push({ name: outType, type: outType, color: TYPE_COLORS[outType] ?? TYPE_COLORS.default });
      });
    }
    return { linkInputs: links, outputs: outs, widgets: wids };
  }, [nodeSpec]);

  const hasIO = linkInputs.length > 0 || outputs.length > 0;
  const hasWidgets = widgets.length > 0;

  return (
    <>
      <NodeResizer
        color="var(--color-accent)"
        isVisible={!!selected && !collapsed}
        minWidth={260}
        minHeight={80}
      />

      <div
        className={[
          'node-wrapper',
          'relative flex flex-col',
          'rounded-[var(--radius-lg)]',
          'border transition-all duration-[var(--motion-normal)]',
          'bg-[var(--color-node-body)]',
          selected
            ? 'border-[var(--color-accent)] shadow-[var(--color-node-shadow-selected)]'
            : 'border-[var(--color-node-border)] shadow-[var(--color-node-shadow)] hover:shadow-[var(--color-node-shadow-hover)] hover:border-[var(--color-node-border-hover)]',
          _invalid ? 'border-[var(--color-warning)] ring-2 ring-[var(--color-warning)]/20' : '',
          status === 'running' ? 'node-running-pulse' : '',
        ].join(' ')}
        style={{ minWidth: 260 }}
      >
        {/* ===== ACCENT STRIP (left edge) ===== */}
        <div
          className="absolute left-0 top-3 bottom-3 w-[3px] rounded-full shrink-0"
          style={{ backgroundColor: accentColor, opacity: 0.8 }}
        />

        {/* ===== HEADER ===== */}
        <div
          className={[
            'relative pl-3 pr-2 py-2 flex items-center justify-between gap-2',
            'bg-[var(--color-node-header)] border-b border-[var(--color-border-subtle)]',
            'rounded-t-[var(--radius-lg)] cursor-pointer select-none',
            _invalid ? '' : '',
          ].join(' ')}
          onDoubleClick={(e) => { e.stopPropagation(); setCollapsed(!collapsed); }}
        >
          {/* Left: icon + title + subtitle */}
          <div className="flex items-center gap-2 min-w-0 flex-1">
            {/* Category dot */}
            <div
              className="w-2 h-2 rounded-full shrink-0"
              style={{ backgroundColor: accentColor, opacity: 0.9 }}
            />
            <div className="flex flex-col min-w-0">
              <div className="flex items-center gap-1.5 min-w-0">
                <span className={[
                  'text-[12px] font-semibold truncate',
                  _invalid ? 'text-[var(--color-warning)]' : 'text-[var(--color-text-primary)]',
                ].join(' ')}>
                  {nodeSpec?.display_name || data.opType || 'Unknown'}
                </span>
                {isOutputNode && (
                  <span className="shrink-0 text-[9px] font-bold uppercase tracking-wider px-1 py-0.5 rounded-full bg-[var(--color-info-soft)] text-[var(--color-info)] border border-[var(--color-info)]/20">
                    OUTPUT
                  </span>
                )}
              </div>
              <span className="text-[10px] text-[var(--color-text-muted)] truncate">
                {nodeSpec?.category?.split('/').pop() ?? nodeSpec?.category ?? data.opType}
              </span>
            </div>
          </div>

          {/* Right: status pill + collapse */}
          <div className="flex items-center gap-1.5 shrink-0">
            {hasFooter && (
              <Pill variant={STATUS_PILL_VARIANT[status]} dot pulse={status === 'running'}>
                {STATUS_LABEL[status]}
              </Pill>
            )}
            {/* Collapse toggle */}
            <button
              className="w-5 h-5 flex items-center justify-center rounded text-[var(--color-text-muted)] hover:text-[var(--color-text-primary)] hover:bg-[var(--color-bg-surface-3)] transition-colors duration-[var(--motion-fast)]"
              onClick={(e) => { e.stopPropagation(); setCollapsed(!collapsed); }}
            >
              <svg
                className={['w-3 h-3 transition-transform duration-200', collapsed ? '' : 'rotate-180'].join(' ')}
                viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2.5}
              >
                <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
              </svg>
            </button>
          </div>
        </div>

        {/* ===== INVALID WARNING ===== */}
        {_invalid && !collapsed && (
          <div className="px-3 py-1.5 bg-[var(--color-warning-soft)] border-b border-[var(--color-warning)]/20 text-[10px] text-[var(--color-warning)]">
            ⚠️ {_warning || 'Node type removed — please replace'}
          </div>
        )}

        {/* ===== BODY ===== */}
        <div className={`node-collapse-wrapper ${collapsed ? 'collapsed' : ''}`}>
          <div className="node-collapse-inner">
            <div className="px-3 py-2.5 flex flex-col gap-3">

              {/* ---- IO Section ---- */}
              {hasIO && (
                <div className="flex gap-3">
                  {/* Inputs column */}
                  {linkInputs.length > 0 && (
                    <div className="flex-1 min-w-0">
                      <div className="text-[9px] font-bold uppercase tracking-wider text-[var(--color-text-muted)] mb-1.5 px-1">
                        Inputs
                      </div>
                      <div className="flex flex-col gap-[3px]">
                        {linkInputs.map((input) => (
                          <div key={input.name} className="relative flex items-center h-6 pl-0.5">
                            <Handle
                              type="target"
                              position={Position.Left}
                              id={input.name}
                              className="!w-2.5 !h-2.5 !-left-[11px] !border-[1.5px] !border-black/20"
                              style={{ backgroundColor: input.color }}
                            />
                            <span className="text-[11px] text-[var(--color-text-secondary)] truncate">{input.name}</span>
                            <span className="ml-auto shrink-0 pl-2">
                              <TypeBadge type={input.type} />
                            </span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Outputs column */}
                  {outputs.length > 0 && (
                    <div className="flex-1 min-w-0">
                      <div className="text-[9px] font-bold uppercase tracking-wider text-[var(--color-text-muted)] mb-1.5 px-1 text-right">
                        Outputs
                      </div>
                      <div className="flex flex-col gap-[3px]">
                        {outputs.map((output, i) => (
                          <div key={i} className="relative flex items-center justify-end h-6 pr-0.5">
                            <span className="mr-auto pr-2">
                              <TypeBadge type={output.type} />
                            </span>
                            <Handle
                              type="source"
                              position={Position.Right}
                              id={String(i)}
                              className="!w-2.5 !h-2.5 !-right-[11px] !border-[1.5px] !border-black/20"
                              style={{ backgroundColor: output.color }}
                            />
                            <span className="text-[11px] text-[var(--color-text-secondary)] truncate uppercase">{output.name}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              )}

              {/* Empty IO hint */}
              {linkInputs.length === 0 && outputs.length === 0 && (
                <div className="text-[10px] text-[var(--color-text-muted)] italic px-1 py-0.5">
                  No inputs or outputs
                </div>
              )}

              {/* ---- Parameters Section ---- */}
              {hasWidgets && (
                <>
                  <div className="border-t border-[var(--color-border-subtle)] pt-2">
                    <div className="text-[9px] font-bold uppercase tracking-wider text-[var(--color-text-muted)] mb-1.5 px-1">
                      Parameters
                    </div>
                    <div className="flex flex-col gap-[2px]">
                      {widgets.map((w) => (
                        <ControlWidget
                          key={w.name}
                          {...w}
                          value={values[w.name]}
                          onChange={handleUpdate}
                          disabled={locked}
                        />
                      ))}
                    </div>
                  </div>
                </>
              )}
            </div>
          </div>
        </div>

        {/* ===== FOOTER ===== */}
        {hasFooter && (
          <div className="mt-auto">
            {/* Progress bar */}
            {showPercentage && (
              <div className="h-[3px] bg-[var(--color-bg-field)] w-full overflow-hidden relative">
                <div
                  className="h-full bg-[var(--color-accent)] transition-all duration-300 relative"
                  style={{ width: `${progressValue}%` }}
                />
                {status === 'running' && (
                  <div className="absolute inset-0 animate-shimmer" />
                )}
              </div>
            )}

            {/* Status row */}
            <div className={[
              'px-3 py-1.5 flex items-center justify-between',
              'border-t border-[var(--color-border-subtle)]',
              'bg-[var(--color-node-header)]',
              'text-[10px] font-mono',
              !showPercentage ? 'justify-center' : '',
            ].join(' ')}>
              <span className={[
                'truncate max-w-[75%]',
                !showPercentage ? 'text-center mx-auto text-[var(--color-text-muted)]' : 'text-[var(--color-text-secondary)]',
              ].join(' ')}>
                {footerMessage}
              </span>
              {showPercentage && status === 'running' && (
                <span className="shrink-0 ml-2 text-[var(--color-accent)] font-semibold text-[10px]">
                  {Math.floor(progressValue)}%
                </span>
              )}
            </div>
          </div>
        )}
      </div>
    </>
  );
};

export default memo(DynamicNode);
