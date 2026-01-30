import { memo, useCallback, useMemo, useState, useRef, useEffect, useLayoutEffect } from 'react';
import { Handle, Position, NodeResizer, type NodeProps, type Node } from '@xyflow/react';
import type { NodeData } from '../types';
import { useFlow } from '../hooks/useFlowContext';
import { createPortal } from 'react-dom';

// === 常量定义：连接点颜色 ===
const TYPE_COLORS: Record<string, string> = {
  "ZARR_HANDLE": "#eab308", "IMAGE": "#3b82f6", "INT": "#22c55e",
  "FLOAT": "#ec4899", "STRING": "#94a3b8", "DATA_STREAM": "#a78bfa", "METADATA": "#fb7185",
  "default": "#a1a1aa",
  "DASK_ARRAY": "#06b6d4",
};

// ==========================================
// 1. 辅助组件：弹窗输入框 (完全保留你的代码)
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

  const popupStyle: React.CSSProperties = useMemo(() => {
    if (!anchorRect) return { top: '50%', left: '50%', transform: 'translate(-50%, -50%)' };
    let top = anchorRect.bottom + 5;
    let left = anchorRect.left + (anchorRect.width / 2) - 160;
    if (left < 10) left = 10;
    if (left + 320 > window.innerWidth) left = window.innerWidth - 330;
    if (top + 50 > window.innerHeight) top = anchorRect.top - 50;
    return { top: `${top}px`, left: `${left}px`, position: 'fixed', transform: 'none' };
  }, [anchorRect]);

  return createPortal(
    <div className="fixed inset-0 z-[9999]" onClick={onClose}>
      <div className="bg-[var(--node-body)] border border-[var(--node-border)] rounded-lg shadow-xl px-2 py-1.5 flex items-center gap-2 w-[320px] animate-in fade-in zoom-in-95 duration-100 ring-1 ring-black/5" style={popupStyle} onClick={e => e.stopPropagation()}>
        <span className="text-[var(--text-label)] font-bold text-[10px] select-none uppercase tracking-wider shrink-0">Value</span>
        <input ref={inputRef} className="flex-1 bg-[var(--widget-bg)] text-[var(--text-val)] text-xs font-mono px-2 py-1 rounded-md border border-transparent focus:border-blue-500/50 outline-none transition-all placeholder-[var(--text-label)]" value={val} onChange={e => setVal(e.target.value)} onKeyDown={e => { if (e.key === 'Enter') { e.preventDefault(); onSave(val); } if (e.key === 'Escape') { onClose(); } }} />
        <button onClick={() => onSave(val)} className="bg-[var(--widget-bg)] hover:bg-[var(--widget-hover)] text-[var(--text-head)] text-[10px] font-bold px-2 py-1 rounded border border-[var(--node-border)] transition-colors shrink-0">OK</button>
      </div>
    </div>, document.body
  );
};

// ==========================================
// 2. Fallback 组件 (完全保留你的代码)
// ==========================================
interface FallbackWidgetProps {
  name: string;
  value: unknown;
  type: string;
  onChange: (name: string, value: unknown) => void;
}

const FallbackWidget = ({ name, value, type, onChange }: FallbackWidgetProps) => {
  const displayValue = typeof value === 'object' && value !== null
    ? JSON.stringify(value)
    : String(value ?? '');

  return (
    <div className="nodrag group flex flex-col bg-[var(--widget-bg)]/50 rounded-sm px-1.5 py-1 mb-[3px] border border-yellow-500/30">
      <div className="flex justify-between items-center mb-0.5">
        <span className="text-[9px] text-[var(--text-label)] font-semibold select-none truncate" title={name}>{name}</span>
        <span className="text-[8px] text-yellow-500 font-mono px-1 bg-yellow-500/10 rounded">UNKNOWN: {type}</span>
      </div>
      <div className="bg-[var(--widget-inner)] rounded-sm px-1 flex items-center min-w-0 border border-[var(--node-border)] focus-within:border-yellow-500/50 transition-colors">
        <input
          className="bg-transparent text-[10px] text-[var(--text-val)] outline-none font-mono w-full py-0.5 border-none focus:ring-0 placeholder-gray-500/30"
          value={displayValue}
          onChange={(e) => onChange(name, e.target.value)}
          placeholder="Raw value override..."
          onPointerDown={(e) => e.stopPropagation()}
        />
      </div>
    </div>
  );
};

// ==========================================
// 3. 组件注册表
// ==========================================
interface ControlWidgetProps {
  name: string;
  config: [string, Record<string, unknown>?];
  value: unknown;
  onChange: (name: string, value: unknown) => void;
}

const WIDGET_REGISTRY: Record<string, React.ComponentType<ControlWidgetProps>> = {};

// ==========================================
// 4. 主控组件：ControlWidget (修复：添加 stopPropagation)
// ==========================================
const ControlWidget = ({ name, config, value, onChange }: ControlWidgetProps) => {
  const [type, options] = config;
  const [showPopup, setShowPopup] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const [anchorRect, setAnchorRect] = useState<DOMRect | null>(null);

  const containerClass = "nodrag group flex items-center justify-between bg-[var(--widget-bg)] hover:bg-[var(--widget-hover)] rounded-sm px-1.5 h-6 mb-[3px] transition-colors border border-transparent hover:border-[var(--node-border)] select-none";
  const labelClass = "text-[10px] text-[var(--text-label)] font-semibold mr-2 shrink-0 tracking-tight max-w-[50%] truncate cursor-default";
  const inputBgClass = "bg-[var(--widget-inner)] rounded-sm px-1 h-4 flex items-center min-w-0";
  const inputClass = "bg-transparent text-[10px] text-[var(--text-val)] text-right outline-none font-mono w-full h-full border-none focus:ring-0 p-0 leading-none truncate cursor-pointer";

  const handlePopupOpen = () => { if (containerRef.current) { setAnchorRect(containerRef.current.getBoundingClientRect()); } setShowPopup(true); };

  // [修复] 阻止冒泡，防止双击输入框时折叠节点
  const stopProp = (e: React.MouseEvent | React.PointerEvent) => e.stopPropagation();

  // A. Boolean
  if (type === 'BOOLEAN') {
      const boolVal = value === true || String(value).toLowerCase() === 'true';
      return (
        <div
          className={`${containerClass} cursor-pointer`}
          onClick={() => onChange(name, !boolVal)}
          onDoubleClick={stopProp} // [修复]
        >
           <span className={labelClass} title={name}>{name}</span>
           <div className={`relative w-8 h-4 rounded-full transition-colors duration-200 ease-in-out ${boolVal ? 'bg-blue-500' : 'bg-gray-400/50'}`}>
              <div className={`absolute top-[2px] left-[2px] w-3 h-3 bg-white rounded-full shadow-sm transition-transform duration-200 ease-in-out ${boolVal ? 'translate-x-4' : 'translate-x-0'}`} />
           </div>
        </div>
      );
  }

  // B. String
  if (type === 'STRING') {
     return (
        <>
          <div ref={containerRef} className={`${containerClass} cursor-text`} onClick={handlePopupOpen} onDoubleClick={stopProp}>
             <span className={labelClass} title={name}>{name}</span>
             <div className={`${inputBgClass} flex-1 justify-end border border-[var(--node-border)] hover:border-[#888] transition-colors`}>
                <span className={`${inputClass} hover:text-blue-500`} title={value != null ? String(value) : ''}>
                  {value != null && String(value) !== '' ? String(value) : <span className="opacity-30 italic">Empty</span>}
                </span>
             </div>
          </div>
          {showPopup && <ValuePopup initialValue={String(value) || ''} anchorRect={anchorRect} onSave={(v) => { onChange(name, v); setShowPopup(false); }} onClose={() => setShowPopup(false)} />}
        </>
     )
  }

  // C. Number
  if (type === 'INT' || type === 'FLOAT' || type === 'LONG') {
      const isFloat = type === 'FLOAT';
      const step = Number(options?.step) || (isFloat ? 0.01 : 1);
      const val = Number(value ?? options?.default ?? 0);

      const handleStep = (direction: 1 | -1, shiftKey: boolean) => {
          const multiplier = shiftKey ? 10 : 1;
          const numericVal = Number(val);
          const numericStep = Number(step);
          let newValue = numericVal + (numericStep * direction * multiplier);

          if (isFloat) newValue = parseFloat(newValue.toFixed(5));
          else newValue = Math.round(newValue);

          if (options?.min !== undefined && newValue < (options.min as number)) newValue = options.min as number;
          if (options?.max !== undefined && newValue > (options.max as number)) newValue = options.max as number;

          onChange(name, newValue);
      };

      return (
        <div className={containerClass} onDoubleClick={stopProp}>
           <span className={labelClass} title={name}>{name}</span>
           <div className="flex items-center gap-0.5 shrink-0 ml-auto">
              <button
                className="text-[var(--text-label)] hover:text-[var(--text-head)] hover:bg-black/10 dark:hover:bg-white/10 rounded cursor-pointer select-none px-1 transition-colors active:scale-90 flex items-center justify-center h-4 w-4"
                onClick={(e) => { e.stopPropagation(); handleStep(-1, e.shiftKey); }}
              >
                -
              </button>

              <div className={`${inputBgClass} w-16 border border-[var(--node-border)] focus-within:border-blue-500`}>
                  <input
                    type="number"
                    className={`${inputClass} text-center no-spinners selection:bg-blue-500/30`}
                    value={val}
                    onChange={e => {
                        let v = Number(e.target.value);
                        if (!isFloat) v = Math.round(v);
                        onChange(name, v);
                    }}
                    step={step}
                    onDoubleClick={stopProp} // [修复]
                    onPointerDown={(e) => e.stopPropagation()}
                    onKeyDown={(e) => e.stopPropagation()}
                  />
              </div>

              <button
                className="text-[var(--text-label)] hover:text-[var(--text-head)] hover:bg-black/10 dark:hover:bg-white/10 rounded cursor-pointer select-none px-1 transition-colors active:scale-90 flex items-center justify-center h-4 w-4"
                onClick={(e) => { e.stopPropagation(); handleStep(1, e.shiftKey); }}
              >
                +
              </button>
           </div>
        </div>
      );
  }

  // D. Dropdown
  if (Array.isArray(type)) {
    return (
      <div className={containerClass} onDoubleClick={stopProp}>
        <span className={labelClass} title={name}>{name}</span>
        <div className={`${inputBgClass} flex-1 justify-end border border-[var(--node-border)] hover:border-[#888]`}>
          <select
            className={`${inputClass} cursor-pointer appearance-none bg-transparent nodrag text-right pr-1`}
            value={String(value ?? type[0])}
            onChange={e => onChange(name, e.target.value)}
            onPointerDown={e => e.stopPropagation()}
          >
            {type.map((o: string) => <option key={o} value={o} className="bg-[var(--node-body)] text-[var(--text-val)]">{o}</option>)}
          </select>
        </div>
      </div>
    );
  }

  const RegisteredWidget = WIDGET_REGISTRY[type as string];
  if (RegisteredWidget) return <RegisteredWidget name={name} config={config} value={value} onChange={onChange} />;
  return <FallbackWidget name={name} value={value} type={type as string} onChange={onChange} />;
};


// ==========================================
// 5. 核心节点组件：DynamicNode
// ==========================================
const DynamicNode = ({ id, data, selected }: NodeProps<Node<NodeData>>) => {
  const { nodeSpec, values = {}, progress, message } = data || {};

  const { updateNodeData, connectingType } = useFlow();
  const [collapsed, setCollapsed] = useState(false);

  const valuesRef = useRef<Record<string, unknown>>(values);

  useLayoutEffect(() => {
    valuesRef.current = values;
  }, [values]);

  const handleUpdate = useCallback((key: string, v: unknown) => {
    updateNodeData(id, { values: { ...valuesRef.current, [key]: v } });
  }, [id, updateNodeData, valuesRef]);

  const { linkInputs, widgets, outputs } = useMemo(() => {
    if (!nodeSpec) return { linkInputs: [], outputs: [], widgets: [] };

    const links: { name: string; type: string; color: string }[] = [];
    const wids: { name: string; config: [string, Record<string, unknown>?] }[] = [];
    const outs: { name: string; type: string; color: string }[] = [];

    const allInputs = { ...(nodeSpec?.input?.required || {}), ...(nodeSpec?.input?.optional || {}) };

    if (allInputs) {
      Object.entries(allInputs).forEach(([name, config]) => {
        if (!config) return;
        const rawType = config[0];
        const rawOptions = config[1];
        const isDropdown = Array.isArray(rawType);
        const hasConfigDict = rawOptions && typeof rawOptions === 'object';
        const isPrimitive = ["INT", "FLOAT", "STRING", "BOOLEAN", "LONG"].includes(rawType as string);
        if (isDropdown || hasConfigDict || isPrimitive) {
             wids.push({ name, config: config as [string, Record<string, unknown>?] });
        } else {
             links.push({ name, type: rawType as string, color: TYPE_COLORS[rawType as string] || TYPE_COLORS.default });
        }
      });
    }
    if (nodeSpec.output && Array.isArray(nodeSpec.output)) {
      nodeSpec.output.forEach((outType) => {
          outs.push({ name: outType, type: outType, color: TYPE_COLORS[outType] || TYPE_COLORS.default });
      });
    }
    return { linkInputs: links, outputs: outs, widgets: wids };
  }, [nodeSpec, TYPE_COLORS]);


  const isRunning = progress !== undefined && progress > 0 && progress < 100;
  const isIndeterminate = progress === -1;
  const isComplete = progress === 100;
  const isError = message?.toLowerCase().includes('error');
  return (
    <>
      <NodeResizer color="#3b82f6" isVisible={!!selected && !collapsed} minWidth={220} minHeight={60} />

      <div className={`node-wrapper relative rounded-[4px] shadow-md bg-[var(--node-body)] transition-all group flex flex-col 
          ${selected ? 'border-[#eee] ring-1 ring-[#eee]/30' : 'border-[var(--node-border)]'} 
          ${isError ? 'border-red-500 shadow-red-500/30' : 'border'}`}
      >
        {/* Header (始终显示) */}
        <div
          className="relative h-6 px-2 flex items-center justify-between bg-[var(--node-header)] border-b border-[var(--node-border)] z-10 rounded-t-[4px] shrink-0 cursor-pointer select-none"
          onDoubleClick={(e) => { e.stopPropagation(); setCollapsed(!collapsed); }}
        >
           <span className="text-[11px] font-bold text-[var(--text-head)] truncate mr-2">{nodeSpec.display_name}</span>
           <div className="flex items-center gap-1.5">
             <div className={`w-1.5 h-1.5 rounded-full shadow-sm transition-colors ${isError ? 'bg-red-500' : isComplete ? 'bg-green-500' : (isRunning || isIndeterminate) ? 'bg-yellow-400' : 'bg-[#444]'}`} />
           </div>
        </div>

        {/* Body (使用 Grid 动画折叠，保留 DOM) */}
        <div className={`node-collapse-wrapper ${collapsed ? 'collapsed' : ''}`}>
           <div className="node-collapse-inner"
                style={{ overflow: collapsed ? 'hidden' : 'visible' }}>
             {/* 内容区域 */}
             <div className="p-1.5 space-y-1.5 flex flex-col">
                <div className="flex justify-between gap-4">
                  {/* Inputs */}
                  <div className="flex flex-col gap-1.5 flex-1 min-w-0">
                    {linkInputs.map((input) => (
                      <div key={input.name} className="relative h-3.5 flex items-center pl-2">
                        <Handle type="target" position={Position.Left} id={input.name}
                                className="!w-2.5 !h-2.5 z-50"
                                style={{ backgroundColor: input.color, left: '-13px' }}
                        />
                        <span className="text-[10px] text-[var(--text-label)] truncate">{input.name}</span>
                      </div>
                    ))}
                  </div>
                  {/* Outputs */}
                  <div className="flex flex-col gap-1.5 items-end flex-1 min-w-0">
                    {outputs.map((output, i) => (
                      <div key={i} className="relative h-3.5 flex items-center justify-end pr-2">
                        <span className="text-[10px] text-[var(--text-label)] truncate uppercase">{output.name}</span>
                        <Handle type="source" position={Position.Right} id={`${i}`}
                                className="!w-2.5 !h-2.5 z-50"
                                style={{ backgroundColor: output.color, right: '-13px' }}
                        />
                      </div>
                    ))}
                  </div>
                </div>

                {/* Widgets */}
                {widgets.length > 0 && (
                  <div className="pt-1.5 border-t border-[var(--node-border)] space-y-[2px] mt-1">
                    {widgets.map((w) => (
                        // 这里直接内联 ControlWidget 或者引用外部组件
                        // 确保 ControlWidget 内部处理了 stopPropagation
                         <ControlWidget key={w.name} {...w} value={values[w.name]} onChange={handleUpdate} />
                    ))}
                  </div>
                )}
             </div>
           </div>
        </div>

        {/* Footer (进度条) - 放在 Grid 外面或者里面都可以，这里放在外面更清晰 */}
        {(!collapsed && (isRunning || isIndeterminate || message)) && (
          <div className="relative mt-auto">
             {/* ... 你的进度条代码 ... */}
             <div className="h-1 bg-[var(--widget-bg)] w-full overflow-hidden relative">
                 {isIndeterminate ? (
                   <div className="absolute inset-0 w-full h-full animate-shimmer bg-gradient-to-r from-transparent via-green-400 to-transparent opacity-80" />
                 ) : (
                   <div className="h-full bg-green-500 transition-all duration-300" style={{ width: `${progress}%` }} />
                 )}
             </div>
             <div className="px-2 py-1 flex justify-between text-[9px] font-mono border-t border-[var(--node-border)] bg-[var(--node-body)] text-[var(--text-sub)]">
                <span className="truncate max-w-[80%]">{message || "Running..."}</span>
                <span>{isRunning ? `${Math.floor(progress)}%` : ''}</span>
             </div>
          </div>
        )}
      </div>
    </>
    );
    };

    export default memo(DynamicNode);