// src/components/DynamicNode.tsx
import { memo, useCallback, useMemo, useState, useRef, useEffect } from 'react';
import { Handle, Position, NodeResizer, type NodeProps } from '@xyflow/react';
import type { BrainFlowNodeData } from '../types';
import { useFlow } from '../hooks/useFlowContext';
import { createPortal } from 'react-dom';

const TYPE_COLORS: Record<string, string> = {
  "ZARR_HANDLE": "#eab308", "IMAGE": "#3b82f6", "INT": "#22c55e",
  "FLOAT": "#ec4899", "STRING": "#94a3b8", "DATA_STREAM": "#a78bfa", "METADATA": "#fb7185",
  "default": "#a1a1aa",
  "DASK_ARRAY": "#06b6d4",
};

// ... (ValuePopup 保持不变) ...
const ValuePopup = ({ initialValue, onSave, onClose, anchorRect }: any) => {
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

// ... (ControlWidget 保持不变) ...
const ControlWidget = ({ name, config, value, onChange }: any) => {
  const [type, options] = config;
  const [showPopup, setShowPopup] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const [anchorRect, setAnchorRect] = useState<DOMRect | null>(null);
  const handlePopupOpen = () => { if (containerRef.current) { setAnchorRect(containerRef.current.getBoundingClientRect()); } setShowPopup(true); };
  const containerClass = "nodrag group flex items-center justify-between bg-[var(--widget-bg)] hover:bg-[var(--widget-hover)] rounded-sm px-1.5 h-6 mb-[3px] transition-colors border border-transparent hover:border-[#444]";
  const labelClass = "text-[10px] text-[var(--text-label)] font-semibold select-none mr-2 shrink-0 tracking-tight max-w-[50%]";
  const inputBgClass = "bg-[var(--widget-inner)] rounded-sm px-1 h-4 flex items-center min-w-0";
  const inputClass = "bg-transparent text-[10px] text-[var(--text-val)] text-right outline-none font-mono w-full h-full border-none focus:ring-0 p-0 leading-none truncate cursor-pointer";

  if (name === 'output_path' || name === 'file_path' || type === 'STRING') {
     const displayLabel = name === 'output_path' ? 'Output' : (name === 'file_path' ? 'Input' : name);
     return (
        <>
          <div ref={containerRef} className={containerClass} onClick={handlePopupOpen}>
             <span className={labelClass} title={name}>{displayLabel}</span>
             <div className={`${inputBgClass} flex-1 justify-end border border-[var(--node-border)] hover:border-[#888] transition-colors`}>
                <span className={`${inputClass} hover:text-blue-500`} title={value}>{value || <span className="opacity-30 italic">Empty</span>}</span>
             </div>
          </div>
          {showPopup && <ValuePopup initialValue={value} anchorRect={anchorRect} onSave={(v: any) => { onChange(name, v); setShowPopup(false); }} onClose={() => setShowPopup(false)} />}
        </>
     )
  }
  if (type === 'INT' || type === 'FLOAT') {
      const step = options?.step || (type === 'FLOAT' ? 0.01 : 1);
      const val = Number(value ?? options?.default ?? 0);
      const handleStep = (direction: 1 | -1, shiftKey: boolean) => { const multiplier = shiftKey ? 10 : 1; onChange(name, type === 'FLOAT' ? parseFloat((val + (step * direction * multiplier)).toFixed(5)) : val + (step * direction * multiplier)); };
      return (
        <div className={containerClass}>
           <span className={labelClass} title={name}>{name}</span>
           <div className="flex items-center gap-0.5 shrink-0 ml-auto">
              <button className="text-[var(--text-label)] hover:text-[var(--text-head)] cursor-pointer select-none px-0.5 transition-colors" onClick={(e) => handleStep(-1, e.shiftKey)}>◀</button>
              <div className={`${inputBgClass} w-16 border border-[var(--node-border)]`}><input type="number" className={`${inputClass} text-center no-spinners selection:bg-blue-500/30`} value={val} onChange={e => onChange(name, Number(e.target.value))} step={step} /></div>
              <button className="text-[var(--text-label)] hover:text-[var(--text-head)] cursor-pointer select-none px-0.5 transition-colors" onClick={(e) => handleStep(1, e.shiftKey)}>▶</button>
           </div>
        </div>
      );
  }
  if (Array.isArray(type)) {
    return (
      <div className={containerClass}>
        <span className={labelClass} title={name}>{name}</span>
        <div className={`${inputBgClass} flex-1 justify-end border border-[var(--node-border)]`}><select className={`${inputClass} cursor-pointer appearance-none bg-transparent nodrag text-right`} value={value ?? type[0]} onChange={e => onChange(name, e.target.value)}>{type.map((o: string) => <option key={o} value={o} className="bg-[var(--node-body)] text-[var(--text-val)]">{o}</option>)}</select></div>
      </div>
    );
  }
  return null;
};

// === 主节点组件 ===
const DynamicNode = ({ id, data, selected }: NodeProps<BrainFlowNodeData>) => {
  const { nodeSpec, values = {}, progress, message } = data;
  const { updateNodeData } = useFlow();
  const [collapsed, setCollapsed] = useState(false);

  const handleUpdate = useCallback((key: string, v: any) => updateNodeData(id, { values: { ...values, [key]: v } }), [id, values, updateNodeData]);

  const { linkInputs, widgets } = useMemo(() => {
    const links: any[] = [], wids: any[] = [];

    // 🔥🔥🔥 【核心修复】同时合并 required 和 optional 🔥🔥🔥
    const allInputs = {
        ...(nodeSpec?.input?.required || {}),
        ...(nodeSpec?.input?.optional || {})
    };

    if (allInputs) {
      Object.entries(allInputs).forEach(([name, config]) => {
        // 安全检查：防止 config 不存在
        if (!config) return;

        const [type] = config as [string, any];
        const isLink = !["INT", "FLOAT", "STRING", "BOOLEAN"].includes(type as string) && !Array.isArray(type);
        if (isLink) links.push({ name, type, color: TYPE_COLORS[type as string] || TYPE_COLORS.default });
        else wids.push({ name, config });
      });
    }
    return { linkInputs: links, widgets: wids };
  }, [nodeSpec]);

  if (!nodeSpec) return null;
  const isRunning = progress !== undefined && progress > 0 && progress < 100;
  const isComplete = progress === 100;
  const isError = message?.toLowerCase().includes('error');

  return (
    <>
      <NodeResizer
        color="#3b82f6"
        isVisible={selected}
        minWidth={220}
        minHeight={60}
        handleStyle={{ width: 6, height: 6, borderRadius: 2 }}
        lineStyle={{ border: 'none' }}
      />

      <div
        className={`
          node-wrapper relative rounded-[4px] shadow-[var(--node-shadow)] bg-[var(--node-body)] transition-all group h-full flex flex-col
          ${selected ? 'border-[#eee] ring-1 ring-[#eee]/30' : 'border-[var(--node-border)]'}
          ${isError ? 'border-red-500 shadow-[0_0_10px_rgba(239,68,68,0.3)]' : 'border'}
        `}
        onDoubleClick={() => setCollapsed(!collapsed)}
      >
        {/* 进度条 */}
        {isRunning && <div className="absolute top-0 left-0 h-full bg-green-500/5 pointer-events-none z-0 rounded-[4px]" style={{ width: `${progress}%` }} />}
        {isRunning && <div className="absolute top-0 left-0 h-0.5 bg-green-500 z-50 rounded-t-[4px] shadow-[0_0_8px_#22c55e]" style={{ width: `${progress}%` }} />}

        {/* Header */}
        <div className="relative h-6 px-2 flex items-center justify-between bg-[var(--node-header)] border-b border-[var(--node-border)] z-10 rounded-t-[4px] shrink-0 cursor-pointer" title="Double click to collapse">
           <span className="text-[11px] font-bold text-[var(--text-head)] truncate tracking-wide mr-2">{nodeSpec.display_name}</span>
           <div className="flex items-center gap-1.5">
             <div className={`w-1.5 h-1.5 rounded-full shadow-sm transition-colors duration-500 
                ${isError ? 'bg-red-500 animate-pulse' : isComplete ? 'bg-green-500' : isRunning ? 'bg-yellow-400' : 'bg-[#444]'}`}
             />
             <span className="text-[9px] text-[var(--text-sub)]">{collapsed ? '▼' : '▲'}</span>
           </div>
        </div>

        {/* Body */}
        <div className="relative p-1.5 space-y-1.5 z-10 flex-1 flex flex-col">
          {/* Inputs / Outputs */}
          <div className="flex justify-between gap-4">
            <div className="flex flex-col gap-1.5 flex-1 min-w-0">
              {linkInputs.map((input) => (
                <div key={input.name} className="relative h-3.5 flex items-center pl-2">
                  <Handle type="target" position={Position.Left} id={input.name} className="!w-2.5 !h-2.5 z-50 transition-transform hover:scale-110" style={{ backgroundColor: input.color, left: '-13px', border: '1px solid #000' }} />
                  <span className="text-[10px] text-[var(--text-label)] truncate leading-none">{input.name}</span>
                </div>
              ))}
            </div>

            <div className="flex flex-col gap-1.5 items-end flex-1 min-w-0">
              {nodeSpec.output && nodeSpec.output.map((outType: string, i: number) => (
                <div key={i} className="relative h-3.5 flex items-center justify-end pr-2">
                  <span className="text-[10px] text-[var(--text-label)] truncate uppercase leading-none">{outType}</span>
                  <Handle type="source" position={Position.Right} id={`${i}`} className="!w-2.5 !h-2.5 z-50 transition-transform hover:scale-110" style={{ backgroundColor: TYPE_COLORS[outType] || TYPE_COLORS.default, right: '-13px', border: '1px solid #000' }} />
                </div>
              ))}
            </div>
          </div>

          {/* Widgets */}
          {!collapsed && widgets.length > 0 && (
            <div className="pt-1.5 border-t border-[var(--node-border)] space-y-[2px] mt-1 flex-1 overflow-y-auto custom-scrollbar max-h-[300px]">
              {widgets.map((w) => <ControlWidget key={w.name} {...w} value={values[w.name]} onChange={handleUpdate} />)}
            </div>
          )}
        </div>

        {/* Message Bar */}
        {(isRunning || isComplete || isError) && message && !collapsed && (
          <div className={`px-2 py-0.5 border-t border-[var(--node-border)] rounded-b-[4px] ${isError ? 'bg-red-500/10' : 'bg-[var(--widget-bg)]/90'}`}>
              <p className={`text-[9px] font-mono truncate text-center ${isError ? 'text-red-500' : 'text-green-500'}`}>{message}</p>
          </div>
        )}
      </div>
    </>
  );
};
export default memo(DynamicNode);