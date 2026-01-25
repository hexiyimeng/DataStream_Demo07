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
// 1. 辅助组件：弹窗输入框 (用于 String/Path 编辑)
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
// 2. Fallback 组件：未知类型的最后防线
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
// 3. 组件注册表：未来扩展点
// ==========================================
interface ControlWidgetProps {
  name: string;
  config: [string, Record<string, unknown>?];
  value: unknown;
  onChange: (name: string, value: unknown) => void;
}

const WIDGET_REGISTRY: Record<string, React.ComponentType<ControlWidgetProps>> = {
  // 示例: "COLOR": ColorWidget
};


// ==========================================
// 4. 主控组件：ControlWidget
// ==========================================
// ==========================================
// 4. 主控组件：ControlWidget (完美版)
// ==========================================
const ControlWidget = ({ name, config, value, onChange }: ControlWidgetProps) => {
  const [type, options] = config;
  const [showPopup, setShowPopup] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const [anchorRect, setAnchorRect] = useState<DOMRect | null>(null);

  // 通用样式基础
  const containerClass = "nodrag group flex items-center justify-between bg-[var(--widget-bg)] hover:bg-[var(--widget-hover)] rounded-sm px-1.5 h-6 mb-[3px] transition-colors border border-transparent hover:border-[var(--node-border)] select-none";
  const labelClass = "text-[10px] text-[var(--text-label)] font-semibold mr-2 shrink-0 tracking-tight max-w-[50%] truncate cursor-default";
  const inputBgClass = "bg-[var(--widget-inner)] rounded-sm px-1 h-4 flex items-center min-w-0";
  const inputClass = "bg-transparent text-[10px] text-[var(--text-val)] text-right outline-none font-mono w-full h-full border-none focus:ring-0 p-0 leading-none truncate cursor-pointer";

  const handlePopupOpen = () => { if (containerRef.current) { setAnchorRect(containerRef.current.getBoundingClientRect()); } setShowPopup(true); };

  // ============================
  // A. Boolean (开关类型) 🔥 [NEW]
  // ============================
  if (type === 'BOOLEAN') {
      const boolVal = value === true || String(value).toLowerCase() === 'true';
      return (
        <div
          className={`${containerClass} cursor-pointer`}
          onClick={() => onChange(name, !boolVal)}
        >
           <span className={labelClass} title={name}>{name}</span>
           {/* iOS 风格的 Toggle 开关 */}
           <div className={`relative w-8 h-4 rounded-full transition-colors duration-200 ease-in-out ${boolVal ? 'bg-blue-500' : 'bg-gray-400/50'}`}>
              <div className={`absolute top-[2px] left-[2px] w-3 h-3 bg-white rounded-full shadow-sm transition-transform duration-200 ease-in-out ${boolVal ? 'translate-x-4' : 'translate-x-0'}`} />
           </div>
        </div>
      );
  }

  // ============================
  // B. String (文本/路径)
  // ============================
  // 逻辑净化：移除了对 'output_path' 等名称的硬编码判断，完全信赖 type
  if (type === 'STRING') {
     return (
        <>
          <div ref={containerRef} className={`${containerClass} cursor-text`} onClick={handlePopupOpen}>
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

  // ============================
  // C. Number (INT / FLOAT / LONG) 🔥 [NEW: LONG]
  // ============================
  if (type === 'INT' || type === 'FLOAT' || type === 'LONG') {
      const isFloat = type === 'FLOAT';
      // 如果是 LONG 或 INT，默认 step 为 1
      const step = Number(options?.step) || (isFloat ? 0.01 : 1);
      const val = Number(value ?? options?.default ?? 0);

      const handleStep = (direction: 1 | -1, shiftKey: boolean) => {
          // Shift 加速：整数 x10，浮点数 x10
          const multiplier = shiftKey ? 10 : 1;
          const numericVal = Number(val);
          const numericStep = Number(step);

          let newValue = numericVal + (numericStep * direction * multiplier);

          if (isFloat) {
             // 解决浮点数精度问题 (0.1 + 0.2 != 0.3)
             newValue = parseFloat(newValue.toFixed(5));
          } else {
             // 整数取整
             newValue = Math.round(newValue);
          }

          // 检查 min/max 限制 (如果有定义)
          if (options?.min !== undefined && newValue < (options.min as number)) newValue = options.min as number;
          if (options?.max !== undefined && newValue > (options.max as number)) newValue = options.max as number;

          onChange(name, newValue);
      };

      return (
        <div className={containerClass}>
           <span className={labelClass} title={name}>{name}</span>
           <div className="flex items-center gap-0.5 shrink-0 ml-auto">
              {/* 减小按钮 */}
              <button
                className="text-[var(--text-label)] hover:text-[var(--text-head)] hover:bg-black/10 dark:hover:bg-white/10 rounded cursor-pointer select-none px-1 transition-colors active:scale-90 flex items-center justify-center h-4 w-4"
                onClick={(e) => { e.stopPropagation(); handleStep(-1, e.shiftKey); }}
                onDoubleClick={(e) => e.stopPropagation()}
                title="Decrease (Shift for 10x)"
              >
                <svg className="w-2.5 h-2.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M15 19l-7-7 7-7" /></svg>
              </button>

              {/* 输入框 */}
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
                    onDoubleClick={(e) => e.stopPropagation()}
                    onPointerDown={(e) => e.stopPropagation()}
                    onKeyDown={(e) => e.stopPropagation()} // 防止触发全局快捷键
                  />
              </div>

              {/* 增加按钮 */}
              <button
                className="text-[var(--text-label)] hover:text-[var(--text-head)] hover:bg-black/10 dark:hover:bg-white/10 rounded cursor-pointer select-none px-1 transition-colors active:scale-90 flex items-center justify-center h-4 w-4"
                onClick={(e) => { e.stopPropagation(); handleStep(1, e.shiftKey); }}
                onDoubleClick={(e) => e.stopPropagation()}
                title="Increase (Shift for 10x)"
              >
                <svg className="w-2.5 h-2.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M9 5l7 7-7 7" /></svg>
              </button>
           </div>
        </div>
      );
  }

  // ============================
  // D. Dropdown (数组枚举)
  // ============================
  if (Array.isArray(type)) {
    return (
      <div className={containerClass}>
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

  // ============================
  // E. 扩展点 (注册表)
  // ============================
  const RegisteredWidget = WIDGET_REGISTRY[type as string];
  if (RegisteredWidget) {
    return <RegisteredWidget name={name} config={config} value={value} onChange={onChange} />;
  }

  // ============================
  // F. Fallback (未知类型)
  // ============================
  return <FallbackWidget name={name} value={value} type={type as string} onChange={onChange} />;
};


// ==========================================
// 5. 核心节点组件：DynamicNode (更新版)
// ==========================================
const DynamicNode = ({ id, data, selected }: NodeProps<Node<NodeData>>) => {
  const { nodeSpec, values = {}, progress, message } = data || {};

  // 获取连线类型，用于判断高亮
  const { updateNodeData, connectingType } = useFlow();
  const [collapsed, setCollapsed] = useState(false);

  // 使用 ref 来避免在回调依赖项中包含 values
  const valuesRef = useRef<Record<string, unknown>>(values);

  useLayoutEffect(() => {
    valuesRef.current = values;
  });

  const handleUpdate = useCallback((key: string, v: unknown) => {
    updateNodeData(id, { values: { ...valuesRef.current, [key]: v } });
  }, [id, updateNodeData]);

  const { linkInputs, widgets } = useMemo(() => {
    // 如果 nodeSpec 不存在，返回空数组而不是提前返回
    if (!nodeSpec) {
      return { linkInputs: [], widgets: [] };
    }

    const links: { name: string; type: string; color: string }[] = [];
    const wids: { name: string; config: [string, Record<string, unknown>?] }[] = [];

    const allInputs = { ...(nodeSpec?.input?.required || {}), ...(nodeSpec?.input?.optional || {}) };

    if (allInputs) {
      Object.entries(allInputs).forEach(([name, config]) => {
        if (!config) return;
        const rawType = config[0];
        const rawOptions = config[1];
        const isDropdown = Array.isArray(rawType);
        const hasConfigDict = rawOptions && typeof rawOptions === 'object';
        const isPrimitive = ["INT", "FLOAT", "STRING", "BOOLEAN"].includes(rawType as string);
        const isWidget = isDropdown || hasConfigDict || isPrimitive;

        if (isWidget) {
             wids.push({ name, config: config as [string, Record<string, unknown>?] });
        } else {
             links.push({
                name,
                type: rawType as string,
                color: TYPE_COLORS[rawType as string] || TYPE_COLORS.default
             });
        }
      });
    }
    return { linkInputs: links, widgets: wids };
  }, [nodeSpec]);


  const isRunning = progress !== undefined && progress > 0 && progress < 100;
  const isComplete = progress === 100;
  const isError = message?.toLowerCase().includes('error');

  return (
    <>
      <NodeResizer color="#3b82f6" isVisible={!!selected} minWidth={220} minHeight={60} handleStyle={{ width: 6, height: 6, borderRadius: 2 }} lineStyle={{ border: 'none' }} />
      <div
        className={`node-wrapper relative rounded-[4px] shadow-[var(--node-shadow)] bg-[var(--node-body)] transition-all group h-full flex flex-col ${selected ? 'border-[#eee] ring-1 ring-[#eee]/30' : 'border-[var(--node-border)]'} ${isError ? 'border-red-500 shadow-[0_0_10px_rgba(239,68,68,0.3)]' : 'border'}`}
        onDoubleClick={() => setCollapsed(!collapsed)}
      >
        {isRunning && (
            <div
                className="absolute inset-0 bg-green-500/10 pointer-events-none z-0 rounded-[4px] animate-pulse"
                style={{ clipPath: `polygon(0 0, ${progress}% 0, ${progress}% 100%, 0 100%)` }}
            />
        )}

        <div className="relative h-6 px-2 flex items-center justify-between bg-[var(--node-header)] border-b border-[var(--node-border)] z-10 rounded-t-[4px] shrink-0 cursor-pointer" title="Double click to collapse">
           <span className="text-[11px] font-bold text-[var(--text-head)] truncate tracking-wide mr-2">{nodeSpec.display_name}</span>
           <div className="flex items-center gap-1.5">
             <div className={`w-1.5 h-1.5 rounded-full shadow-sm transition-colors duration-500 ${isError ? 'bg-red-500 animate-pulse' : isComplete ? 'bg-green-500' : isRunning ? 'bg-yellow-400' : 'bg-[#444]'}`} />
             <span className="text-[9px] text-[var(--text-sub)]">{collapsed ? '▼' : '▲'}</span>
           </div>
        </div>

        <div className="relative p-1.5 space-y-1.5 z-10 flex-1 flex flex-col">
          <div className="flex justify-between gap-4">
            <div className="flex flex-col gap-1.5 flex-1 min-w-0">
              {linkInputs.map((input) => {
                // 计算样式
                let handleClass = "!w-2.5 !h-2.5 z-50 transition-all duration-300";
                const customStyle = { backgroundColor: input.color, left: '-13px', border: '1px solid #000' };

                if (connectingType) {
                   // 判断兼容性
                   const isCompatible = input.type === connectingType || input.type === "*" || connectingType === "*";
                   if (isCompatible) {
                      // 兼容：绿色高亮
                      handleClass += " bg-green-500 !w-3.5 !h-3.5 ring-4 ring-green-500/30 scale-110";
                      customStyle.backgroundColor = '#22c55e';
                   } else {
                      // 不兼容：变暗
                      handleClass += " opacity-20 scale-75 grayscale";
                      customStyle.backgroundColor = '#555';
                   }
                }

                return (
                  <div key={input.name} className={`relative h-3.5 flex items-center pl-2 transition-opacity ${connectingType && !(input.type === connectingType || input.type === "*") ? 'opacity-30' : 'opacity-100'}`}>
                    <Handle
                        type="target"
                        position={Position.Left}
                        id={input.name}
                        className={handleClass}
                        style={customStyle}
                    />
                    <span className="text-[10px] text-[var(--text-label)] truncate leading-none">{input.name}</span>
                  </div>
                );
              })}
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
          {!collapsed && widgets.length > 0 && (
            <div className="pt-1.5 border-t border-[var(--node-border)] space-y-[2px] mt-1 flex-1 overflow-y-auto custom-scrollbar max-h-[300px]">
              {widgets.map((w) => <ControlWidget key={w.name} {...w} value={values[w.name]} onChange={handleUpdate} />)}
            </div>
          )}
        </div>
        {(isRunning || message) && (
          <div className="relative mt-auto">
            {/* 进度条轨道 */}
            {isRunning && (
                <div className="h-1 bg-[var(--widget-bg)] w-full overflow-hidden">
                    <div
                        className="h-full bg-green-500 transition-all duration-300 ease-out shadow-[0_0_10px_rgba(34,197,94,0.5)]"
                        style={{ width: `${progress}%` }}
                    />
                </div>
            )}

            {/* 状态文字 */}
            <div className={`
                px-2 py-1 flex justify-between items-center text-[9px] font-mono border-t border-[var(--node-border)] rounded-b-[4px]
                ${isError ? 'bg-red-500/20 text-red-500' : 'bg-[var(--node-body)] text-[var(--text-sub)]'}
            `}>
                <span className="truncate max-w-[80%]" title={message}>
                    {message || (isRunning ? "Processing..." : "Ready")}
                </span>
                {isRunning && <span className="text-green-500 font-bold">{Math.floor(progress)}%</span>}
            </div>
          </div>
        )}
      </div>
    </>
  );
};
export default memo(DynamicNode);