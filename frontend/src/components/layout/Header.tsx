import { useRef, type ChangeEvent } from 'react';
import { useReactFlow, getNodesBounds, getViewportForBounds } from '@xyflow/react';
import { useFlow } from '../../hooks/useFlowContext';

const SunIcon = () => <span>☀️</span>;
const MoonIcon = () => <span>🌙</span>;
const LoadIcon = () => <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12" /></svg>;
const SaveIcon = () => <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 7H5a2 2 0 00-2 2v9a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-3m-1 4l-3 3m0 0l-3-3m3 3V4" /></svg>;
const PlusIcon = () => <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" /></svg>;
const XIcon = () => <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" /></svg>;

export default function Header() {
  const {
    theme, toggleTheme,
    workflows, activeWorkflowId,
    createWorkflow, switchWorkflow, deleteWorkflow, renameWorkflow,
    runFlow, setNodes, setEdges
  } = useFlow();

  const reactFlowInstance = useReactFlow();
  const fileInputRef = useRef<HTMLInputElement>(null);

  // === 1. 保存逻辑 (自动使用当前标签名) ===
  const handleSave = () => {
    if (!reactFlowInstance) return;

    const currentFlow = workflows.find(w => w.id === activeWorkflowId);
    const defaultName = currentFlow ? currentFlow.name : `workflow_${Date.now()}`;

    const fileName = prompt("保存工作流为:", defaultName);
    if (fileName === null) return;

    const finalName = fileName.trim() || defaultName;
    const flowData = reactFlowInstance.toObject();

    const exportData = {
      ...flowData,
      workflow_name: finalName,
      timestamp: Date.now()
    };

    const blob = new Blob([JSON.stringify(exportData, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);

    const link = document.createElement('a');
    link.href = url;
    link.download = `${finalName}.json`;
    document.body.appendChild(link);
    link.click();

    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };



    // === 修改 handleLoad 函数 ===
    const handleLoad = (e: ChangeEvent<HTMLInputElement>) => {
      const file = e.target.files?.[0];
      if (!file) return;

      const reader = new FileReader();
      reader.onload = (event) => {
        try {
          const flow = JSON.parse(event.target?.result as string);

          // 1. 基础验证
          if (!Array.isArray(flow.nodes) || !Array.isArray(flow.edges)) {
            alert("文件格式错误：缺少 nodes 或 edges");
            return;
          }

          // 2. 设置状态 (先设为空，防止 ID 冲突，可选)
          setNodes([]);
          setEdges([]);

          // 3. 延迟一下再设置新数据，或者直接设置
          // 注意：这里建议把 viewport 操作放在 requestAnimationFrame 或 setTimeout 里
          setTimeout(() => {
              setNodes(flow.nodes);
              setEdges(flow.edges);

              // 4. 安全地重命名 (加个判断)
              if (flow.workflow_name && activeWorkflowId) {
                 try {
                    renameWorkflow(activeWorkflowId, flow.workflow_name);
                 } catch (err) {
                    console.warn("重命名工作流失败，但不影响加载:", err);
                 }
              }

              // 5. 调整视图
              const bounds = getNodesBounds(flow.nodes);
              // 注意：如果 bounds 无效(比如节点还没渲染)，viewport 可能会计算出 NaN
              if (bounds && bounds.width > 0) {
                 const { x, y, zoom } = getViewportForBounds(bounds, window.innerWidth, window.innerHeight, 0.1, 2);
                 reactFlowInstance.setViewport({ x, y, zoom });
              }
          }, 0);

        } catch (error) {
          // 关键：打印出真正的错误对象，不要只弹窗
          console.error("加载工作流详细错误: ", error);
          alert(`无法加载工作流: ${(error as Error).message}`);
        }
      };
      reader.readAsText(file);
      e.target.value = '';
    };

  return (
    <header className="h-12 bg-[var(--node-header)] border-b border-[var(--node-border)] flex items-center justify-between px-2 z-30 transition-colors duration-300 select-none">

      <input type="file" ref={fileInputRef} onChange={handleLoad} accept=".json" className="hidden" />

      {/* 1. Logo 区域 */}
      <div className="flex items-center gap-2 w-48 pl-2">
         <div className="w-5 h-5 bg-blue-600 rounded flex items-center justify-center text-white font-bold text-[10px] shadow-sm">
            BF
         </div>
         <span className="text-sm font-bold text-[var(--text-head)] tracking-tight">
           Brain<span className="text-blue-500">Flow</span>
         </span>
      </div>

      {/* 2. 中间：工作流 Tabs (Chrome 风格) - 已修复滚动条问题 */}
      <div className="
        flex-1 flex items-end justify-start h-full px-4 gap-1
        overflow-x-auto overflow-y-hidden
        [&::-webkit-scrollbar]:hidden [-ms-overflow-style:'none'] [scrollbar-width:'none']
      ">
         {workflows.map(wf => {
           const isActive = wf.id === activeWorkflowId;
           return (
             <div
               key={wf.id}
               onClick={() => switchWorkflow(wf.id)}
               onDoubleClick={() => {
                 const newName = prompt("重命名工作流:", wf.name);
                 if (newName) renameWorkflow(wf.id, newName.trim());
               }}
               className={`
                 group relative flex items-center gap-2 px-3 h-9 rounded-t-lg transition-all cursor-pointer min-w-[120px] max-w-[200px] border-t border-x
                 ${isActive 
                   ? 'bg-[var(--bg-canvas)] border-[var(--node-border)] text-[var(--text-head)] font-medium z-10' 
                   : 'bg-[var(--bg-node)] border-transparent text-[var(--text-label)] hover:bg-[var(--widget-hover)] opacity-80 hover:opacity-100 mt-1 h-8'}
               `}
             >
               <span className="truncate text-xs flex-1">{wf.name}</span>

               {workflows.length > 1 && (
                 <button
                   onClick={(e) => {
                     e.stopPropagation();
                     if(confirm(`确定要删除 "${wf.name}" 吗?`)) deleteWorkflow(wf.id);
                   }}
                   className={`w-4 h-4 rounded-full flex items-center justify-center hover:bg-red-500/20 hover:text-red-500 opacity-0 group-hover:opacity-100 transition-opacity`}
                 >
                   <XIcon />
                 </button>
               )}

               {isActive && <div className="absolute -bottom-[1px] left-0 w-full h-[1px] bg-[var(--bg-canvas)] z-20"></div>}
             </div>
           );
         })}

         <button
           onClick={createWorkflow}
           className="h-8 w-8 mb-0.5 flex items-center justify-center text-[var(--text-sub)] hover:text-[var(--text-head)] hover:bg-[var(--widget-bg)] rounded-full transition-colors shrink-0"
           title="New Workflow"
         >
           <PlusIcon />
         </button>
      </div>

      {/* 3. 右侧：功能区 */}
      <div className="flex items-center gap-2 w-fit justify-end pr-2">
        <button
          onClick={toggleTheme}
          className="w-7 h-7 rounded bg-[var(--widget-bg)] hover:bg-[var(--node-body)] text-[var(--text-sub)] flex items-center justify-center transition-colors"
        >
          {theme === 'dark' ? <MoonIcon /> : <SunIcon />}
        </button>

        <div className="h-4 w-[1px] bg-[var(--node-border)] mx-1"></div>

        <button
             onClick={() => fileInputRef.current?.click()}
             className="px-3 py-1.5 text-xs font-medium text-[var(--text-head)] hover:bg-[var(--widget-bg)] rounded transition-colors flex items-center gap-1"
           >
             <LoadIcon /> Load
        </button>
        <button
             onClick={handleSave}
             className="px-3 py-1.5 text-xs font-medium text-[var(--text-head)] hover:bg-[var(--widget-bg)] rounded transition-colors flex items-center gap-1"
           >
             <SaveIcon /> Save
        </button>

        <button
          onClick={runFlow}
          className="ml-2 bg-blue-600 hover:bg-blue-500 text-white text-xs font-bold px-4 py-1.5 rounded shadow-sm transition-all active:scale-95"
        >
          QUEUE
        </button>
      </div>
    </header>
  );
}