// React 基础 hooks 和类型定义
import { useState, useCallback, type ComponentType } from 'react';
import {
  ReactFlow,
  Background,
  MiniMap,
  Panel,
  useReactFlow,
  useViewport,
  BackgroundVariant,
  ConnectionMode,
  type NodeTypes
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { useFlow } from '../../hooks/useFlowContext';
import DynamicNode from '../DynamicNode';
import ContextMenu from './ContextMenu'; // 【新增】




// 注册动态节点组件到 React Flow
// 将 DynamicNode 转换为 ComponentType<any> 类型以兼容 React Flow 的节点类型要求
const nodeTypes: NodeTypes = { dynamic: DynamicNode as ComponentType<any> };

// ... (FitIcon, MapIcon 等图标组件保持不变)
const FitIcon = () => <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 8V4m0 0h4M4 4l5 5m11-1V4m0 0h-4m4 0l-5 5M4 16v4m0 0h4m-4 0l5-5m11 5l-5-5m5 5v-4m0 4h-4" /></svg>;
const MapIcon = () => <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 20l-5.447-2.724A1 1 0 013 16.382V5.618a1 1 0 011.447-.894L9 7m0 13l6-3m-6 3V7m6 10l4.553 2.276A1 1 0 0021 18.382V7.618a1 1 0 00-.553-.894L15 4m0 13V4m0 0L9 7" /></svg>;
const ZoomInIcon = () => <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6v6m0 0v6m0-6h6m-6 0H6" /></svg>;
const ZoomOutIcon = () => <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M20 12H4" /></svg>;

function BottomToolbar() {
  // ... (保持不变)
  const { fitView, zoomIn, zoomOut } = useReactFlow();
  const { zoom } = useViewport();
  const [showMap, setShowMap] = useState(true);
  return (
    <div className="flex flex-col items-end gap-2 pointer-events-auto">
      <div className={`transition-all duration-300 origin-bottom-right overflow-hidden rounded-lg shadow-2xl border border-[var(--node-border)] bg-[var(--bg-canvas)] ${showMap ? 'w-52 h-36 opacity-100 mb-1' : 'w-0 h-0 opacity-0'}`}>
        <MiniMap className="!relative !m-0 !w-full !h-full !bg-transparent" nodeColor="#3b82f6" maskColor="rgba(0, 0, 0, 0.3)" zoomable pannable />
      </div>
      <div className="flex items-center p-1 gap-1 rounded-lg bg-[var(--node-header)] border border-[var(--node-border)] shadow-xl">
        <button onClick={() => setShowMap(!showMap)} className={`p-2 rounded hover:bg-[var(--widget-hover)] transition-colors ${showMap ? 'text-blue-500' : 'text-[var(--text-sub)]'}`}><MapIcon /></button>
        <div className="w-[1px] h-4 bg-[var(--node-border)] mx-0.5"></div>
        <button onClick={() => zoomOut()} className="p-2 text-[var(--text-sub)] hover:text-[var(--text-head)] hover:bg-[var(--widget-hover)] rounded transition-colors"><ZoomOutIcon /></button>
        <span className="text-[10px] font-mono font-medium text-[var(--text-sub)] min-w-[3rem] text-center cursor-pointer hover:text-[var(--text-head)] select-none" onClick={() => fitView({ duration: 500 })}>{(zoom * 100).toFixed(0)}%</span>
        <button onClick={() => zoomIn()} className="p-2 text-[var(--text-sub)] hover:text-[var(--text-head)] hover:bg-[var(--widget-hover)] rounded transition-colors"><ZoomInIcon /></button>
        <div className="w-[1px] h-4 bg-[var(--node-border)] mx-0.5"></div>
        <button onClick={() => fitView({ duration: 500 })} className="p-2 text-[var(--text-sub)] hover:text-[var(--text-head)] hover:bg-[var(--widget-hover)] rounded transition-colors"><FitIcon /></button>
      </div>
    </div>
  );
}


// 主要的工作流编辑器组件，提供可视化节点编辑界面
export default function FlowEditor() {
  const { nodes, edges, onNodesChange, onEdgesChange, onConnect, theme, isConsoleOpen, addNodeAt ,isValidConnection} = useFlow();

  // 获取 React Flow 实例用于坐标转换
  const { screenToFlowPosition } = useReactFlow();

  // 定义连线时的样式
  const connectionLineStyle = {
    stroke: '#fff',
    strokeWidth: 3,
  };
  // 菜单状态
  const [menu, setMenu] = useState<{ x: number, y: number, flowPos: {x:number, y:number} } | null>(null);

  const onPaneContextMenu = useCallback(
    (event: React.MouseEvent) => {
      event.preventDefault(); // 阻止默认浏览器菜单

      // 1. 获取点击时的屏幕坐标 (用于 ContextMenu CSS 定位)
      const screenX = event.clientX;
      const screenY = event.clientY;

      // 2. 获取点击时对应的画布坐标 (用于 addNodeAt 放置节点)
      // screenToFlowPosition 会自动计算 Zoom 和 Pan
      const flowPos = screenToFlowPosition({ x: screenX, y: screenY });

      setMenu({ x: screenX, y: screenY, flowPos });
    },
    [screenToFlowPosition]
  );

  const onPaneClick = useCallback(() => setMenu(null), []);

  const defaultEdgeOptions = {
    type: 'default',
    animated: true,
    style: { strokeWidth: 3, stroke: '#555' }
};




  return (
    <div className="flex-1 h-full w-full bg-[var(--bg-canvas)] relative transition-colors duration-300">
      <ReactFlow
        nodes={nodes} edges={edges}
        onNodesChange={onNodesChange} onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        nodeTypes={nodeTypes}
        defaultEdgeOptions={defaultEdgeOptions}
        connectionMode={ConnectionMode.Loose}
        defaultViewport={{ x: 0, y: 0, zoom: 0.8 }}
        isValidConnection={isValidConnection}
        connectionLineStyle={connectionLineStyle}
        selectNodesOnDrag={false}
        panOnScroll={true} zoomOnScroll={true} panOnDrag={[0, 1, 2]} selectionOnDrag={false}
        minZoom={0.1} maxZoom={3}
        colorMode={theme === 'dark' ? 'dark' : 'light'}
        // 【关键】绑定右键和点击事件
        onNodeContextMenu={onPaneContextMenu}
        onPaneClick={onPaneClick}
      >
        <Background id="grid-minor" className="pointer-events-none" color="var(--bg-grid-minor)" variant={BackgroundVariant.Lines} gap={10} size={1} lineWidth={1} style={{ backgroundColor: 'var(--bg-canvas)' }} />
        <Background id="grid-major" className="pointer-events-none" color="var(--bg-grid-major)" variant={BackgroundVariant.Lines} gap={100} size={1} lineWidth={2} style={{ backgroundColor: 'transparent' }} />

        <Panel
          position="bottom-right"
          className={`mr-4 z-50 transition-all duration-300 ease-in-out ${isConsoleOpen ? 'mb-52' : 'mb-12'}`}
        >
           <BottomToolbar />
        </Panel>
      </ReactFlow>

      {/* 渲染右键菜单 */}
      {menu && (
        <ContextMenu
          top={menu.y}
          left={menu.x}
          onClose={() => setMenu(null)}
          onAddNode={(type) => {
            // 使用之前计算好的画布坐标添加节点
            addNodeAt(type, menu.flowPos);
          }}
        />
      )}
    </div>
  );
}