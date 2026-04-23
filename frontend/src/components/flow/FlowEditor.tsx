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
import ContextMenu from './ContextMenu';

// 注册动态节点
const nodeTypes: NodeTypes = { dynamic: DynamicNode as ComponentType<any> };

// 图标组件
const FitIcon = () => <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 8V4m0 0h4M4 4l5 5m11-1V4m0 0h-4m4 0l-5 5M4 16v4m0 0h4m-4 0l5-5m11 5l-5-5m5 5v-4m0 4h-4" /></svg>;
const MapIcon = () => <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 20l-5.447-2.724A1 1 0 013 16.382V5.618a1 1 0 011.447-.894L9 7m0 13l6-3m-6 3V7m6 10l4.553 2.276A1 1 0 0021 18.382V7.618a1 1 0 00-.553-.894L15 4m0 13V4m0 0L9 7" /></svg>;
const ZoomInIcon = () => <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6v6m0 0v6m0-6h6m-6 0H6" /></svg>;
const ZoomOutIcon = () => <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M20 12H4" /></svg>;

function BottomToolbar() {
  const { fitView, zoomIn, zoomOut } = useReactFlow();
  const { zoom } = useViewport();
  const [showMap, setShowMap] = useState(false);
  return (
    <div className="flex flex-col items-end gap-2 pointer-events-auto">
      <div className={`transition-all duration-300 origin-bottom-right overflow-hidden rounded border border-[var(--node-border)] bg-[var(--bg-canvas)] ${showMap ? 'w-52 h-36 opacity-100 mb-1' : 'w-0 h-0 opacity-0'}`}>
        <MiniMap className="!relative !m-0 !w-full !h-full !bg-transparent" nodeColor="#3b82f6" maskColor="rgba(0, 0, 0, 0.3)" zoomable pannable />
      </div>
      <div className="flex items-center p-0.5 gap-0.5 rounded bg-[var(--node-header)] border border-[var(--node-border)] shadow-sm opacity-70 hover:opacity-100 transition-opacity">
        <button onClick={() => setShowMap(!showMap)} className={`p-1.5 rounded hover:bg-[var(--widget-hover)] transition-colors ${showMap ? 'text-blue-500' : 'text-[var(--text-sub)]'}`}><MapIcon /></button>
        <div className="w-[1px] h-3 bg-[var(--node-border)] mx-0.5"></div>
        <button onClick={() => zoomOut()} className="p-1.5 text-[var(--text-sub)] hover:text-[var(--text-head)] hover:bg-[var(--widget-hover)] rounded transition-colors"><ZoomOutIcon /></button>
        <span className="text-[10px] font-mono font-medium text-[var(--text-sub)] min-w-[2.5rem] text-center cursor-pointer hover:text-[var(--text-head)] select-none" onClick={() => fitView({ duration: 300 })}>{(zoom * 100).toFixed(0)}%</span>
        <button onClick={() => zoomIn()} className="p-1.5 text-[var(--text-sub)] hover:text-[var(--text-head)] hover:bg-[var(--widget-hover)] rounded transition-colors"><ZoomInIcon /></button>
        <div className="w-[1px] h-3 bg-[var(--node-border)] mx-0.5"></div>
        <button onClick={() => fitView({ duration: 300 })} className="p-1.5 text-[var(--text-sub)] hover:text-[var(--text-head)] hover:bg-[var(--widget-hover)] rounded transition-colors"><FitIcon /></button>
      </div>
    </div>
  );
}

export default function FlowEditor() {
  const {
      nodes, edges,
      onNodesChange, onEdgesChange,
      onConnect,
      theme,
      isConsoleOpen,
      addNodeAt,
      isValidConnection,
      // 引入新增的事件回调
      onConnectStart, onConnectEnd
  } = useFlow();

  const { screenToFlowPosition } = useReactFlow();

  // 连线时的样式
  const connectionLineStyle = {
    stroke: '#fff',
    strokeWidth: 3,
  };

  // 右键菜单位置状态
  const [menu, setMenu] = useState<{ x: number, y: number, flowPos: {x:number, y:number} } | null>(null);

  const onPaneContextMenu = useCallback(
    (event: React.MouseEvent<Element> | globalThis.MouseEvent) => {
      event.preventDefault();
      const nativeEvent = (event as React.MouseEvent).nativeEvent || event;
      const screenX = nativeEvent.clientX;
      const screenY = nativeEvent.clientY;

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

  // 处理拖拽放置
  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const onDrop = useCallback(
    (event: React.DragEvent) => {
      event.preventDefault();

      const type = event.dataTransfer.getData('application/reactflow');
      if (typeof type === 'undefined' || !type) {
        return;
      }

      const position = screenToFlowPosition({
        x: event.clientX,
        y: event.clientY,
      });

      addNodeAt(type, position);
    },
    [screenToFlowPosition, addNodeAt],
  );

  return (
    <div
        className="flex-1 h-full w-full bg-[var(--bg-canvas)] relative transition-colors duration-300"
        onDrop={onDrop}
        onDragOver={onDragOver}
    >
      <ReactFlow
        nodes={nodes} edges={edges}
        onNodesChange={onNodesChange} onEdgesChange={onEdgesChange}
        onConnect={onConnect}

        // 绑定连线开始/结束事件
        onConnectStart={onConnectStart}
        onConnectEnd={onConnectEnd}

        nodeTypes={nodeTypes}
        defaultEdgeOptions={defaultEdgeOptions}
        connectionMode={ConnectionMode.Loose}
        defaultViewport={{ x: 0, y: 0, zoom: 1.0 }}
        isValidConnection={isValidConnection}
        connectionLineStyle={connectionLineStyle}
        selectNodesOnDrag={false}
        panOnScroll={true} zoomOnScroll={true} panOnDrag={[0, 1, 2]} selectionOnDrag={false}
        minZoom={0.1} maxZoom={3}
        colorMode={theme === 'dark' ? 'dark' : 'light'}

        onNodeContextMenu={onPaneContextMenu}
        onPaneContextMenu={onPaneContextMenu}
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
            addNodeAt(type, menu.flowPos);
          }}
        />
      )}
    </div>
  );
}