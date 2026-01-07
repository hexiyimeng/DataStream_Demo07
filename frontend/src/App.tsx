import { ReactFlowProvider } from '@xyflow/react'; // 【新增】
import Header from './components/layout/Header';
import Sidebar from './components/layout/Sidebar';
import BottomPanel from './components/layout/BottomPanel';
import FlowEditor from './components/flow/FlowEditor';

export default function App() {
  return (
    // 【关键修复】必须包裹 ReactFlowProvider，否则 useReactFlow 无法在子组件使用
    <ReactFlowProvider>
      <div className="w-screen h-screen flex flex-col bg-[var(--bg-canvas)] text-[var(--text-primary)] overflow-hidden font-sans transition-colors duration-300">
        <Header />
        <div className="flex-1 flex overflow-hidden relative">
          <Sidebar />
          <main className="flex-1 relative flex flex-col bg-[var(--bg-canvas)] overflow-hidden">
            <FlowEditor />
            <BottomPanel />
          </main>
        </div>
      </div>
    </ReactFlowProvider>
  );
}