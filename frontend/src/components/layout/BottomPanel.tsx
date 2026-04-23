// src/components/layout/BottomPanel.tsx
import { useEffect, useRef, useState, useCallback, useMemo } from 'react';
import { useFlow } from '../../hooks/useFlowContext';
import type { ExecutionPhase, WebSocketStatus } from '../../types';

function shortId(id: string | null): string {
  if (!id) return '—';
  return id.slice(0, 8);
}

function formatTime(ts: string | number) {
  if (!ts) return '--:--:--';
  const date = new Date(ts);
  if (isNaN(date.getTime())) return '--:--:--';
  return date.toLocaleTimeString([], { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

// === Status pill component ===
const StatusPill = ({ label, value, color }: { label: string; value: string; color: string }) => (
  <div className="flex items-center gap-1.5">
    <span className="text-[9px] uppercase tracking-wider font-mono opacity-50 shrink-0">{label}</span>
    <span className={`text-[10px] font-bold font-mono px-1.5 py-0.5 rounded-sm border ${color}`}>{value}</span>
  </div>
);

// === WebSocket status color ===
function wsStatusColor(s: WebSocketStatus): string {
  if (s === 'connected') return 'text-green-500 border-green-500/40 bg-green-500/10';
  if (s === 'reconnecting') return 'text-yellow-500 border-yellow-500/40 bg-yellow-500/10';
  return 'text-red-500 border-red-500/40 bg-red-500/10';
}

// === Execution phase color & label ===
function phaseLabel(p: ExecutionPhase): string {
  const map: Record<ExecutionPhase, string> = {
    idle: 'Idle',
    graph_building: 'Building...',
    submitted: 'Submitted',
    running: 'Running',
    cancelling: 'Cancelling',
    succeeded: 'Done',
    failed: 'Failed',
    cancelled: 'Cancelled',
  };
  return map[p] ?? p;
}

function phaseColor(p: ExecutionPhase): string {
  const map: Record<string, string> = {
    idle: 'text-[var(--text-sub)] border-[var(--node-border)] bg-transparent',
    graph_building: 'text-blue-400 border-blue-400/40 bg-blue-400/10',
    submitted: 'text-blue-400 border-blue-400/40 bg-blue-400/10',
    running: 'text-yellow-400 border-yellow-400/40 bg-yellow-400/10',
    cancelling: 'text-orange-400 border-orange-400/40 bg-orange-400/10',
    succeeded: 'text-green-400 border-green-400/40 bg-green-400/10',
    failed: 'text-red-400 border-red-400/40 bg-red-400/10',
    cancelled: 'text-gray-400 border-gray-400/40 bg-gray-400/10',
  };
  return map[p] ?? 'text-[var(--text-sub)] border-[var(--node-border)] bg-transparent';
}

// === Main component ===
export default function BottomPanel() {
  const {
    logs, clearLogs, isConsoleOpen, toggleConsole,
    executionState, websocketStatus, isExecuting, isCancelling, stopFlow,
  } = useFlow();

  // Auto-expand on reconnecting when execution is active
  useEffect(() => {
    if (websocketStatus === 'reconnecting' && executionState.executionId && !isConsoleOpen) {
      toggleConsole(); // expand panel when reconnecting with active execution
    }
  }, [websocketStatus, executionState.executionId, isConsoleOpen, toggleConsole]);

  const endRef = useRef<HTMLDivElement>(null);
  const [height, setHeight] = useState(192);
  const isDragging = useRef(false);

  // Auto-scroll logs
  useEffect(() => {
    if (isConsoleOpen) endRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [logs, isConsoleOpen]);

  // Drag-to-resize
  const startResizing = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    isDragging.current = true;
    document.body.style.cursor = 'row-resize';
    document.body.style.userSelect = 'none';
  }, []);

  const stopResizing = useCallback(() => {
    isDragging.current = false;
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
  }, []);

  const resize = useCallback((e: MouseEvent) => {
    if (isDragging.current) {
      const newHeight = window.innerHeight - e.clientY;
      setHeight(Math.max(32, Math.min(newHeight, window.innerHeight * 0.8)));
    }
  }, []);

  useEffect(() => {
    window.addEventListener('mousemove', resize);
    window.addEventListener('mouseup', stopResizing);
    return () => {
      window.removeEventListener('mousemove', resize);
      window.removeEventListener('mouseup', stopResizing);
    };
  }, [resize, stopResizing]);

  // Derived display values
  const phase = executionState.phase;
  const execId = executionState.executionId;
  const mainMessage = useMemo(() => {
    if (phase === 'idle') return 'Ready';
    if (phase === 'graph_building') return 'Building Graph...';
    if (phase === 'submitted') return 'Submitted';
    if (phase === 'running') {
      if (executionState.totalChunks > 0) {
        return `${executionState.processedChunks}/${executionState.totalChunks} chunks`;
      }
      return 'Running';
    }
    if (phase === 'cancelling') return 'Cancelling...';
    if (phase === 'succeeded') return 'Finished Successfully';
    if (phase === 'failed') {
      // Show error reason more clearly when failed
      const err = executionState.lastError;
      return err ? `Failed: ${err.slice(0, 80)}` : 'Failed';
    }
    if (phase === 'cancelled') return 'Cancelled';
    return phase;
  }, [phase, executionState]);

  // Has chunk stats
  const hasChunkStats = executionState.totalChunks > 0;
  const chunkSummary = hasChunkStats
    ? `${executionState.processedChunks}/${executionState.totalChunks}`
    : null;

  return (
    <div
      className={`
        absolute bottom-0 left-0 w-full z-40 flex flex-col transition-none ease-in-out
        border-t border-[var(--console-border)]
        bg-[var(--console-bg)] text-[var(--console-text)]
        shadow-[0_-4px_20px_rgba(0,0,0,0.1)]
      `}
      style={{ height: isConsoleOpen ? height : 48 }}
    >
      {/* Drag handle (only when open) */}
      {isConsoleOpen && (
        <div
          onMouseDown={startResizing}
          className="absolute top-0 left-0 w-full h-1 cursor-row-resize hover:bg-blue-500/50 z-50 -mt-0.5"
        />
      )}

      {/* ===== Summary bar (always visible) ===== */}
      <div
        className="h-12 border-b border-[var(--console-border)] flex items-center justify-between px-4 select-none shrink-0"
        onClick={toggleConsole}
      >
        <div className="flex items-center gap-4 flex-1 min-w-0">

          {/* Collapse chevron */}
          <div className={`transition-transform duration-200 ${isConsoleOpen ? 'rotate-0' : '-rotate-90'}`}>
            ▼
          </div>

          {/* WebSocket status */}
          <StatusPill
            label="WS"
            value={websocketStatus === 'connected' ? 'Connected' : websocketStatus === 'reconnecting' ? 'Reconnecting' : 'Disconnected'}
            color={wsStatusColor(websocketStatus)}
          />

          {/* Execution phase */}
          <StatusPill
            label="EXEC"
            value={phaseLabel(phase)}
            color={phaseColor(phase)}
          />

          {/* executionId */}
          {execId && (
            <StatusPill
              label="ID"
              value={shortId(execId)}
              color="text-cyan-400 border-cyan-400/40 bg-cyan-400/10"
            />
          )}

          {/* Main message */}
          <span className="text-[11px] font-mono text-[var(--console-text)] truncate">
            {mainMessage}
          </span>

          {/* Chunk stats */}
          {chunkSummary && (
            <span className="text-[10px] font-mono text-green-400 shrink-0">
              ✓ {chunkSummary}
            </span>
          )}

          {/* Inference/skipped/failed breakdown */}
          {hasChunkStats && executionState.completedInferenceChunks > 0 && (
            <span className="text-[9px] font-mono text-[var(--text-sub)] shrink-0">
              inf={executionState.completedInferenceChunks}
              {executionState.skippedChunks > 0 && ` skip=${executionState.skippedChunks}`}
              {executionState.failedChunks > 0 && ` fail=${executionState.failedChunks}`}
            </span>
          )}
        </div>

        {/* Buttons */}
        <div className="flex items-center gap-2 shrink-0">
          {/* Stop button — only when executing or cancelling */}
          {(isExecuting || isCancelling) && (
            <button
              onClick={(e) => { e.stopPropagation(); stopFlow(); }}
              className="text-[10px] px-2 py-1 rounded font-bold border border-red-500/50 text-red-400 hover:bg-red-500/20 transition-all uppercase"
            >
              Stop
            </button>
          )}

          <button
            onClick={(e) => { e.stopPropagation(); clearLogs(); }}
            className="text-[10px] px-2 py-1 rounded hover:bg-[var(--widget-bg)] opacity-60 hover:opacity-100 transition-all uppercase font-semibold border border-transparent hover:border-[var(--console-border)]"
          >
            Clear
          </button>
        </div>
      </div>

      {/* ===== Log area (expanded) ===== */}
      {isConsoleOpen && (
        <div className="flex-1 overflow-y-auto p-2 font-mono text-[11px] leading-relaxed custom-scrollbar select-text bg-[var(--console-bg)]">
          {logs.length === 0 ? (
            <div className="opacity-40 p-2 italic">{'>'} System ready... Waiting for workflow execution.</div>
          ) : (
            logs.map((log) => (
              <div key={log.id} className="flex gap-3 py-0.5 hover:bg-[var(--widget-bg)] px-2 rounded-sm border-l-2 border-transparent hover:border-blue-500/30 transition-colors">
                <span className="opacity-40 w-16 shrink-0 select-none text-right">
                  {formatTime(log.timestamp)}
                </span>
                <span className={`flex-1 break-all tracking-tight
                  ${log.type === 'error' ? 'text-red-500 font-bold' : ''}
                  ${log.type === 'success' ? 'text-green-600 dark:text-green-400 font-bold' : ''}
                  ${log.type === 'warning' ? 'text-amber-500' : ''}
                  ${log.type === 'info' ? 'text-[var(--console-text)]' : ''}
                `}>
                  <span className="opacity-50 mr-2 select-none">
                    {log.type === 'error' && '✗'}
                    {log.type === 'success' && '✓'}
                    {log.type === 'warning' && '⚠'}
                    {log.type === 'info' && '>'}
                  </span>
                  {log.message}
                </span>
              </div>
            ))
          )}
          <div ref={endRef} />
        </div>
      )}
    </div>
  );
}
