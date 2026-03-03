import React from 'react'
import ReactDOM from 'react-dom/client'
import { ReactFlowProvider } from '@xyflow/react';
import { FlowProvider } from './context/FlowContext'
import App from './App'
import './index.css'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <ReactFlowProvider>
      <FlowProvider>
        <App />
      </FlowProvider>
    </ReactFlowProvider>
  </React.StrictMode>,
)