import React from 'react'
import ReactDOM from 'react-dom/client'
import { ReactFlowProvider } from '@xyflow/react';
import { FlowProvider } from './context/FlowContext'
import App from './App'
import './index.css' // <--- 这一行绝对不能少！！

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <ReactFlowProvider>
      <FlowProvider>
        <App />
      </FlowProvider>
    </ReactFlowProvider>
  </React.StrictMode>,
)