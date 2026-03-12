/**
 * 测试前端环境变量加载
 *
 * 使用方法:
 * 1. 在 frontend 目录运行: node test_env_vars.js
 * 2. 或在构建前运行
 */

// 模拟 Vite 的环境变量（在 Node.js 环境中）
const mockEnv = {
  VITE_API_BASE_URL: process.env.VITE_API_BASE_URL || 'http://localhost:8000',
  VITE_WS_URL: process.env.VITE_WS_URL || 'ws://localhost:8000',
};

console.log("=== 测试前端环境变量 ===");

// 测试 1: 检查环境变量默认值
console.log("\n1. 环境变量值:");
console.log(`   VITE_API_BASE_URL: ${process.env.VITE_API_BASE_URL || '(未设置，使用默认值)'}`);
console.log(`   实际使用的 API URL: ${mockEnv.VITE_API_BASE_URL}`);
console.log(`   VITE_WS_URL: ${process.env.VITE_WS_URL || '(未设置，使用默认值)'}`);
console.log(`   实际使用的 WS URL: ${mockEnv.VITE_WS_URL}`);

// 测试 2: 验证默认值
const expectedDefaults = {
  VITE_API_BASE_URL: 'http://localhost:8000',
  VITE_WS_URL: 'ws://localhost:8000',
};

console.log("\n2. 默认值验证:");
for (const [key, expected] of Object.entries(expectedDefaults)) {
  if (!process.env[key]) {
    console.log(`   ✅ ${key} 使用默认值: ${expected}`);
  } else {
    console.log(`   ℹ️  ${key} 被覆盖: ${process.env[key]}`);
  }
}

// 测试 3: 模拟 useFlowEngine 中的用法
console.log("\n3. 模拟实际使用场景:");

function mockFetchObjectInfo() {
  const url = `${mockEnv.VITE_API_BASE_URL}/object_info`;
  console.log(`   fetch URL: ${url}`);
  return url;
}

function mockWebSocketConnect() {
  const url = `${mockEnv.VITE_WS_URL}/ws/run`;
  console.log(`   WebSocket URL: ${url}`);
  return url;
}

mockFetchObjectInfo();
mockWebSocketConnect();

// 测试 4: 检查 .env.example 文件
const fs = require('fs');
const path = require('path');

const envExamplePath = path.join(__dirname, '.env.example');
if (fs.existsSync(envExamplePath)) {
  console.log("\n4. .env.example 文件检查:");
  const content = fs.readFileSync(envExamplePath, 'utf-8');
  const hasApiUrl = content.includes('VITE_API_BASE_URL');
  const hasWsUrl = content.includes('VITE_WS_URL');

  console.log(`   ${hasApiUrl ? '✅' : '⚠️ '} VITE_API_BASE_URL ${hasApiUrl ? '已定义' : '未定义'}`);
  console.log(`   ${hasWsUrl ? '✅' : '⚠️ '} VITE_WS_URL ${hasWsUrl ? '已定义' : '未定义'}`);
} else {
  console.log("\n4. .env.example 文件不存在（这是可选的）");
}

// 测试 5: 检查 .env 文件（如果存在）
const envPath = path.join(__dirname, '.env');
if (fs.existsSync(envPath)) {
  console.log("\n5. .env 文件检查:");
  const content = fs.readFileSync(envPath, 'utf-8');
  console.log("   ℹ️  .env 文件存在（本地配置，不应提交）");
  console.log(`   内容行数: ${content.split('\n').filter(line => line.trim() && !line.startsWith('#')).length}`);
} else {
  console.log("\n5. .env 文件不存在（将使用默认值）");
}

console.log("\n✅ 环境变量加载测试完成");

// 测试 6: 验证 URL 格式
console.log("\n6. URL 格式验证:");

function validateUrl(url, name) {
  try {
    new URL(url);
    console.log(`   ✅ ${name}: 格式正确 (${url})`);
    return true;
  } catch {
    console.log(`   ❌ ${name}: 格式错误 (${url})`);
    return false;
  }
}

const apiValid = validateUrl(mockEnv.VITE_API_BASE_URL, 'API URL');
const wsValid = mockEnv.VITE_WS_URL.startsWith('ws://') || mockEnv.VITE_WS_URL.startsWith('wss://');
if (wsValid) {
  console.log(`   ✅ WS URL: 格式正确 (${mockEnv.VITE_WS_URL})`);
} else {
  console.log(`   ❌ WS URL: 格式错误 (${mockEnv.VITE_WS_URL})`);
}

console.log("\n=== 测试完成 ===");