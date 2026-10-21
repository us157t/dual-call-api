module.exports = {
  apps: [
    {
      name: 'app',
      script: './app.js',
      instances: 4,                  // 4 核机器开 4 个进程跑满集群负载均衡
      exec_mode: 'cluster',
      max_memory_restart: '2G',
      env: {
        NODE_ENV: 'production',
        PORT: 3000                   // 你的 Express API 监听 3000 端口
      }
    },
    {
      name: 'bullmq-worker',
      script: './worker.js',
      instances: 2,                  // 开启 2 个 Worker 进程并发消费 BullMQ 任务
      exec_mode: 'fork',             // Worker 通常用 fork 模式独立运行
      max_memory_restart: '1G',
      env: {
        NODE_ENV: 'production'
      }
    }
  ]
};
