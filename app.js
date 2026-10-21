import express from 'express';
import { Queue, Worker } from 'bullmq';
import IORedis from 'ioredis';
import pg from 'pg';

const app = express();
app.use(express.json());

const pgPool = new pg.Pool({
  connectionString: 'postgres://admin:123456@127.0.0.1:6432/postgres',
  max: 20,
  idleTimeoutMillis: 30000,
});

// 1. Redis 连接
const redisConnection = new IORedis({
  host: '127.0.0.1',
  port: 6379,
  maxRetriesPerRequest: null
});

// 2. 创建双呼任务队列
export const dualCallQueue = new Queue('dual-call-queue', 
    { 
        connection: redisConnection,
        defaultJobOptions: {
            attempts: 3,
            backoff: {
                type: 'exponential',
                delay: 1000,
            },
            removeOnComplete: true,
            removeOnFail: 1000,
        },
});

async function handleCallRequest(req, res) {
  const { caller, callee } = req.body;

  // 1. 将任务打包丢入 Redis 队列（RPUSH）
  const task = JSON.stringify({ caller, callee, timestamp: Date.now() });
  await redis.rpush('operator_queue', task);

  // 2. 极速响应前端，用户零感知延迟 (< 2ms)
  return res.json({ 
    code: 200, 
    message: 'Request accepted, processing in background' 
  });
}

app.post('/api/v1/call/dual2', async (req, res) => {
  // 在写数据库或处理请求的代码中加入这一行模拟运营商耗时
  // await new Promise((resolve) => setTimeout(resolve, 500));
  const { caller, callee } = req.body || {};

  if (!caller || !callee) {
    return res.status(400).json({ code: 400, error: '主叫和被叫号码不能为空' });
  }

  const callId = 'call_' + Date.now() + '_' + Math.random().toString(36).substring(2, 7);

  try {
    await dualCallQueue.add('trigger-dual-call', {
        callId,
        caller,
        callee,
        timestamp: Date.now(),
    });

    return res.json({ code: 200, message: '双呼任务已受理', callId });
  } catch (err) {
    console.error('入队失败:', err);
    return res.status(500).json({ code: 500, error: '数据库繁忙，系统降级' });
  }
});

// 4. 本地先单进程跑一次测试，确保没语法错误
const PORT = 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 语音双呼服务已启动: http://localhost:${PORT}`);
});