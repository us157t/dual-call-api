import IORedis from 'ioredis';
import { Worker } from 'bullmq';
import pg from 'pg';

// ==========================================
// 1. 基础设施与连接配置
// ==========================================
const pgPool = new pg.Pool({
  connectionString: 'postgres://admin:123456@127.0.0.1:6432/postgres',
  max: 30,
  idleTimeoutMillis: 30000,
});

const redisConnection = new IORedis({
  host: '127.0.0.1',
  port: 6379,
  maxRetriesPerRequest: null
});

// ==========================================
// 2. 话单批量落库管理器 (Batch Inserter)
// ==========================================
class LogBatcher {
  constructor(pool, batchSize = 5000, flushIntervalMs = 60000) {
    this.pool = pool;
    this.batchSize = batchSize;
    this.buffer = [];
    
    // 定时刷盘
    this.timer = setInterval(() => this.flush(), flushIntervalMs);
  }

  push(record) {
    this.buffer.push(record);
    if (this.buffer.length >= this.batchSize) {
      this.flush();
    }
  }

  async flush() {
    if (this.buffer.length === 0) return;

    const chunk = this.buffer;
    this.buffer = []; // 立即清空，准备接纳下一批
    //console.log(`🔥 [批量落库触发] 本次一次性写入数据库的记录数: ${chunk.length} 条`);

    const values = [];
    const tuples = [];
    let idx = 1;

    for (const item of chunk) {
      tuples.push(`($${idx}, $${idx+1}, $${idx+2}, $${idx+3}, $${idx+4}, NOW())`);
      values.push(item.callId, item.caller, item.callee, item.status, item.errorMsg);
      idx += 5;
    }

    const query = `
      INSERT INTO call_logs (call_id, caller, callee, status, error_msg, updated_at)
      VALUES ${tuples.join(', ')}
      ON CONFLICT (call_id) 
      DO UPDATE SET status = EXCLUDED.status, error_msg = EXCLUDED.error_msg, updated_at = NOW();
    `;

    try {
      await this.pool.query(query, values);
    } catch (err) {
      console.error('💥 [批量落库失败]:', err.message);
    }
  }

  getPendingCount() {
    return this.buffer.length;
  }
}

const logBatcher = new LogBatcher(pgPool);

// ==========================================
// 3. 业务逻辑层（运营商接口模拟）
// ==========================================
async function callOperatorAPI(caller, callee) {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      if (Math.random() < 0.05) {
        return reject(new Error('运营商接口响应超时'));
      }
      resolve(true);
    }, 500);
  });
}

// ==========================================
// 4. QPS 与队列监控统计
// ==========================================
let processedCount = 0;

setInterval(() => {
  console.log(`[Worker 状态] 消费速率: ${processedCount} QPS | 内存待落库积压: ${logBatcher.getPendingCount()} 条`);
  processedCount = 0;
}, 10000);

// ==========================================
// 5. 启动 BullMQ Worker
// ==========================================
const worker = new Worker(
  'dual-call-queue',
  async (job) => {
    const { callId, caller, callee } = job.data;

    try {
      await callOperatorAPI(caller, callee);

      logBatcher.push({ callId, caller, callee, status: 'SUCCESS', errorMsg: null });
      processedCount++;
    } catch (err) {
      logBatcher.push({ callId, caller, callee, status: 'FAILED', errorMsg: err.message });
      throw err; // 触发重试
    }
  },
  {
    connection: redisConnection,
    concurrency: 100,
    limiter: { max: 200, duration: 1000 }, // 200 QPS 下游限流
  }
);

// 周期性打印 Redis 队列状态
setInterval(async () => {
  try {
    const counts = await worker.queue.getJobCounts('wait', 'active', 'failed');
    console.log(`📊 [Redis 队列] 等待中: ${counts.wait} | 处理中: ${counts.active} | 失败: ${counts.failed}`);
  } catch (err) {}
}, 3000);

console.log('🚀 模块化高性能 Worker 启动成功！');