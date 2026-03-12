import Koa from 'koa';
import Router from '@koa/router';
import bodyParser from 'koa-bodyparser';

async function readRawBody(req: any): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

const app = new Koa();
const router = new Router();

// 只给 /api/* 开 JSON body parser，避免把 /websocket_callback 的二进制流提前吃掉
const jsonParser = bodyParser({ enableTypes: ['json'], jsonLimit: '1mb' });
app.use(async (ctx, next) => {
  if (ctx.path.startsWith('/api/')) return jsonParser(ctx, next);
  return next();
});

// 1) 健康检查：用于确认部署成功
router.get('/healthz', (ctx) => {
  ctx.status = 200;
  ctx.body = 'ok';
});

// 2) WebSocket 网关回调：connect/disconnect/uplink 都会打到这里
router.all('/websocket_callback', async (ctx) => {
  const eventType = (ctx.get('x-tt-event-type') || '').toLowerCase();

  try {
    if (eventType === 'uplink') {
      const raw = await readRawBody(ctx.req);
      console.log(
        JSON.stringify({
          tag: 'websocket_callback',
          eventType,
          rawLen: raw.length,
          contentType: ctx.get('content-type')
        })
      );
    } else {
      // connect / disconnect 先不要读 body，先保证握手成功
      console.log(
        JSON.stringify({
          tag: 'websocket_callback',
          eventType
        })
      );
    }

    ctx.status = 200;
    ctx.type = 'text/plain; charset=utf-8';
    ctx.body = 'success';   // 关键：先严格按官方示例返回纯文本 success
  } catch (err: any) {
    console.error('websocket_callback error:', err?.stack || err);
    ctx.status = 200;
    ctx.type = 'text/plain; charset=utf-8';
    ctx.body = 'success';   // 先兜底，避免握手被 500 打断
  }
});

app.use(router.routes()).use(router.allowedMethods());

// 抖音云容器会注入 PORT；本地跑也能用
const PORT = Number(process.env.PORT || 8000);
app.listen(PORT, () => console.log(`server listening on ${PORT}`));