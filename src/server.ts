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
  const eventType = (ctx.get('x-tt-event-type') || '').toLowerCase(); // connect / disconnect / uplink
  const raw = await readRawBody(ctx.req);
  const rawLen = raw.length;

  // 打日志：后面我们就靠这个确认“音频分片 uplink 到达后端”
  console.log(
    JSON.stringify({
      tag: 'websocket_callback',
      eventType,
      rawLen,
      contentType: ctx.get('content-type'),
      // 有时网关会带一些 ws openid 等头，先一起打出来
      headers: {
        'x-tt-event-type': ctx.get('x-tt-event-type'),
        'x-tt-ws-openids': ctx.get('x-tt-ws-openids'),
      },
    })
  );

  // 必须返回 success（最简单先这样）
  ctx.status = 200;
  ctx.body = { success: true };
});

app.use(router.routes()).use(router.allowedMethods());

// 抖音云容器会注入 PORT；本地跑也能用
const PORT = Number(process.env.PORT || 8000);
app.listen(PORT, () => console.log(`server listening on ${PORT}`));