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

  if (eventType === 'uplink') {
    const body = await readRawBody(ctx.req);
    const text = body.toString('utf-8');

    try {
      const data = JSON.parse(text);
      console.log(
        JSON.stringify({
          tag: 'websocket_callback',
          eventType,
          type: data.type,
          seq: data.seq,
          format: data.format,
          sampleRate: data.sampleRate,
          channels: data.channels,
          audioBase64Len: data.audioBase64 ? data.audioBase64.length : 0,
          isLastFrame: !!data.isLastFrame
        })
      );
    } catch (e) {
      console.log(
        JSON.stringify({
          tag: 'websocket_callback',
          eventType,
          rawTextPreview: text.slice(0, 200)
        })
      );
    }
  } else {
    console.log(`eventType: ${eventType} tag: websocket_callback`);
  }

  ctx.status = 200;
  ctx.type = 'text/plain; charset=utf-8';
  ctx.body = 'success';
});

app.use(router.routes()).use(router.allowedMethods());

// 抖音云容器会注入 PORT；本地跑也能用
const PORT = Number(process.env.PORT || 8000);
app.listen(PORT, () => console.log(`server listening on ${PORT}`));