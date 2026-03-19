import Koa from 'koa';
import Router from '@koa/router';
import bodyParser from 'koa-bodyparser';
import { randomUUID } from 'crypto';
import { gzipSync, gunzipSync } from 'zlib';

// 用 require，避免 ts/esModuleInterop 兼容问题
const WebSocket = require('ws');

async function readRawBody(req: any): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

function buildHeader(
  messageType: number,
  flags: number,
  serialization: number,
  compression: number
): Buffer {
  const protocolVersion = 0x1;
  const headerSize = 0x1; // 4 bytes
  return Buffer.from([
    (protocolVersion << 4) | headerSize,
    (messageType << 4) | flags,
    (serialization << 4) | compression,
    0x00
  ]);
}

function buildFullClientRequestPacket() {
  const payloadObj = {
    user: {
      uid: 'douyin-miniapp-user'
    },
    audio: {
      format: 'pcm',
      codec: 'raw',
      rate: 16000,
      bits: 16,
      channel: 1
    },
    request: {
      model_name: 'bigmodel',
      enable_itn: true,
      enable_punc: true,
      enable_ddc: false,
      show_utterances: true,
      result_type: 'single'
    }
  };

  const payloadJson = Buffer.from(JSON.stringify(payloadObj), 'utf-8');
  const payloadGzip = gzipSync(payloadJson);

  const header = buildHeader(
    0x1, // full client request
    0x0, // no sequence
    0x1, // JSON
    0x1  // Gzip
  );

  const payloadSize = Buffer.alloc(4);
  payloadSize.writeUInt32BE(payloadGzip.length, 0);

  return Buffer.concat([header, payloadSize, payloadGzip]);
}

function buildAudioOnlyPacket(pcm: Buffer, isLast: boolean) {
  // 按你贴的示例：
  // 普通音频包：flags=0x0
  // 最后一包：flags=0x2
  const flags = isLast ? 0x2 : 0x0;

  const payloadGzip = gzipSync(pcm);

  const header = buildHeader(
    0x2, // audio only request
    flags,
    0x0, // raw bytes
    0x1  // Gzip
  );

  const payloadSize = Buffer.alloc(4);
  payloadSize.writeUInt32BE(payloadGzip.length, 0);

  return Buffer.concat([header, payloadSize, payloadGzip]);
}

function parseServerMessage(buf: Buffer) {
  const byte0 = buf[0];
  const byte1 = buf[1];
  const byte2 = buf[2];

  const protocolVersion = byte0 >> 4;
  const headerSizeInBytes = (byte0 & 0x0f) * 4;
  const messageType = byte1 >> 4;
  const flags = byte1 & 0x0f;
  const serialization = byte2 >> 4;
  const compression = byte2 & 0x0f;

  let offset = headerSizeInBytes;

  // 错误帧
  if (messageType === 0xF) {
    const errorCode = buf.readUInt32BE(offset);
    offset += 4;
    const errorSize = buf.readUInt32BE(offset);
    offset += 4;
    const errorMessage = buf.slice(offset, offset + errorSize).toString('utf-8');
    return {
      kind: 'error',
      protocolVersion,
      errorCode,
      errorMessage
    };
  }

  // full server response 带 sequence
  let sequence: number | null = null;
  if (messageType === 0x9) {
    sequence = buf.readInt32BE(offset);
    offset += 4;
  }

  const payloadSize = buf.readUInt32BE(offset);
  offset += 4;

  let payload = buf.slice(offset, offset + payloadSize);

  if (compression === 0x1) {
    payload = gunzipSync(payload);
  }

  let json: any = null;
  let text = '';

  if (serialization === 0x1) {
    text = payload.toString('utf-8');
    try {
      json = JSON.parse(text);
    } catch {
      json = null;
    }
  } else {
    text = payload.toString('utf-8');
  }

  return {
    kind: 'full',
    protocolVersion,
    messageType,
    flags,
    sequence,
    text,
    json
  };
}

type AudioChunk = {
  pcm: Buffer;
  isLast: boolean;
};

type AsrSession = {
  ws: any | null;
  connectId: string;
  queue: AudioChunk[];
  opened: boolean;
  fullRequestAcked: boolean;
};

let asrSession: AsrSession | null = null;

function closeAsrSession() {
  if (!asrSession) return;
  try {
    if (asrSession.ws) {
      asrSession.ws.close();
    }
  } catch (_) {}
  asrSession = null;
}

function flushAudioQueue() {
  if (!asrSession || !asrSession.ws) return;
  if (asrSession.ws.readyState !== WebSocket.OPEN) return;
  if (!asrSession.fullRequestAcked) return;

  while (asrSession.queue.length > 0) {
    const item = asrSession.queue.shift()!;
    const packet = buildAudioOnlyPacket(item.pcm, item.isLast);
    asrSession.ws.send(packet);
  }
}

function ensureAsrSession() {
  if (asrSession && asrSession.ws && asrSession.ws.readyState === WebSocket.OPEN) {
    return asrSession;
  }

  const appid = process.env.VOLC_APPID || '';
  const token = process.env.VOLC_ACCESS_TOKEN || '';
  const resourceId = process.env.VOLC_RESOURCE_ID || '';

  if (!appid || !token || !resourceId) {
    console.log(
      JSON.stringify({
        tag: 'asr_env_missing',
        hasAppid: !!appid,
        hasToken: !!token,
        hasResourceId: !!resourceId
      })
    );
    return null;
  }

  const connectId = randomUUID();
  const url =
  process.env.VOLC_ASR_WS_URL ||
  'wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_async';
  console.log(
  JSON.stringify({
    tag: 'asr_connect_try',
    url,
    appid,
    resourceId,
    tokenPrefix: token ? token.slice(0, 8) : '',
    tokenLen: token ? token.length : 0
  })
);
  const ws = new WebSocket(url, {
    headers: {
      'X-Api-App-Key': appid,
      'X-Api-Access-Key': token,
      'X-Api-Resource-Id': resourceId,
      'X-Api-Connect-Id': connectId
    }
  });

  ws.on('unexpected-response', (_req: any, res: any) => {
  let body = '';

  res.on('data', (chunk: Buffer) => {
    body += chunk.toString('utf8');
  });

  res.on('end', () => {
    console.log(
      JSON.stringify({
        tag: 'asr_ws_unexpected_response',
        connectId,
        statusCode: res.statusCode,
        statusMessage: res.statusMessage,
        headers: res.headers,
        body
      })
    );
  });
});
  ws.on('upgrade', (res: any) => {
  console.log(
    JSON.stringify({
      tag: 'asr_ws_upgrade',
      connectId,
      headers: res.headers
    })
  );
});
  asrSession = {
    ws,
    connectId,
    queue: [],
    opened: false,
    fullRequestAcked: false
  };

  ws.on('open', () => {
    if (!asrSession) return;
    asrSession.opened = true;

    console.log(
      JSON.stringify({
        tag: 'asr_ws_open',
        connectId
      })
    );

    const firstPacket = buildFullClientRequestPacket();
    ws.send(firstPacket);
  });

  ws.on('message', (raw: any) => {
    const buf = Buffer.isBuffer(raw) ? raw : Buffer.from(raw);
    const parsed = parseServerMessage(buf);

    if (parsed.kind === 'error') {
      console.log(
        JSON.stringify({
          tag: 'asr_server_error',
          connectId,
          errorCode: parsed.errorCode,
          errorMessage: parsed.errorMessage
        })
      );
      return;
    }

    // 收到第一个 full server response，才允许继续发音频包
    if (asrSession && !asrSession.fullRequestAcked) {
      asrSession.fullRequestAcked = true;
      console.log(
        JSON.stringify({
          tag: 'asr_full_request_acked',
          connectId,
          sequence: parsed.sequence
        })
      );
      flushAudioQueue();
    }

    const resultText = parsed.json?.result?.text || '';
    const utterances = parsed.json?.result?.utterances || [];

    console.log(
      JSON.stringify({
        tag: 'asr_server_message',
        connectId,
        sequence: parsed.sequence,
        text: resultText,
        utterancesCount: Array.isArray(utterances) ? utterances.length : 0,
        definite: Array.isArray(utterances)
          ? utterances.some((u: any) => !!u?.definite)
          : false
      })
    );
  });

  ws.on('close', () => {
    console.log(
      JSON.stringify({
        tag: 'asr_ws_close',
        connectId
      })
    );
    if (asrSession && asrSession.connectId === connectId) {
      asrSession = null;
    }
  });

  ws.on('error', (err: any) => {
    console.log(
      JSON.stringify({
        tag: 'asr_ws_error',
        connectId,
        error: String(err)
      })
    );
  });

  return asrSession;
}

const app = new Koa();
const router = new Router();

const jsonParser = bodyParser({ enableTypes: ['json'], jsonLimit: '2mb' });
app.use(async (ctx, next) => {
  if (ctx.path.startsWith('/api/')) return jsonParser(ctx, next);
  return next();
});

router.get('/healthz', (ctx) => {
  ctx.status = 200;
  ctx.body = 'ok';
});

router.all('/websocket_callback', async (ctx) => {
  const eventType = (ctx.get('x-tt-event-type') || '').toLowerCase();

  if (eventType === 'connect') {
    console.log(`eventType: connect tag: websocket_callback`);
    ensureAsrSession();
  } else if (eventType === 'disconnect') {
    console.log(`eventType: disconnect tag: websocket_callback`);
    closeAsrSession();
  } else if (eventType === 'uplink') {
    const body = await readRawBody(ctx.req);
    const text = body.toString('utf-8');

    try {
      const data = JSON.parse(text);

      if (data.type === 'audio_chunk' && data.audioBase64) {
        const pcmBuffer = Buffer.from(data.audioBase64, 'base64');

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
            pcmBytes: pcmBuffer.length,
            isLastFrame: !!data.isLastFrame
          })
        );

        const session = ensureAsrSession();
        if (session) {
          session.queue.push({
            pcm: pcmBuffer,
            isLast: !!data.isLastFrame
          });
          flushAudioQueue();
        }
      } else {
        console.log(
          JSON.stringify({
            tag: 'websocket_callback',
            eventType,
            rawTextPreview: text.slice(0, 200)
          })
        );
      }
    } catch (e) {
      console.log(
        JSON.stringify({
          tag: 'websocket_callback',
          eventType,
          parseError: String(e),
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

const PORT = Number(process.env.PORT || 8000);
app.listen(PORT, () => console.log(`server listening on ${PORT}`));