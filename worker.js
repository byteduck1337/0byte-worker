const ALLOWED_ORIGINS = new Set([
  'https://byteduck1337.github.io',
  'http://localhost:8080',
  'http://127.0.0.1:8080',
  'http://localhost:5500',
  'http://127.0.0.1:5500',
  'http://localhost:3000',
  'http://127.0.0.1:3000',
  'http://localhost:5173',
  'http://127.0.0.1:5173'
]);

const ROOM_TTL = 900000;
const MAX_MSG = 65536;
const HEARTBEAT = 20000;

function corsHeaders(origin) {
  const h = {
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '86400',
    'Cache-Control': 'no-store',
    'Vary': 'Origin'
  };
  if (origin && ALLOWED_ORIGINS.has(origin)) h['Access-Control-Allow-Origin'] = origin;
  return h;
}

function json(data, status, origin) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', ...corsHeaders(origin) }
  });
}

export class Room {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.streams = new Map();
  }

  sockets() {
    try { return this.state.getWebSockets(); } catch (e) { return []; }
  }

  clients() {
    const list = [];
    for (const ws of this.sockets()) list.push({ kind: 'ws', ref: ws });
    for (const s of this.streams.values()) list.push({ kind: 'sse', ref: s });
    return list;
  }

  count() {
    return this.sockets().length + this.streams.size;
  }

  async touch() {
    try { await this.state.storage.setAlarm(Date.now() + ROOM_TTL); } catch (e) {}
  }

  send(ws, obj) {
    try { ws.send(typeof obj === 'string' ? obj : JSON.stringify(obj)); } catch (e) {}
  }

  push(stream, text) {
    try {
      stream.controller.enqueue(stream.encoder.encode(text));
      return true;
    } catch (e) {
      this.dropStream(stream.token, false);
      return false;
    }
  }

  dropStream(token, notify) {
    const stream = this.streams.get(token);
    if (!stream) return;
    this.streams.delete(token);
    clearInterval(stream.heartbeat);
    try { stream.controller.close(); } catch (e) {}
    if (notify) this.broadcast({ type: 'peer-left' }, null);
  }

  deliver(from, payload) {
    let sent = 0;
    for (const c of this.clients()) {
      if (c.ref === from) continue;
      if (c.kind === 'ws') {
        try { c.ref.send(payload); sent++; } catch (e) {}
      } else if (this.push(c.ref, 'data: ' + payload + '\n\n')) {
        sent++;
      }
    }
    return sent;
  }

  broadcast(obj, except) {
    return this.deliver(except, JSON.stringify(obj));
  }

  async fetch(request) {
    const url = new URL(request.url);

    if (url.pathname === '/pub' && request.method === 'POST') {
      const body = await request.text();
      if (body.length > MAX_MSG) return new Response('too big', { status: 413 });
      await this.touch();
      const sent = this.deliver(null, body);
      return new Response(JSON.stringify({ ok: true, peers: this.count(), sent }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    if (url.pathname === '/sse') {
      if (this.count() >= 2) {
        return new Response(JSON.stringify({ error: 'room_full' }), {
          status: 409,
          headers: { 'Content-Type': 'application/json' }
        });
      }
      const token = crypto.randomUUID();
      const encoder = new TextEncoder();
      const self = this;
      let record = null;
      const stream = new ReadableStream({
        start(controller) {
          record = { token, controller, encoder, heartbeat: null };
          self.streams.set(token, record);
          self.push(record, 'retry: 2000\n\n');
          const peers = self.count();
          self.push(record, 'data: ' + JSON.stringify({
            type: peers > 1 ? 'peer-joined' : 'waiting',
            peers,
            transport: 'sse'
          }) + '\n\n');
          if (peers > 1) self.broadcast({ type: 'peer-joined', peers }, record);
          record.heartbeat = setInterval(() => self.push(record, ': ping\n\n'), HEARTBEAT);
        },
        cancel() {
          self.dropStream(token, true);
        }
      });
      await this.touch();
      return new Response(stream, {
        status: 200,
        headers: {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-store, no-transform',
          'Connection': 'keep-alive',
          'X-Accel-Buffering': 'no'
        }
      });
    }

    if ((request.headers.get('Upgrade') || '').toLowerCase() !== 'websocket') {
      return new Response('websocket required', { status: 426 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    if (this.count() >= 2) {
      try {
        server.accept();
        server.send(JSON.stringify({ type: 'room-full' }));
        server.close(4001, 'room full');
      } catch (e) {}
      return new Response(null, { status: 101, webSocket: client });
    }

    this.state.acceptWebSocket(server);
    await this.touch();
    try { server.serializeAttachment({ joinedAt: Date.now() }); } catch (e) {}

    const peers = this.count();
    if (peers > 1) this.broadcast({ type: 'peer-joined', peers }, null);
    else this.send(server, { type: 'waiting', peers: 1, transport: 'ws' });

    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, message) {
    const size = typeof message === 'string' ? message.length : message.byteLength;
    if (size > MAX_MSG) {
      try { ws.close(4002, 'message too big'); } catch (e) {}
      return;
    }
    if (typeof message !== 'string') return;
    await this.touch();
    this.deliver(ws, message);
  }

  async webSocketClose(ws) {
    try { ws.close(1000, 'closed'); } catch (e) {}
    this.broadcast({ type: 'peer-left' }, ws);
    await this.touch();
  }

  async webSocketError(ws) {
    try { ws.close(1011, 'error'); } catch (e) {}
    this.broadcast({ type: 'peer-left' }, ws);
  }

  async alarm() {
    for (const ws of this.sockets()) {
      try { ws.close(4000, 'room expired'); } catch (e) {}
    }
    for (const s of Array.from(this.streams.values())) {
      this.push(s, 'data: ' + JSON.stringify({ type: 'room-expired' }) + '\n\n');
      this.dropStream(s.token, false);
    }
    try { await this.state.storage.deleteAll(); } catch (e) {}
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const origin = request.headers.get('Origin') || '';

    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders(origin) });
    }

    if (url.pathname === '/' || url.pathname === '/health') {
      let rooms = 'missing';
      try {
        if (env.ROOMS && typeof env.ROOMS.idFromName === 'function') rooms = 'ok';
      } catch (e) {
        rooms = 'error';
      }
      return json({
        ok: true,
        service: '0byte-signal',
        version: 4,
        transports: ['websocket', 'sse'],
        rooms,
        turn: Boolean(env.TURN_KEY_ID && env.TURN_API_TOKEN),
        ts: Date.now()
      }, 200, origin);
    }

    if (url.pathname === '/turn') {
      let iceServers = [
        { urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] },
        { urls: 'stun:stun.cloudflare.com:3478' }
      ];
      if (env.TURN_KEY_ID && env.TURN_API_TOKEN) {
        try {
          const r = await fetch(
            'https://rtc.live.cloudflare.com/v1/turn/keys/' + env.TURN_KEY_ID + '/credentials/generate',
            {
              method: 'POST',
              headers: {
                'Authorization': 'Bearer ' + env.TURN_API_TOKEN,
                'Content-Type': 'application/json'
              },
              body: JSON.stringify({ ttl: 3600 })
            }
          );
          if (r.ok) {
            const data = await r.json();
            if (data.iceServers) {
              const list = Array.isArray(data.iceServers) ? data.iceServers : [data.iceServers];
              iceServers = iceServers.concat(list);
            }
          }
        } catch (e) {}
      }
      return json({ iceServers }, 200, origin);
    }

    const match = url.pathname.match(/^\/(ws|sse|pub)\/([a-f0-9]{32})$/);
    if (!match) return json({ error: 'not_found' }, 404, origin);

    if (origin && !ALLOWED_ORIGINS.has(origin)) {
      return json({ error: 'forbidden_origin', origin }, 403, origin);
    }

    if (!env.ROOMS || typeof env.ROOMS.idFromName !== 'function') {
      return json({ error: 'rooms_binding_missing', hint: 'Add Durable Object binding ROOMS -> Room' }, 503, origin);
    }

    const mode = match[1];
    const roomId = match[2];

    try {
      const stub = env.ROOMS.get(env.ROOMS.idFromName(roomId));

      if (mode === 'ws') {
        if ((request.headers.get('Upgrade') || '').toLowerCase() !== 'websocket') {
          return new Response('websocket required', { status: 426 });
        }
        return await stub.fetch(request);
      }

      if (mode === 'sse') {
        const res = await stub.fetch(new Request('https://room.internal/sse', { method: 'GET' }));
        const headers = new Headers(res.headers);
        const cors = corsHeaders(origin);
        for (const k in cors) headers.set(k, cors[k]);
        return new Response(res.body, { status: res.status, headers });
      }

      if (request.method !== 'POST') return json({ error: 'method_not_allowed' }, 405, origin);
      const body = await request.text();
      if (body.length > MAX_MSG) return json({ error: 'too_big' }, 413, origin);
      const res = await stub.fetch(new Request('https://room.internal/pub', { method: 'POST', body }));
      const text = await res.text();
      return new Response(text, {
        status: res.status,
        headers: { 'Content-Type': 'application/json', ...corsHeaders(origin) }
      });
    } catch (e) {
      return json({ error: 'room_unavailable', detail: String((e && e.message) || e) }, 503, origin);
    }
  }
};
