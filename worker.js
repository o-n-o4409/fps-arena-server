// FPS練習場 対戦用 中継サーバー (Cloudflare Workers + Durable Objects)
// 部屋コードごとに Durable Object が1つ作られ、全員の presence をまとめて配信する。
// 誰も接続していない間は何も動かない(待機中は課金・消費なし)。

const MAX = 4;          // 1部屋の最大人数
const TICK = 33;        // スナップショット送信間隔(ms)
const IDLE_MS = 12000;  // これだけ無通信なら切断扱い
const BAD = new Set(['__proto__', 'constructor', 'prototype']);

const merge = (o, p) => {
  for (const k in p) {
    if (BAD.has(k)) continue;
    if (p[k] === null) delete o[k]; else o[k] = p[k];
  }
};
const lab = n => 'p' + String(n).padStart(4, '0');

export class Room {
  constructor(state, env) {
    this.table = {};   // id -> presence
    this.conns = {};   // id -> { ws, seen }
    this.n = 0;
    this.dirty = true;
    this.last = 0;
    this.timer = null;
  }

  fetch(request) {
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('expected websocket', { status: 426 });
    }
    const pair = new WebSocketPair();
    const client = pair[0], ws = pair[1];
    ws.accept();

    if (Object.keys(this.conns).length >= MAX) {
      ws.send(JSON.stringify({ t: 'full' }));
      setTimeout(() => { try { ws.close(1000, 'full'); } catch (_) {} }, 300);
      return new Response(null, { status: 101, webSocket: client });
    }

    const id = lab(this.n++);   // 先に入った人ほど小さいID = ロビーのホスト
    this.conns[id] = { ws, seen: Date.now() };
    this.table[id] = {};
    this.dirty = true;
    ws.send(JSON.stringify({ t: 'hi', id }));

    ws.addEventListener('message', ev => {
      const o = this.conns[id];
      if (!o || typeof ev.data !== 'string' || ev.data.length > 16384) return;
      o.seen = Date.now();
      let d;
      try { d = JSON.parse(ev.data); } catch (_) { return; }
      if (d && d.t === 'pr' && d.p && typeof d.p === 'object' && this.table[id]) {
        merge(this.table[id], d.p);
        this.dirty = true;
      }
    });
    ws.addEventListener('close', () => this.drop(id));
    ws.addEventListener('error', () => this.drop(id));

    if (!this.timer) this.timer = setInterval(() => this.tick(), TICK);
    return new Response(null, { status: 101, webSocket: client });
  }

  drop(id) {
    const o = this.conns[id];
    if (!o) return;
    delete this.conns[id];
    delete this.table[id];
    try { o.ws.close(); } catch (_) {}
    this.dirty = true;
    if (!Object.keys(this.conns).length) {
      this.n = 0; this.table = {};
      clearInterval(this.timer); this.timer = null;   // 空になったら止める
    }
  }

  tick() {
    const now = Date.now();
    for (const id of Object.keys(this.conns)) {
      if (now - this.conns[id].seen > IDLE_MS) this.drop(id);
    }
    if (!this.timer) return;
    if (this.dirty || now - this.last > 2000) {
      this.dirty = false; this.last = now;
      const msg = JSON.stringify({ t: 's', s: this.table });
      for (const id in this.conns) {
        const w = this.conns[id].ws;
        if (w.readyState === 1) { try { w.send(msg); } catch (_) {} }
      }
    }
  }
}

export default {
  async fetch(request, env) {
    // ブラウザで開いたとき/ウェイクアップ用
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('fps server ok', {
        headers: { 'content-type': 'text/plain; charset=utf-8', 'access-control-allow-origin': '*' },
      });
    }
    const url = new URL(request.url);
    const code = (url.searchParams.get('room') || '').toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 12);
    if (!code) return new Response('room required', { status: 400 });
    return env.ROOM.get(env.ROOM.idFromName(code)).fetch(request);
  },
};
