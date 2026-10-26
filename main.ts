/* 宝石进化 · 联机中继 v2（Deno Deploy 版，KV + HTTP 轮询）
 *
 * 为什么不用 WebSocket？Deno Deploy 免费版会把不同请求路由到不同隔离实例，
 * 内存里的房间会"消失"（实测复现：第二个客人加入时被分到全新空房）。
 * Deno KV 是全球一致存储，所有实例共享，彻底根治掉线。
 *
 * KV 结构：
 *   ["room", code]        → 房间主数据（座位令牌/started/seats/rev/intentsRev/state/intents）
 *   ["seen", code, token] → 该玩家最后一次心跳时间戳（独立 key，避免与房间写冲突）
 *
 * 接口（全部 HTTP POST，JSON）：
 *   POST /api/join  {code}                      → {ok,seat,token,started,seats,members,rev,intentsRev}
 *   POST /api/poll  {code,token,since:{rev,intentsRev}} → 心跳+拉取
 *   POST /api/start {code,token,seats}          → {ok}（仅房主）
 *   POST /api/state {code,token,state}          → {ok,rev}（仅房主，rev++）
 *   POST /api/intent {code,token,intent}        → {ok}（仅客人，intentsRev++）
 *   GET  /health
 *
 * 心跳超过 15 秒视为离线，其座位可被新玩家接管；游戏中重进自动回到对局。
 */

const STALE_MS = 15000;
let kv: any = null;
async function getKV() {
  if (!kv) kv = await Deno.openKv();
  return kv;
}

function json(obj: any, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "access-control-allow-origin": "*" },
  });
}

const tok = () => Math.random().toString(36).slice(2) + Date.now().toString(36);

function fresh(ts: number | undefined) {
  return ts && (Date.now() - ts < STALE_MS);
}

async function seatTs(code: string, m: { token: string } | null): Promise<number> {
  if (!m) return 0;
  const r = await (await getKV()).get(["seen", code, m.token]);
  return (r.value as number) || 0;
}

async function countMembers(code: string, room: any) {
  let n = 0;
  if (room.host && fresh(await seatTs(code, room.host))) n++;
  for (const g of room.guests || []) if (g && fresh(await seatTs(code, g))) n++;
  return n;
}

Deno.serve(async (req) => {
  const url = new URL(req.url);
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: { "access-control-allow-origin": "*", "access-control-allow-headers": "content-type", "access-control-allow-methods": "POST" } });
  }
  if (url.pathname === "/health") {
    const k = await getKV();
    let n = 0;
    for await (const e of k.list({ prefix: ["room"] }, { limit: 500 })) {
      const room: any = e.value;
      // 只统计最近 60 秒有活动的房间
      if (room && (Date.now() - (room.updated || 0) < 60000)) n++;
    }
    return json({ ok: true, rooms: n });
  }
  if (url.pathname === "/") return json({ ok: true, v: 2, usage: "POST /api/join {code}" });
  if (!url.pathname.startsWith("/api/")) return new Response("not found", { status: 404 });

  let body: any = {};
  try { body = await req.json(); } catch { /* */ }
  const code = String(body.code || "").toUpperCase().slice(0, 8);
  if (!/^[A-Z0-9]{3,8}$/.test(code)) return json({ ok: false, msg: "房号无效" }, 400);

  const now = Date.now();
  const k = await getKV();
  const key = ["room", code];

  /* ---- 加入 ---- */
  if (url.pathname === "/api/join") {
    for (let attempt = 0; attempt < 8; attempt++) {
      const cur = await k.get(key);
      const room: any = cur.value ? JSON.parse(JSON.stringify(cur.value)) :
        { host: null, guests: [null, null, null], started: false, seats: 0, rev: 0, intentsRev: 0, state: null, intents: [], updated: now };
      let seat = -1;
      const token = tok();

      const hostTs = room.host ? await seatTs(code, room.host) : 0;
      if (!room.host || !fresh(hostTs)) {
        room.host = { token }; seat = 0;
      } else {
        const gs = room.guests;
        for (let i = 0; i < 3; i++) {
          const gts = gs[i] ? await seatTs(code, gs[i]) : 0;
          if (!gs[i] || !fresh(gts)) { gs[i] = { token }; seat = i + 1; break; }
        }
        if (seat < 0) return json({ ok: false, msg: "房间已满（最多 4 人）" }, 403);
      }
      await k.set(["seen", code, token], now);
      room.updated = now;
      const res = await k.atomic().check(cur).set(key, room).commit();
      if (res.ok) {
        return json({ ok: true, seat, token, started: !!room.started, seats: room.seats || 0, members: await countMembers(code, room), rev: room.rev, intentsRev: room.intentsRev });
      }
    }
    return json({ ok: false, msg: "房间繁忙，请重试" }, 503);
  }

  /* ---- 鉴权 ---- */
  const room0: any = (await k.get(key)).value;
  if (!room0) return json({ ok: false, msg: "房间不存在", rejoin: true }, 404);
  const token = body.token;
  const isHost = room0.host && room0.host.token === token;
  let mySeat = isHost ? 0 : -1;
  if (!isHost) {
    const gs = room0.guests || [];
    for (let i = 0; i < 3; i++) if (gs[i] && gs[i].token === token) mySeat = i + 1;
  }
  if (mySeat < 0) return json({ ok: false, msg: "令牌失效，请重新加入", rejoin: true }, 401);

  /* ---- 心跳 + 拉取（只写自己的 seen key，绝不碰房间主数据）---- */
  if (url.pathname === "/api/poll") {
    await k.set(["seen", code, token], now);
    const room: any = (await k.get(key)).value;
    if (!room) return json({ ok: false, msg: "房间不存在", rejoin: true }, 404);
    const since = body.since || {};
    const out: any = { ok: true, seat: mySeat, members: await countMembers(code, room), started: !!room.started, seats: room.seats || 0, rev: room.rev, intentsRev: room.intentsRev };
    if (!isHost && room.rev > (since.rev || 0) && room.state) out.state = room.state;
    if (isHost && room.intentsRev > (since.intentsRev || 0)) out.intents = room.intents || [];
    return json(out);
  }

  /* ---- 房主开局 ---- */
  if (url.pathname === "/api/start") {
    if (!isHost) return json({ ok: false, msg: "只有房主能开始" }, 403);
    for (let attempt = 0; attempt < 8; attempt++) {
      const cur = await k.get(key);
      const room: any = cur.value;
      if (!room) return json({ ok: false, msg: "房间不存在", rejoin: true }, 404);
      room.started = true;
      room.seats = Math.max(2, Math.min(4, body.seats || await countMembers(code, room)));
      room.rev++; room.intents = []; room.intentsRev = 0; room.updated = now;
      if ((await k.atomic().check(cur).set(key, room).commit()).ok) return json({ ok: true, seats: room.seats });
    }
    return json({ ok: false, msg: "房间繁忙，请重试" }, 503);
  }

  /* ---- 房主广播局面（原子 rev++，不会丢）---- */
  if (url.pathname === "/api/state") {
    if (!isHost) return json({ ok: false, msg: "只有房主能广播" }, 403);
    for (let attempt = 0; attempt < 8; attempt++) {
      const cur = await k.get(key);
      const room: any = cur.value;
      if (!room) return json({ ok: false, msg: "房间不存在", rejoin: true }, 404);
      room.state = body.state;
      room.rev++; room.updated = now;
      if ((await k.atomic().check(cur).set(key, room).commit()).ok) return json({ ok: true, rev: room.rev });
    }
    return json({ ok: false, msg: "房间繁忙，请重试" }, 503);
  }

  /* ---- 客人操作（原子追加，不会丢）---- */
  if (url.pathname === "/api/intent") {
    if (isHost) return json({ ok: false, msg: "房主无需转发" }, 400);
    for (let attempt = 0; attempt < 8; attempt++) {
      const cur = await k.get(key);
      const room: any = cur.value;
      if (!room) return json({ ok: false, msg: "房间不存在", rejoin: true }, 404);
      room.intents = room.intents || [];
      room.intents.push({ ...body.intent, seat: mySeat, seq: (room.intentsRev || 0) + 1 });
      if (room.intents.length > 50) room.intents = room.intents.slice(-50);
      room.intentsRev = (room.intentsRev || 0) + 1; room.updated = now;
      if ((await k.atomic().check(cur).set(key, room).commit()).ok) return json({ ok: true });
    }
    return json({ ok: false, msg: "房间繁忙，请重试" }, 503);
  }

  return json({ ok: false, msg: "未知接口" }, 404);
});
