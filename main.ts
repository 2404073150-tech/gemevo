/* 宝石进化 · 联机中继（Deno Deploy 版）
 *
 * 职责：只做「房间 + 消息转发」，不跑游戏逻辑。
 * 房主（seat 0）本地跑游戏逻辑广播 state，客人只发 intent。
 *
 * 与 App 的协议：
 *   客户端 -> 服务端
 *     {type:"start", seats:N}          房主开局（只认 seat 0）
 *     {type:"state", state:{...}}      房主广播局面（只认 seat 0）
 *     {type:"intent", seat, kind, ...} 客人操作（只转发给 seat 0，且校验座位）
 *     {type:"ping"}                    心跳
 *   服务端 -> 客户端
 *     {type:"hello", id, seat, members}       连上即分配座位
 *     {type:"members", n, assign?, seat?}     人数变化
 *     {type:"start"|"state"|"intent"}         原样转发
 *     {type:"error", msg}
 */

const MAX_SEATS = 4;

/** peer: { ws, id, seat } */
const rooms = new Map(); // code -> Set<peer>

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function send(ws, obj) {
  try {
    if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
  } catch { /* 忽略 */ }
}

function broadcast(peers, msg, exclude) {
  for (const p of peers) if (p.ws !== exclude) send(p.ws, msg);
}

function nextSeat(peers) {
  const used = new Set([...peers].map((p) => p.seat));
  for (let i = 0; i < MAX_SEATS; i++) if (!used.has(i)) return i;
  return -1;
}

Deno.serve((req) => {
  const url = new URL(req.url);

  if (url.pathname === "/health") return json({ ok: true, rooms: rooms.size });
  if (url.pathname === "/") {
    return json({ ok: true, rooms: rooms.size, usage: "wss://<host>/ws/<ROOM>" });
  }

  const m = url.pathname.match(/^\/ws\/([A-Za-z0-9]{3,8})$/);
  if (!m) return new Response("not found", { status: 404 });
  if (req.headers.get("upgrade") !== "websocket") {
    return new Response("websocket required", { status: 426 });
  }

  const code = m[1].toUpperCase();
  let room = rooms.get(code);
  if (!room) {
    room = new Set();
    rooms.set(code, room);
  }

  const { socket, response } = Deno.upgradeWebSocket(req);

  if (room.size >= MAX_SEATS) {
    socket.onopen = () => send(socket, { type: "error", msg: "房间已满（最多 4 人）" });
    socket.onopen && setTimeout(() => { try { socket.close(4002); } catch { /* */ } }, 100);
    return response;
  }

  const seat = nextSeat(room);
  const id = `${Date.now() % 1000000000}_${seat}_${Math.floor(Math.random() * 1000)}`;
  const peer = { ws: socket, id, seat };
  room.add(peer);

  socket.onopen = () => {
    send(socket, { type: "hello", id, seat, members: room.size });
    broadcast(room, { type: "members", n: room.size, assign: id, seat }, socket);
  };

  socket.onmessage = (e) => {
    let msg;
    try { msg = JSON.parse(e.data); } catch { return; }
    const t = msg.type;

    if (t === "ping") { send(socket, { type: "pong" }); return; }
    if (t === "hello") { send(socket, { type: "hello", id, seat, members: room.size }); return; }
    if (t === "members") return;

    if (t === "intent") {
      // 只接受本人座位发出的操作，并只转发给房主（seat 0）
      if (msg.seat !== peer.seat) return;
      msg.seat = peer.seat;
      msg.from = peer.id;
      for (const p of room) if (p.seat === 0 && p.ws !== socket) send(p.ws, msg);
      return;
    }

    if (t === "start" || t === "state") {
      // 只允许房主广播
      if (peer.seat !== 0) return;
      broadcast(room, msg, socket);
      return;
    }
  };

  socket.onclose = () => {
    room.delete(peer);
    if (room.size === 0) rooms.delete(code);
    else broadcast(room, { type: "members", n: room.size });
  };
  socket.onerror = () => {
    room.delete(peer);
    if (room.size === 0) rooms.delete(code);
  };

  return response;
});
