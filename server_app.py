#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
璀璨宝石 · 宝可梦版 —— 小程序联机中继服务

职责很薄：只做「房间 + 消息转发」，不跑游戏逻辑。
   房主（seat 0）本地跑逻辑，把 state 广播给客人
   客人只发 intent，由房主校验执行

消息协议（JSON）
   客户端 -> 服务端
     {type:"hello", nick?}            加入房间（连上即自动发，可省略）
     {type:"start", seats:N}          房主开局
     {type:"state", state:{...}}      房主广播局面
     {type:"intent", seat:i, kind, ...} 客人操作
     {type:"ping"}                    心跳
   服务端 -> 客户端
     {type:"hello", id, seat, members} 分配座位
     {type:"members", n, assign?, seat?} 人数变化
     {type:"start"|"state"|"intent"}   原样转发给同房间其他人
     {type:"error", msg}

部署
   pip install fastapi uvicorn
   uvicorn server_app:app --host 0.0.0.0 --port 8787
   Nginx 反代并开启 WebSocket 升级（wss://你的域名/ws/<房间码>）
   小程序后台把 wss://你的域名 加入 socket 合法域名（需已备案域名 + HTTPS 证书）

   一键部署：bash deploy.sh（Ubuntu/Debian，自动装依赖 + systemd + Nginx + 证书）

环境变量（均可选）
   PORT / RELAY_PORT   监听端口，默认 8000
   MAX_SEATS           每房人数上限，默认 4
   ROOM_TTL_HOURS      空房回收时长（小时），默认 6
   RELAY_TOKEN         设置后客户端须带 ?token=xxx，防止房号被扫

运行
   python3 server_app.py            # 默认 0.0.0.0:8000
   PORT=9000 python3 server_app.py
"""
import asyncio
import json
import os
import re
import time
from typing import Dict, List, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

MAX_SEATS = int(os.environ.get("MAX_SEATS", "4"))
ROOM_TTL = int(float(os.environ.get("ROOM_TTL_HOURS", "6")) * 3600)   # 房间空闲回收
RELAY_TOKEN = os.environ.get("RELAY_TOKEN", "")                       # 非空则强制校验
CODE_RE = re.compile(r"^[A-Z0-9]{3,8}$")


class Peer:
    __slots__ = ("ws", "id", "seat", "room", "last")

    def __init__(self, ws: WebSocket, pid: str, room: str):
        self.ws = ws
        self.id = pid
        self.room = room
        self.seat = -1
        self.last = time.time()

    async def send(self, obj: dict) -> bool:
        try:
            await self.ws.send_text(json.dumps(obj, ensure_ascii=False))
            return True
        except Exception:
            return False


class Room:
    def __init__(self, code: str):
        self.code = code
        self.peers: List[Peer] = []
        self.created = time.time()
        self.touched = time.time()

    def seat_of(self, pid: str) -> int:
        for p in self.peers:
            if p.id == pid:
                return p.seat
        return -1

    def next_seat(self) -> int:
        used = {p.seat for p in self.peers if p.seat >= 0}
        for i in range(MAX_SEATS):
            if i not in used:
                return i
        return -1

    def others(self, exclude: Peer) -> List[Peer]:
        return [p for p in self.peers if p is not exclude]


ROOMS: Dict[str, Room] = {}
app = FastAPI(title="Splendor Pokemon Relay")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def index():
    return {
        "ok": True,
        "rooms": len(ROOMS),
        "usage": "wss://<host>/ws/<ROOM_CODE>",
    }


@app.get("/health")
async def health():
    return {"ok": True, "rooms": len(ROOMS), "ts": int(time.time())}


async def broadcast(room: Room, msg: dict, exclude: Optional[Peer] = None):
    for p in room.others(exclude) if exclude else room.peers:
        await p.send(msg)


@app.websocket("/ws/{code}")
async def ws_room(ws: WebSocket, code: str, token: str = ""):
    if RELAY_TOKEN and token != RELAY_TOKEN:
        await ws.close(code=4003)
        return
    code = (code or "").upper()
    if not CODE_RE.match(code):
        await ws.close(code=4001)
        return
    await ws.accept()

    room = ROOMS.get(code)
    if room is None:
        room = ROOMS[code] = Room(code)

    if len(room.peers) >= MAX_SEATS:
        await ws.send_text(json.dumps({"type": "error", "msg": "房间已满（最多 4 人）"}))
        await ws.close(code=4002)
        return

    pid = f"{int(time.time()*1000)%10**9}_{len(room.peers)}"
    peer = Peer(ws, pid, code)
    peer.seat = room.next_seat()
    room.peers.append(peer)
    room.touched = time.time()

    # 分配座位
    await peer.send({"type": "hello", "id": pid, "seat": peer.seat, "members": len(room.peers)})
    await broadcast(room, {"type": "members", "n": len(room.peers), "assign": pid, "seat": peer.seat}, peer)

    try:
        while True:
            raw = await ws.receive_text()
            peer.last = time.time()
            room.touched = time.time()
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            t = msg.get("type")

            if t == "ping":
                await peer.send({"type": "pong"})
                continue

            if t == "hello":
                # 客户端主动声明身份：补发座位
                await peer.send({"type": "hello", "id": pid, "seat": peer.seat, "members": len(room.peers)})
                continue

            if t == "members":
                continue

            if t == "intent":
                # 只接受本人座位发出的操作，并只转发给房主（seat 0）
                if msg.get("seat") != peer.seat:
                    continue
                msg["seat"] = peer.seat
                msg["from"] = pid
                for p in room.peers:
                    if p.seat == 0 and p is not peer:
                        await p.send(msg)
                continue

            if t in ("start", "state"):
                # 只允许房主广播
                if peer.seat != 0:
                    continue
                await broadcast(room, msg, peer)
                continue
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        if peer in room.peers:
            room.peers.remove(peer)
        room.touched = time.time()
        if room.peers:
            await broadcast(room, {"type": "members", "n": len(room.peers)})
        else:
            ROOMS.pop(code, None)


async def reaper():
    """回收长时间无人活动的房间"""
    while True:
        await asyncio.sleep(300)
        now = time.time()
        for code in list(ROOMS.keys()):
            r = ROOMS.get(code)
            if r and not r.peers and now - r.touched > ROOM_TTL:
                ROOMS.pop(code, None)


@app.on_event("startup")
async def _startup():
    asyncio.create_task(reaper())


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
