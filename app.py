import os
import json
import time
import gzip
import zlib
import asyncio
import traceback
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from collections import deque

import websockets
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse, JSONResponse

# -----------------------------
# Config (env)
# -----------------------------
WS_URL = os.environ.get("QD_WS_URL", "wss://contract.mexc.com/edge")
SYMBOL_WS = os.environ.get("QD_SYMBOL_WS", "BTC_USDT")  # MEXC WS uses BTC_USDT
PORT = int(os.environ.get("PORT", "8000"))

MAX_TRADES = int(os.environ.get("QD_MAX_TRADES", "4000"))
TOP_LEVELS = int(os.environ.get("QD_TOP_LEVELS", "120"))  # depth levels each side
SNAPSHOT_HZ = float(os.environ.get("QD_SNAPSHOT_HZ", "5"))  # UI updates/sec

# -----------------------------
# Helpers
# -----------------------------
def now_ms() -> int:
    return int(time.time() * 1000)

def try_decompress(payload: bytes) -> bytes:
    # MEXC edge WS may send gzip or zlib frames
    try:
        return gzip.decompress(payload)
    except Exception:
        pass
    try:
        return zlib.decompress(payload)
    except Exception:
        return payload

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

# -----------------------------
# State
# -----------------------------
@dataclass
class OrderBook:
    bids: Dict[float, float] = field(default_factory=dict)  # price -> qty
    asks: Dict[float, float] = field(default_factory=dict)

    def apply_side_updates(self, side: str, levels: List[List[Any]]) -> None:
        # push.depth levels are [price, qty, count] (count optional sometimes)
        # qty == 0 => remove
        book = self.bids if side == "bids" else self.asks
        for lvl in levels:
            if not lvl or len(lvl) < 2:
                continue
            p = safe_float(lvl[0])
            q = safe_float(lvl[1])
            if q <= 0:
                if p in book:
                    del book[p]
            else:
                book[p] = q

    def top_n(self, n: int = 50) -> Dict[str, List[Tuple[float, float]]]:
        bids_sorted = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]
        asks_sorted = sorted(self.asks.items(), key=lambda x: x[0], reverse=False)[:n]
        return {"bids": bids_sorted, "asks": asks_sorted}

@dataclass
class Trades:
    dq: deque = field(default_factory=lambda: deque(maxlen=MAX_TRADES))

    def append(self, t: Dict[str, Any]) -> None:
        # expected fields: p (price), v (vol), T (1 buy / 2 sell), t (ms)
        self.dq.append(t)

class MexcFeed:
    def __init__(self) -> None:
        self.book = OrderBook()
        self.trades = Trades()
        self.ws_ok = False
        self.last_px: Optional[float] = None
        self.last_trade_ms: Optional[int] = None
        self.last_depth_ms: Optional[int] = None
        self._stop = False

        # Web UI clients
        self._clients: List[WebSocket] = []
        self._clients_lock = asyncio.Lock()

    async def add_client(self, ws: WebSocket) -> None:
        async with self._clients_lock:
            self._clients.append(ws)

    async def remove_client(self, ws: WebSocket) -> None:
        async with self._clients_lock:
            self._clients = [c for c in self._clients if c is not ws]

    async def broadcast_snapshot(self) -> None:
        # snapshot for UI: top levels + recent trades + last price + health
        book = self.book.top_n(TOP_LEVELS)
        # only send the most recent ~800 trades to keep payload sane
        trades = list(self.trades.dq)[-800:]

        payload = {
            "ts": now_ms(),
            "symbol": SYMBOL_WS,
            "ws_ok": self.ws_ok,
            "last_px": self.last_px,
            "book": {
                "bids": book["bids"],
                "asks": book["asks"],
            },
            "trades": trades,
            "health": {
                "depth_age_ms": (now_ms() - self.last_depth_ms) if self.last_depth_ms else None,
                "trade_age_ms": (now_ms() - self.last_trade_ms) if self.last_trade_ms else None,
                "bids_n": len(self.book.bids),
                "asks_n": len(self.book.asks),
                "trades_n": len(self.trades.dq),
            }
        }

        msg = json.dumps(payload)
        async with self._clients_lock:
            clients = list(self._clients)

        # send to all, drop dead sockets
        dead: List[WebSocket] = []
        for c in clients:
            try:
                await c.send_text(msg)
            except Exception:
                dead.append(c)

        if dead:
            async with self._clients_lock:
                self._clients = [c for c in self._clients if c not in dead]

    async def _snapshot_loop(self) -> None:
        # UI snapshot loop
        period = max(0.05, 1.0 / max(1.0, SNAPSHOT_HZ))
        while not self._stop:
            try:
                await self.broadcast_snapshot()
            except Exception:
                pass
            await asyncio.sleep(period)

    async def run(self) -> None:
        # MEXC WS feed loop
        asyncio.create_task(self._snapshot_loop())

        while not self._stop:
            try:
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                    self.ws_ok = True

                    # subscribe depth + deals (schema aligned with MEXC swap WS)
                    sub_depth = {"method": "sub.depth", "param": {"symbol": SYMBOL_WS}}
                    sub_deal = {"method": "sub.deal", "param": {"symbol": SYMBOL_WS}}

                    await ws.send(json.dumps(sub_depth))
                    await ws.send(json.dumps(sub_deal))

                    while not self._stop:
                        raw = await ws.recv()
                        if isinstance(raw, bytes):
                            raw = try_decompress(raw).decode("utf-8", errors="ignore")

                        msg = json.loads(raw)
                        ch = msg.get("channel") or msg.get("ch")  # defensive

                        # depth updates
                        if ch in ("push.depth", "rs.push.depth"):
                            data = msg.get("data", {})
                            bids = data.get("bids", []) or []
                            asks = data.get("asks", []) or []
                            self.book.apply_side_updates("bids", bids)
                            self.book.apply_side_updates("asks", asks)
                            self.last_depth_ms = now_ms()

                        # deals (trades)
                        if ch in ("push.deal", "rs.push.deal"):
                            data = msg.get("data", [])
                            if isinstance(data, list):
                                for t in data:
                                    # Normalize / ensure expected keys exist
                                    p = safe_float(t.get("p"))
                                    v = safe_float(t.get("v"))
                                    T = int(t.get("T", 0)) if str(t.get("T", "")).isdigit() else t.get("T", 0)
                                    tm = int(t.get("t", now_ms()))
                                    tt = {"p": p, "v": v, "T": T, "t": tm}
                                    self.trades.append(tt)
                                    self.last_px = p if p > 0 else self.last_px
                                    self.last_trade_ms = now_ms()

            except Exception:
                self.ws_ok = False
                await asyncio.sleep(1.5)

# -----------------------------
# Web App
# -----------------------------
app = FastAPI()
feed = MexcFeed()

HTML = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Bookmap UI MVP</title>
  <style>
    html, body { margin:0; padding:0; background:#0b0f14; color:#cbd5e1; height:100%; }
    #topbar { padding:10px 12px; font-family: -apple-system, system-ui, Arial; font-size:14px; display:flex; gap:14px; align-items:center; border-bottom:1px solid #111827;}
    #statusDot { width:10px; height:10px; border-radius:50%; display:inline-block; background:#ef4444; }
    #wrap { height: calc(100% - 44px); display:flex; }
    canvas { width: 100%; height: 100%; display:block; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size:12px; color:#93c5fd; }
  </style>
</head>
<body>
  <div id="topbar">
    <span id="statusDot"></span>
    <span id="sym"></span>
    <span class="mono" id="health"></span>
  </div>
  <div id="wrap">
    <canvas id="cv"></canvas>
  </div>

<script>
(() => {
  const cv = document.getElementById("cv");
  const ctx = cv.getContext("2d");
  const symEl = document.getElementById("sym");
  const dot = document.getElementById("statusDot");
  const healthEl = document.getElementById("health");

  function resize() {
    const dpr = window.devicePixelRatio || 1;
    cv.width = Math.floor(cv.clientWidth * dpr);
    cv.height = Math.floor(cv.clientHeight * dpr);
    ctx.setTransform(dpr,0,0,dpr,0,0);
  }
  window.addEventListener("resize", resize);
  resize();

  let last = null;
  let trades = [];
  let lastPx = null;

  function wsUrl() {
    const proto = (location.protocol === "https:") ? "wss" : "ws";
    return `${proto}://${location.host}/ws`;
  }

  const WS = new WebSocket(wsUrl());

  WS.onmessage = (ev) => {
    const msg = JSON.parse(ev.data);
    last = msg;
    symEl.textContent = msg.symbol + "  |  last_px=" + (msg.last_px ?? "None");
    dot.style.background = msg.ws_ok ? "#22c55e" : "#ef4444";

    healthEl.textContent =
      `bids=${msg.health.bids_n} asks=${msg.health.asks_n} trades=${msg.health.trades_n} ` +
      `depth_age_ms=${msg.health.depth_age_ms ?? "None"} trade_age_ms=${msg.health.trade_age_ms ?? "None"}`;

    lastPx = msg.last_px ?? lastPx;

    // keep trade history window (last 6 minutes)
    const now = Date.now();
    const windowMs = 6 * 60 * 1000;
    if (Array.isArray(msg.trades)) {
      // append only new trades by timestamp
      for (const t of msg.trades) trades.push(t);
    }
    trades = trades.filter(t => (now - t.t) <= windowMs);
  };

  WS.onclose = () => { dot.style.background = "#ef4444"; };

  function draw() {
    const w = cv.clientWidth;
    const h = cv.clientHeight;
    ctx.clearRect(0,0,w,h);

    // background
    ctx.fillStyle = "#0b0f14";
    ctx.fillRect(0,0,w,h);

    if (!last || !last.book) {
      ctx.fillStyle = "#94a3b8";
      ctx.font = "14px -apple-system, system-ui, Arial";
      ctx.fillText("Waiting for data...", 12, 24);
      requestAnimationFrame(draw);
      return;
    }

    // Determine price range from book + lastPx
    const bids = last.book.bids || [];
    const asks = last.book.asks || [];
    let pMin = Infinity, pMax = -Infinity;

    for (const [p,q] of bids) { if (p > 0) { pMin = Math.min(pMin, p); pMax = Math.max(pMax, p);} }
    for (const [p,q] of asks) { if (p > 0) { pMin = Math.min(pMin, p); pMax = Math.max(pMax, p);} }

    if (!isFinite(pMin) || !isFinite(pMax)) {
      pMin = (lastPx || 0) - 200;
      pMax = (lastPx || 0) + 200;
    }

    // add margin
    const mid = lastPx || (pMin + pMax) / 2;
    const span = Math.max(50, (pMax - pMin));
    pMin = mid - span*0.6;
    pMax = mid + span*0.6;

    const yOf = (p) => {
      const t = (p - pMin) / (pMax - pMin);
      return h - t*h;
    };

    // Draw depth bands (resting liquidity)
    // We map qty to alpha and thickness
    function drawBands(levels, isBid) {
      // normalize qty
      let qMax = 0;
      for (const [p,q] of levels) qMax = Math.max(qMax, q);
      qMax = Math.max(qMax, 1e-9);

      for (const [p,q] of levels) {
        const y = yOf(p);
        if (y < -20 || y > h+20) continue;
        const a = Math.min(0.60, 0.08 + 0.52*(q/qMax));
        const thick = Math.min(10, 1 + 9*(q/qMax));

        ctx.fillStyle = isBid ? `rgba(34,197,94,${a})` : `rgba(239,68,68,${a})`;
        ctx.fillRect(0, y - thick/2, w, thick);
      }
    }

    drawBands(bids, true);
    drawBands(asks, false);

    // Draw trade bubbles (time on X, price on Y)
    const now = Date.now();
    const windowMs = 6 * 60 * 1000;
    const xOf = (tms) => {
      const dt = now - tms;
      const frac = 1 - (dt / windowMs);
      return Math.max(0, Math.min(w, frac * w));
    };

    // bubble sizing from volume percentile-ish
    let vMax = 0;
    for (const t of trades) vMax = Math.max(vMax, t.v || 0);
    vMax = Math.max(vMax, 1e-9);

    for (const t of trades) {
      const x = xOf(t.t);
      const y = yOf(t.p);
      if (y < -30 || y > h+30) continue;

      const v = (t.v || 0);
      const r = 2 + 14 * Math.sqrt(v / vMax);

      // T: 1 buy, 2 sell
      const isBuy = (t.T === 1);
      ctx.beginPath();
      ctx.fillStyle = isBuy ? "rgba(59,130,246,0.70)" : "rgba(245,158,11,0.70)";
      ctx.arc(x, y, r, 0, Math.PI*2);
      ctx.fill();
    }

    // Draw last price line
    if (lastPx) {
      const y = yOf(lastPx);
      ctx.strokeStyle = "rgba(226,232,240,0.9)";
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(0,y);
      ctx.lineTo(w,y);
      ctx.stroke();

      ctx.fillStyle = "rgba(226,232,240,0.9)";
      ctx.font = "12px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace";
      ctx.fillText(`PX ${lastPx.toFixed(2)}`, 10, Math.max(12, y-6));
    }

    // Overlay scale text
    ctx.fillStyle = "rgba(148,163,184,0.85)";
    ctx.font = "12px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace";
    ctx.fillText(`range: ${pMin.toFixed(2)} .. ${pMax.toFixed(2)} | window: 6m`, 10, h - 10);

    requestAnimationFrame(draw);
  }

  requestAnimationFrame(draw);
})();
</script>
</body>
</html>
"""

@app.on_event("startup")
async def startup() -> None:
    asyncio.create_task(feed.run())

@app.get("/")
async def index():
    return HTMLResponse(HTML)

@app.get("/health")
async def health():
    return {
        "service": "bookmap-ui-mvp",
        "symbol": SYMBOL_WS,
        "ws_ok": feed.ws_ok,
        "last_px": feed.last_px,
        "bids_n": len(feed.book.bids),
        "asks_n": len(feed.book.asks),
        "trades_n": len(feed.trades.dq),
        "depth_age_ms": (now_ms() - feed.last_depth_ms) if feed.last_depth_ms else None,
        "trade_age_ms": (now_ms() - feed.last_trade_ms) if feed.last_trade_ms else None,
    }

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    await feed.add_client(ws)
    try:
        # keep open; snapshots are pushed from server loop
        while True:
            # client doesn't need to send anything, but keep it alive
            await asyncio.sleep(10)
    except Exception:
        pass
    finally:
        await feed.remove_client(ws)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=PORT)
