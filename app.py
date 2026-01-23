# QuantDesk Bookmap Service (Replit/GitHub) — FIX2
# Goals:
# 1) Smooth, stable Bookmap-like scroll/zoom (no "pressed to left" drift)
# 2) Correct heat palette: weak=light blue, strong=red, none=black
# 3) Heat dimmer acts like a "vmax/exposure" control (keeps dense bands visible, doesn't nuke everything)
# 4) Reduce over-crowding: client uses server trade list as authoritative (no duplicate appends)
# 5) Performance: reuse offscreen canvas + ImageData buffers; no per-frame canvas allocations

import os
import json
import time
import gzip
import zlib
import asyncio
import base64
import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from collections import deque

import websockets
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse


# -----------------------------
# Config (env)
# -----------------------------
WS_URL = os.environ.get("QD_WS_URL", "wss://contract.mexc.com/edge")
SYMBOL_WS = os.environ.get("QD_SYMBOL_WS", "BTC_USDT")
PORT = int(os.environ.get("PORT", "5000"))

MAX_TRADES = int(os.environ.get("QD_MAX_TRADES", "6000"))

TOP_LEVELS = int(os.environ.get("QD_TOP_LEVELS", "250"))
SNAPSHOT_HZ = float(os.environ.get("QD_SNAPSHOT_HZ", "6"))

HEAT_WINDOW_SEC = int(os.environ.get("QD_HEAT_WINDOW_SEC", "3600"))
HEAT_COL_MS = int(os.environ.get("QD_HEAT_COL_MS", "1000"))

PRICE_BIN_USD = float(os.environ.get("QD_PRICE_BIN_USD", "1.0"))
RANGE_USD = float(os.environ.get("QD_RANGE_USD", "4000"))
DEFAULT_PRICE_SPAN = float(os.environ.get("QD_DEFAULT_PRICE_SPAN", "600.0"))

HEAT_LOG_SCALE = float(os.environ.get("QD_HEAT_LOG_SCALE", "32.0"))
HEAT_CELL_MAX = int(os.environ.get("QD_HEAT_CELL_MAX", "255"))

# server-side optional decay (keeps very old resting levels from saturating to 255)
HEAT_DECAY_PER_COL = float(os.environ.get("QD_HEAT_DECAY_PER_COL", "0.985"))  # 1.0 disables
HEAT_BOOK_SAMPLE_LIMIT = int(os.environ.get("QD_HEAT_BOOK_SAMPLE_LIMIT", "0"))  # 0 => full book

# -----------------------------
# Helpers
# -----------------------------
def now_ms() -> int:
    return int(time.time() * 1000)

def try_decompress(payload: bytes) -> bytes:
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

def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


# -----------------------------
# State
# -----------------------------
@dataclass
class OrderBook:
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)

    def apply_side_updates(self, side: str, levels: List[List[Any]]) -> None:
        # MEXC push.depth levels: [price, qty, count]; qty==0 => remove
        book = self.bids if side == "bids" else self.asks
        for lvl in levels:
            if not lvl or len(lvl) < 2:
                continue
            p = safe_float(lvl[0])
            q = safe_float(lvl[1])
            if p <= 0:
                continue
            if q <= 0:
                book.pop(p, None)
            else:
                book[p] = q

    def best_bid_ask(self) -> Tuple[Optional[float], Optional[float]]:
        bb = max(self.bids.keys()) if self.bids else None
        ba = min(self.asks.keys()) if self.asks else None
        return bb, ba

    def top_n(self, n: int) -> Dict[str, List[Tuple[float, float]]]:
        bids_sorted = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]
        asks_sorted = sorted(self.asks.items(), key=lambda x: x[0], reverse=False)[:n]
        return {"bids": bids_sorted, "asks": asks_sorted}

    def iter_sampled(self) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        """Optionally sample book (for CPU control). If limit==0, return full."""
        lim = int(HEAT_BOOK_SAMPLE_LIMIT)
        if lim <= 0:
            return list(self.bids.items()), list(self.asks.items())
        # sample by taking top lim by qty (roughly highlights dominant resting liquidity)
        bids = sorted(self.bids.items(), key=lambda x: x[1], reverse=True)[:lim]
        asks = sorted(self.asks.items(), key=lambda x: x[1], reverse=True)[:lim]
        return bids, asks

@dataclass
class Trades:
    dq: deque = field(default_factory=lambda: deque(maxlen=MAX_TRADES))

    def append(self, t: Dict[str, Any]) -> None:
        self.dq.append(t)


class HeatmapRing:
    """
    Absolute price grid heat storage:
      - rows = fixed price bins (anchor_min_price + i*bin_usd)
      - cols = fixed time columns (every col_ms), stored in a ring
    Server sends patches (one new column per head advance).
    """
    def __init__(self, bin_usd: float, range_usd: float, window_sec: int, col_ms: int) -> None:
        self.bin_usd = float(bin_usd)
        self.range_usd = float(range_usd)
        self.window_sec = int(window_sec)
        self.col_ms = int(col_ms)

        self.cols = max(30, int((self.window_sec * 1000) // max(250, self.col_ms)))
        half = max(50.0, self.range_usd / 2.0)
        self.rows = max(200, int((2.0 * half) / max(0.25, self.bin_usd)))

        self._anchor_min_price: Optional[float] = None
        self._anchor_min_bin: Optional[int] = None
        self._head_col_idx: int = -1
        self._grid: List[Optional[bytearray]] = [None] * self.cols
        self._last_col_open_ms: Optional[int] = None

    @property
    def anchor_min_price(self) -> Optional[float]:
        return self._anchor_min_price

    @property
    def anchor_min_bin(self) -> Optional[int]:
        return self._anchor_min_bin

    @property
    def head(self) -> int:
        return self._head_col_idx

    def _price_to_bin_index(self, p: float) -> int:
        return int(math.floor(p / self.bin_usd))

    def ensure_anchor(self, mid_px: float) -> None:
        if self._anchor_min_price is not None:
            return
        half = self.range_usd / 2.0
        min_price = max(0.0, mid_px - half)
        min_bin = self._price_to_bin_index(min_price)
        self._anchor_min_bin = min_bin
        self._anchor_min_price = float(min_bin) * self.bin_usd

    def _row_of_price(self, p: float) -> Optional[int]:
        if self._anchor_min_bin is None:
            return None
        b = self._price_to_bin_index(p)
        row = b - self._anchor_min_bin
        if 0 <= row < self.rows:
            return row
        return None

    def maybe_open_new_column(self, t_ms: int) -> Optional[int]:
        """
        Opens new columns whenever col_ms boundary passes.
        Returns new head col_idx if opened, else None.
        """
        if self._last_col_open_ms is None:
            self._last_col_open_ms = t_ms
            self._head_col_idx += 1
            self._grid[self._head_col_idx % self.cols] = bytearray(self.rows)
            return self._head_col_idx

        if (t_ms - self._last_col_open_ms) >= self.col_ms:
            steps = int((t_ms - self._last_col_open_ms) // self.col_ms)
            for _ in range(steps):
                self._last_col_open_ms += self.col_ms
                self._head_col_idx += 1
                self._grid[self._head_col_idx % self.cols] = bytearray(self.rows)
            return self._head_col_idx
        return None

    def _apply_decay_to_current(self) -> None:
        # Decay previous column into current one (so bands persist but don't hard-saturate)
        if HEAT_DECAY_PER_COL >= 0.9999:
            return
        if self._head_col_idx <= 0:
            return
        prev = self._grid[(self._head_col_idx - 1) % self.cols]
        cur = self._grid[self._head_col_idx % self.cols]
        if prev is None or cur is None:
            return
        k = float(HEAT_DECAY_PER_COL)
        # cur = prev * k (uint8)
        for i in range(self.rows):
            v = int(prev[i] * k)
            cur[i] = v if v <= 255 else 255

    def write_level(self, p: float, q: float) -> None:
        if self._head_col_idx < 0:
            return
        row = self._row_of_price(p)
        if row is None:
            return
        col = self._grid[self._head_col_idx % self.cols]
        if col is None:
            col = bytearray(self.rows)
            self._grid[self._head_col_idx % self.cols] = col

        inten = math.log1p(max(0.0, q)) * HEAT_LOG_SCALE
        v = int(inten)
        if v <= 0:
            return
        if v > HEAT_CELL_MAX:
            v = HEAT_CELL_MAX
        nv = col[row] + v
        col[row] = HEAT_CELL_MAX if nv > HEAT_CELL_MAX else nv

    def export_patch_b64(self, col_idx: int) -> Optional[str]:
        col = self._grid[col_idx % self.cols] if col_idx >= 0 else None
        if col is None:
            return None
        return base64.b64encode(bytes(col)).decode("ascii")

    def get_col(self, col_idx: int) -> Optional[bytearray]:
        return self._grid[col_idx % self.cols] if col_idx >= 0 else None


class MexcFeed:
    def __init__(self) -> None:
        self.book = OrderBook()
        self.trades = Trades()

        self.ws_ok = False
        self.last_px: Optional[float] = None
        self.last_trade_ms: Optional[int] = None
        self.last_depth_ms: Optional[int] = None

        self.best_bid: Optional[float] = None
        self.best_ask: Optional[float] = None
        self.mid_px: Optional[float] = None

        self._stop = False
        self._clients: List[WebSocket] = []
        self._clients_lock = asyncio.Lock()

        self.heat = HeatmapRing(
            bin_usd=PRICE_BIN_USD,
            range_usd=RANGE_USD,
            window_sec=HEAT_WINDOW_SEC,
            col_ms=HEAT_COL_MS,
        )
        self._last_patch_sent_head: int = -1

    async def add_client(self, ws: WebSocket) -> None:
        async with self._clients_lock:
            self._clients.append(ws)

    async def remove_client(self, ws: WebSocket) -> None:
        async with self._clients_lock:
            self._clients = [c for c in self._clients if c is not ws]

    def _update_bba_mid(self) -> None:
        bb, ba = self.book.best_bid_ask()
        self.best_bid = bb
        self.best_ask = ba
        if bb is not None and ba is not None and bb > 0 and ba > 0:
            self.mid_px = (bb + ba) / 2.0
        elif self.last_px is not None:
            self.mid_px = self.last_px

    async def broadcast_snapshot(self) -> None:
        self._update_bba_mid()
        book = self.book.top_n(TOP_LEVELS)
        trades = list(self.trades.dq)[-1500:]  # authoritative snapshot

        if self.mid_px is not None:
            self.heat.ensure_anchor(self.mid_px)

        heat_patch = None
        if self.heat.anchor_min_price is not None and self.heat.head >= 0:
            if self.heat.head != self._last_patch_sent_head:
                b64 = self.heat.export_patch_b64(self.heat.head)
                if b64 is not None:
                    heat_patch = {"col_idx": self.heat.head, "b64": b64}
                self._last_patch_sent_head = self.heat.head

        payload = {
            "ts": now_ms(),
            "symbol": SYMBOL_WS,
            "ws_url": WS_URL,
            "ws_ok": self.ws_ok,
            "last_px": self.last_px,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "mid_px": self.mid_px,
            "book": {"bids": book["bids"], "asks": book["asks"]},
            "trades": trades,
            "health": {
                "depth_age_ms": (now_ms() - self.last_depth_ms) if self.last_depth_ms else None,
                "trade_age_ms": (now_ms() - self.last_trade_ms) if self.last_trade_ms else None,
                "bids_n": len(self.book.bids),
                "asks_n": len(self.book.asks),
                "trades_n": len(self.trades.dq),
            },
            "heat_meta": {
                "mode": "resting_density_absolute_grid",
                "window_sec": HEAT_WINDOW_SEC,
                "col_ms": HEAT_COL_MS,
                "cols": self.heat.cols,
                "bin_usd": self.heat.bin_usd,
                "range_usd": self.heat.range_usd,
                "rows": self.heat.rows,
                "anchor_min_price": self.heat.anchor_min_price,
                "anchor_min_bin": self.heat.anchor_min_bin,
                "head": self.heat.head,
            },
            "heat_patch": heat_patch,
            "ui_defaults": {
                "default_price_span": DEFAULT_PRICE_SPAN,
            },
        }

        msg = json.dumps(payload)
        async with self._clients_lock:
            clients = list(self._clients)

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
        period = max(0.05, 1.0 / max(1.0, SNAPSHOT_HZ))
        while not self._stop:
            try:
                await self.broadcast_snapshot()
            except Exception:
                pass
            await asyncio.sleep(period)

    async def _heat_loop(self) -> None:
        while not self._stop:
            try:
                t = now_ms()
                self._update_bba_mid()
                if self.mid_px is not None:
                    self.heat.ensure_anchor(self.mid_px)

                new_head = self.heat.maybe_open_new_column(t)
                if new_head is not None:
                    # decay previous -> current (prevents hard saturation)
                    self.heat._apply_decay_to_current()

                    # sample current resting book and write into current column
                    bids, asks = self.book.iter_sampled()
                    for p, q in bids:
                        if q > 0:
                            self.heat.write_level(p, q)
                    for p, q in asks:
                        if q > 0:
                            self.heat.write_level(p, q)
            except Exception:
                pass
            await asyncio.sleep(0.05)

    async def run(self) -> None:
        asyncio.create_task(self._snapshot_loop())
        asyncio.create_task(self._heat_loop())

        while not self._stop:
            try:
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                    self.ws_ok = True
                    await ws.send(json.dumps({"method": "sub.depth", "param": {"symbol": SYMBOL_WS}}))
                    await ws.send(json.dumps({"method": "sub.deal", "param": {"symbol": SYMBOL_WS}}))

                    while not self._stop:
                        raw = await ws.recv()
                        if isinstance(raw, bytes):
                            raw = try_decompress(raw).decode("utf-8", errors="ignore")
                        msg = json.loads(raw)

                        ch = msg.get("channel") or msg.get("ch")

                        if ch in ("push.depth", "rs.push.depth"):
                            data = msg.get("data", {}) or {}
                            self.book.apply_side_updates("bids", data.get("bids", []) or [])
                            self.book.apply_side_updates("asks", data.get("asks", []) or [])
                            self.last_depth_ms = now_ms()
                            self._update_bba_mid()

                        elif ch in ("push.deal", "rs.push.deal"):
                            data = msg.get("data", [])
                            if isinstance(data, list):
                                for t in data:
                                    p = safe_float(t.get("p"))
                                    v = safe_float(t.get("v"))
                                    T = safe_int(t.get("T", 0))
                                    tm = safe_int(t.get("t", now_ms()))
                                    self.trades.append({"p": p, "v": v, "T": T, "t": tm})
                                    if p > 0:
                                        self.last_px = p
                                    self.last_trade_ms = now_ms()
                            self._update_bba_mid()

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
  <meta name="viewport" content="width=device-width,initial-scale=1, maximum-scale=1, user-scalable=no" />
  <title>QuantDesk Bookmap (FIX2)</title>
  <style>
    html, body { margin:0; padding:0; background:#0b0f14; color:#cbd5e1; height:100%; overflow:hidden; }
    #topbar {
      padding:10px 12px;
      font-family: -apple-system, system-ui, Arial;
      font-size:14px;
      display:flex;
      gap:10px;
      align-items:center;
      border-bottom:1px solid #111827;
      user-select:none;
      -webkit-user-select:none;
      flex-wrap:wrap;
    }
    #statusDot { width:10px; height:10px; border-radius:50%; display:inline-block; background:#ef4444; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size:12px; color:#93c5fd; }
    button {
      padding:6px 10px;
      border:1px solid #1f2937;
      border-radius:10px;
      background:#0b1220;
      color:#cbd5e1;
      font-size:12px;
    }
    .ctl { display:flex; align-items:center; gap:8px; }
    input[type="range"] { width: 160px; }
    #wrap { height: calc(100% - 96px); display:flex; }
    canvas { width: 100%; height: 100%; display:block; touch-action:none; }
  </style>
</head>
<body>
  <div id="topbar">
    <span id="statusDot"></span>
    <span class="mono" id="sym"></span>
    <span class="mono" id="health"></span>
    <button id="btnAF">AutoFollow: ON</button>
    <button id="btnTm">-T</button>
    <button id="btnTp">+T</button>
    <button id="btnPm">-P</button>
    <button id="btnPp">+P</button>
  </div>

  <div id="topbar" style="border-bottom:1px solid #111827;">
    <button id="btnAutoExp">AutoExp: ON</button>

    <div class="ctl"><span class="mono">Heat Dim</span>
      <input id="slDim" type="range" min="0" max="100" value="20"/>
    </div>
    <div class="ctl"><span class="mono">Heat Thr</span>
      <input id="slThr" type="range" min="0" max="100" value="6"/>
    </div>
    <div class="ctl"><span class="mono">Heat Ctr</span>
      <input id="slCtr" type="range" min="0" max="100" value="50"/>
    </div>

    <div class="ctl"><span class="mono">Bub Op</span>
      <input id="slBubOp" type="range" min="0" max="100" value="65"/>
    </div>
    <div class="ctl"><span class="mono">Bub Sz</span>
      <input id="slBubSz" type="range" min="10" max="200" value="100"/>
    </div>
    <div class="ctl"><span class="mono">Bub Min</span>
      <input id="slBubMin" type="range" min="0" max="100" value="2"/>
    </div>
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

  const btnAF = document.getElementById("btnAF");
  const btnTm = document.getElementById("btnTm");
  const btnTp = document.getElementById("btnTp");
  const btnPm = document.getElementById("btnPm");
  const btnPp = document.getElementById("btnPp");

  const btnAutoExp = document.getElementById("btnAutoExp");
  const slDim = document.getElementById("slDim");
  const slThr = document.getElementById("slThr");
  const slCtr = document.getElementById("slCtr");
  const slBubOp = document.getElementById("slBubOp");
  const slBubSz = document.getElementById("slBubSz");
  const slBubMin = document.getElementById("slBubMin");

  function resize() {
    const dpr = window.devicePixelRatio || 1;
    cv.width = Math.floor(cv.clientWidth * dpr);
    cv.height = Math.floor(cv.clientHeight * dpr);
    // draw in device pixels; no transform drift
    ctx.setTransform(1,0,0,1,0,0);
  }
  window.addEventListener("resize", resize);
  resize();

  // -------------- Data state --------------
  let last = null;
  let trades = [];
  let lastPx = null;
  let midPx = null;

  // time window
  let timeSpanSec = 90;
  const MIN_TSPAN = 30;
  const MAX_TSPAN = 60 * 60;

  // price view
  let autoFollow = true;
  let viewMid = null;
  let viewSpan = null;
  const MIN_PSPAN = 40;
  const MAX_PSPAN = 20000;

  // heat ring
  let heat = {
    ready: false,
    windowSec: null,
    colMs: null,
    cols: null,
    binUsd: null,
    rangeUsd: null,
    rows: null,
    anchorMinPrice: null,
    head: -1,
    ring: [],
  };

  // UI controls
  let autoExp = true;
  let heatDim = 0.20;    // acts on vmax (lower -> brighter)
  let heatThr = 0.06;    // per-pixel alpha threshold
  let heatCtr = 0.50;    // gamma/contrast
  let bubOp = 0.65;
  let bubSz = 1.00;
  let bubMin = 0.02;

  function clamp(x, lo, hi) { return Math.max(lo, Math.min(hi, x)); }

  function setFromSliders() {
    heatDim = clamp(parseInt(slDim.value,10)/100, 0.0, 1.0);
    heatThr = clamp(parseInt(slThr.value,10)/100, 0.0, 1.0);
    heatCtr = clamp(parseInt(slCtr.value,10)/100, 0.0, 1.0);
    bubOp = clamp(parseInt(slBubOp.value,10)/100, 0.0, 1.0);
    bubSz = clamp(parseInt(slBubSz.value,10)/100, 0.1, 2.0);
    bubMin = clamp(parseInt(slBubMin.value,10)/100, 0.0, 1.0);
  }
  slDim.oninput = setFromSliders;
  slThr.oninput = setFromSliders;
  slCtr.oninput = setFromSliders;
  slBubOp.oninput = setFromSliders;
  slBubSz.oninput = setFromSliders;
  slBubMin.oninput = setFromSliders;
  setFromSliders();

  btnAutoExp.onclick = () => {
    autoExp = !autoExp;
    btnAutoExp.textContent = `AutoExp: ${autoExp ? "ON" : "OFF"}`;
  };

  btnAF.onclick = () => {
    autoFollow = !autoFollow;
    btnAF.textContent = `AutoFollow: ${autoFollow ? "ON" : "OFF"}`;
  };

  btnTm.onclick = () => { timeSpanSec = clamp(Math.floor(timeSpanSec / 1.5), MIN_TSPAN, MAX_TSPAN); };
  btnTp.onclick = () => { timeSpanSec = clamp(Math.floor(timeSpanSec * 1.5), MIN_TSPAN, MAX_TSPAN); };
  btnPm.onclick = () => { viewSpan = clamp(viewSpan * 0.75, MIN_PSPAN, MAX_PSPAN); };
  btnPp.onclick = () => { viewSpan = clamp(viewSpan * 1.33, MIN_PSPAN, MAX_PSPAN); };

  // -------------- WebSocket --------------
  function wsUrl() {
    const proto = (location.protocol === "https:") ? "wss" : "ws";
    return `${proto}://${location.host}/ws`;
  }
  const WS = new WebSocket(wsUrl());

  function b64ToU8(b64) {
    const bin = atob(b64);
    const len = bin.length;
    const out = new Uint8Array(len);
    for (let i=0;i<len;i++) out[i] = bin.charCodeAt(i);
    return out;
  }

  function initHeat(meta) {
    heat.windowSec = meta.window_sec;
    heat.colMs = meta.col_ms;
    heat.cols = meta.cols;
    heat.binUsd = meta.bin_usd;
    heat.rangeUsd = meta.range_usd;
    heat.rows = meta.rows;
    heat.anchorMinPrice = meta.anchor_min_price;
    heat.head = meta.head;
    heat.ring = new Array(heat.cols).fill(null);
    heat.ready = !!(heat.anchorMinPrice !== null && heat.rows && heat.cols);
  }

  WS.onmessage = (ev) => {
    const msg = JSON.parse(ev.data);
    last = msg;

    dot.style.background = msg.ws_ok ? "#22c55e" : "#ef4444";

    lastPx = (msg.last_px ?? lastPx);
    midPx = (msg.mid_px ?? midPx);

    // show "actual" price sources: trade, mid, bid/ask
    const bid = (msg.best_bid ?? null);
    const ask = (msg.best_ask ?? null);
    const primaryPx = (midPx ?? lastPx);
    const primaryTag = (midPx !== null && midPx !== undefined) ? "MID" : "TRADE";
    symEl.textContent = `${msg.symbol} | PRIMARY(${primaryTag})=${(primaryPx ?? "None")} | TRADE=${(lastPx ?? "None")} | MID=${(midPx ?? "None")} | bid=${(bid ?? "None")} ask=${(ask ?? "None")}`;

    healthEl.textContent =
      `book: bids=${msg.health.bids_n} asks=${msg.health.asks_n} age: depth=${msg.health.depth_age_ms ?? "None"}ms trade=${msg.health.trade_age_ms ?? "None"}ms`;

    if (viewMid === null && (midPx || lastPx)) {
      viewMid = (midPx ?? lastPx);
      viewSpan = (msg.ui_defaults?.default_price_span ?? 600.0);
    }

    if (!heat.ready && msg.heat_meta && msg.heat_meta.anchor_min_price !== null) {
      initHeat(msg.heat_meta);
    } else if (heat.ready && msg.heat_meta) {
      heat.head = msg.heat_meta.head;
      heat.anchorMinPrice = msg.heat_meta.anchor_min_price ?? heat.anchorMinPrice;
    }

    if (heat.ready && msg.heat_patch && msg.heat_patch.b64) {
      const colIdx = msg.heat_patch.col_idx;
      const u8 = b64ToU8(msg.heat_patch.b64);
      if (u8.length === heat.rows) {
        heat.ring[colIdx % heat.cols] = u8;
      }
    }

    // IMPORTANT: trades snapshot is authoritative; do NOT append (prevents drift/crowding)
    if (Array.isArray(msg.trades)) {
      trades = msg.trades;
    }
  };

  WS.onclose = () => { dot.style.background = "#ef4444"; };

  // -------------- Interaction (pointer) --------------
  let pointers = new Map();
  let pinchStartDist = null;
  let pinchStartSpan = null;
  let dragStartY = null;
  let dragStartMid = null;

  cv.addEventListener("pointerdown", (e) => {
    cv.setPointerCapture(e.pointerId);
    pointers.set(e.pointerId, {x: e.clientX, y: e.clientY});
    if (pointers.size === 1) { dragStartY = e.clientY; dragStartMid = viewMid; }
    if (pointers.size === 2) {
      const pts = Array.from(pointers.values());
      const dx = pts[0].x - pts[1].x, dy = pts[0].y - pts[1].y;
      pinchStartDist = Math.sqrt(dx*dx + dy*dy);
      pinchStartSpan = viewSpan;
    }
  });

  cv.addEventListener("pointermove", (e) => {
    if (!pointers.has(e.pointerId)) return;
    pointers.set(e.pointerId, {x: e.clientX, y: e.clientY});
    if (viewMid === null || viewSpan === null) return;

    if (pointers.size === 1 && dragStartY !== null) {
      autoFollow = false; btnAF.textContent = "AutoFollow: OFF";
      const dy = e.clientY - dragStartY;
      const pxPerPrice = (cv.clientHeight) / viewSpan;
      const dPrice = dy / Math.max(1, pxPerPrice);
      viewMid = (dragStartMid ?? viewMid) + dPrice;
    }

    if (pointers.size === 2 && pinchStartDist && pinchStartSpan) {
      autoFollow = false; btnAF.textContent = "AutoFollow: OFF";
      const pts = Array.from(pointers.values());
      const dx = pts[0].x - pts[1].x, dy = pts[0].y - pts[1].y;
      const dist = Math.sqrt(dx*dx + dy*dy);
      const scale = pinchStartDist / Math.max(10, dist);
      viewSpan = clamp(pinchStartSpan * scale, MIN_PSPAN, MAX_PSPAN);
    }
  });

  function endPointer(e) {
    pointers.delete(e.pointerId);
    if (pointers.size < 2) { pinchStartDist = null; pinchStartSpan = null; }
    if (pointers.size === 0) { dragStartY = null; dragStartMid = null; }
  }
  cv.addEventListener("pointerup", endPointer);
  cv.addEventListener("pointercancel", endPointer);
  cv.addEventListener("pointerout", endPointer);

  cv.addEventListener("wheel", (e) => {
    if (viewMid === null || viewSpan === null) return;
    e.preventDefault();
    autoFollow = false; btnAF.textContent = "AutoFollow: OFF";
    const factor = (e.deltaY > 0) ? 1.10 : 0.90;
    viewSpan = clamp(viewSpan * factor, MIN_PSPAN, MAX_PSPAN);
  }, {passive:false});

  // -------------- Rendering pipeline (reused buffers) --------------
  const off = document.createElement("canvas");
  const octx = off.getContext("2d");
  let img = null; // ImageData
  let imgW = 0, imgH = 0;

  function ensureImg(w, h) {
    if (!img || imgW !== w || imgH !== h) {
      imgW = w; imgH = h;
      off.width = w; off.height = h;
      img = octx.createImageData(w, h);
    }
    // clear alpha quickly
    img.data.fill(0);
    return img;
  }

  function yOf(p, pMin, pMax, h) {
    const t = (p - pMin) / (pMax - pMin);
    return h - t*h;
  }

  function rowOfPrice(p) {
    return Math.floor((p - heat.anchorMinPrice) / heat.binUsd);
  }

  // Bookmap-like palette: light blue -> cyan -> yellow -> orange -> red (strongest)
  function heatRGBA(a) {
    // apply contrast (gamma) around center
    // heatCtr=0..1 => gamma range [0.6..2.2] (higher => more contrast)
    const gamma = 0.6 + (2.2 - 0.6) * heatCtr;
    const g = Math.pow(clamp(a,0,1), gamma);

    // piecewise gradient
    let r=0, gg=0, b=0, alpha=0;
    if (g < 0.25) {
      // very weak: light blue
      const t = g / 0.25;
      r = 30 + 20*t;      // 30 -> 50
      gg = 120 + 80*t;    // 120 -> 200
      b = 220 + 20*(1-t); // 220 -> 200
      alpha = 0.10 + 0.30*t;
    } else if (g < 0.50) {
      // cyan -> yellow
      const t = (g - 0.25) / 0.25;
      r = 50 + 180*t;     // 50 -> 230
      gg = 200 + 40*t;    // 200 -> 240
      b = 200*(1-t);      // 200 -> 0
      alpha = 0.25 + 0.35*t;
    } else if (g < 0.75) {
      // yellow -> orange
      const t = (g - 0.50) / 0.25;
      r = 230 + 20*t;     // 230 -> 250
      gg = 240 - 80*t;    // 240 -> 160
      b = 0;
      alpha = 0.45 + 0.35*t;
    } else {
      // orange -> red (strongest)
      const t = (g - 0.75) / 0.25;
      r = 250;
      gg = 160*(1-t);     // 160 -> 0
      b = 0;
      alpha = 0.70 + 0.30*t;
    }
    return [r|0, gg|0, b|0, clamp(alpha, 0, 1)];
  }

  function draw() {
    const w = cv.width;
    const h = cv.height;

    ctx.clearRect(0,0,w,h);
    ctx.fillStyle = "#0b0f14";
    ctx.fillRect(0,0,w,h);

    if (!last || viewMid === null || viewSpan === null) {
      ctx.fillStyle = "#94a3b8";
      ctx.font = "28px -apple-system, system-ui, Arial";
      ctx.fillText("Waiting for data...", 24, 48);
      requestAnimationFrame(draw);
      return;
    }

    if (autoFollow && (midPx || lastPx)) {
      const primaryPx = (midPx ?? lastPx);
      viewMid = primaryPx;
    }

    const pMin = viewMid - viewSpan/2;
    const pMax = viewMid + viewSpan/2;

    // ---- HEATMAP ----
    if (heat.ready && heat.head >= 0 && heat.anchorMinPrice !== null) {
      const colsVisible = clamp(Math.floor((timeSpanSec*1000) / heat.colMs), 10, heat.cols);
      const head = heat.head;
      const startCol = head - colsVisible + 1;

      let rTop = rowOfPrice(pMax);
      let rBot = rowOfPrice(pMin);
      rTop = clamp(rTop, 0, heat.rows-1);
      rBot = clamp(rBot, 0, heat.rows-1);
      if (rBot < rTop) { const tmp=rTop; rTop=rBot; rBot=tmp; }
      const rowsVisible = clamp((rBot - rTop + 1), 50, heat.rows);

      const img = ensureImg(colsVisible, rowsVisible);
      const data = img.data;

      
// Exposure (Bookmap-like): use histogram percentile (NOT max) + dimming as contrast compression
// We treat incoming u8 values as "density" in [0..255].
//  - AutoExp computes a robust percentile-based vmax from sampled recent columns.
//  - Heat Dim moves the target percentile higher (keeps only dense bands).
//  - Heat Thr is applied AFTER normalization as a floor, but we also re-map so bands survive.
//
// Notes:
//  - This prevents global saturation (everything red).
//  - This prevents "uniform dim" behavior; dimming suppresses weak bins first.

function histPercentileValue(hist, pct) {
  const total = hist.reduce((a,b)=>a+b,0);
  if (total <= 0) return 64;
  const target = Math.max(1, Math.floor(total * pct));
  let c = 0;
  for (let i=0;i<hist.length;i++) { c += hist[i]; if (c >= target) return i; }
  return hist.length - 1;
}

let vmax = 64;
if (autoExp) {
  // Build a 0..255 histogram from a sparse sample of recent heat columns.
  // Sampling is O(visible) but bounded.
  const hist = new Array(256).fill(0);
  const sampleCols = Math.min(colsVisible, 120);
  const step = Math.max(1, Math.floor(colsVisible / sampleCols));

  for (let ci = startCol; ci <= head; ci += step) {
    const col = heat.ring[((ci % heat.cols)+heat.cols)%heat.cols];
    if (!col) continue;
    // stride rows to keep CPU stable
    for (let rr = rTop; rr <= rBot; rr += 4) {
      const v = col[rr] || 0;
      if (v > 0) hist[v] += 1;
    }
  }

  // Dim controls the target percentile: low dim => brighter (lower pct), high dim => darker (higher pct).
  // Range chosen to mimic Bookmap exposure behavior.
  const pct = clamp(0.950 + 0.049 * heatDim, 0.90, 0.999); // 0.95 .. ~0.999
  vmax = histPercentileValue(hist, pct);
  vmax = Math.max(vmax, 24);
} else {
  vmax = 96; // fixed fallback
}

// Additional "contrast compression" knee: removes low-intensity clutter as heatDim increases.
// knee in [0..0.35]
const knee = 0.35 * heatDim;

// fill pixels
      // fill pixels
      for (let x = 0; x < colsVisible; x++) {
        const colIdx = startCol + x;
        const col = heat.ring[((colIdx % heat.cols) + heat.cols) % heat.cols];
        if (!col) continue;

        for (let y = 0; y < rowsVisible; y++) {
          const row = rTop + y;
          const v = col[row] || 0;
          if (v === 0) continue;

let a = clamp(v / vmax, 0, 1);

// "knee" removes low-density haze while preserving dense bands.
if (knee > 0) {
  a = clamp((a - knee) / Math.max(1e-6, (1 - knee)), 0, 1);
}

// HeatThr acts as a floor but re-maps so top bands remain visible.
if (a < heatThr) continue;
a = clamp((a - heatThr) / Math.max(1e-6, (1 - heatThr)), 0, 1);

const [rr, gg, bb, aa] = heatRGBA(a);
          const i = (y*colsVisible + x) * 4;
          data[i+0] = rr;
          data[i+1] = gg;
          data[i+2] = bb;
          data[i+3] = Math.floor(255 * aa);
        }
      }

      octx.putImageData(img, 0, 0);
      ctx.imageSmoothingEnabled = true;
      ctx.drawImage(off, 0, 0, w, h);
    }

    // grid
    ctx.strokeStyle = "rgba(148,163,184,0.08)";
    ctx.lineWidth = 1;
    for (let k=1;k<8;k++) {
      const yy = (h/8)*k;
      ctx.beginPath(); ctx.moveTo(0, yy); ctx.lineTo(w, yy); ctx.stroke();
    }

    // ---- Trade bubbles ----
    const now = (last && last.ts) ? last.ts : Date.now();
    const windowMs = timeSpanSec * 1000;
    const xOf = (tms) => {
      const dtm = now - tms;
      const frac = 1 - (dtm / windowMs);
      return clamp(frac, 0, 1) * w;
    };

    let vMax = 0;
    for (const t of trades) vMax = Math.max(vMax, t.v || 0);
    vMax = Math.max(vMax, 1e-9);

    for (const t of trades) {
      const p = t.p;
      if (!(p >= pMin && p <= pMax)) continue;
      const vv = (t.v || 0);
      const vFrac = Math.sqrt(vv / vMax);
      if (vFrac < bubMin) continue;

      const x = xOf(t.t);
      const y = yOf(p, pMin, pMax, h);

      const r = (1.5 + 16 * vFrac) * bubSz;
      const isBuy = (t.T === 1);
      ctx.beginPath();
      ctx.fillStyle = isBuy ? `rgba(59,130,246,${bubOp})` : `rgba(245,158,11,${bubOp})`;
      ctx.arc(x, y, r, 0, Math.PI*2);
      ctx.fill();
    }

    // price lines
    if (lastPx) {
      const y = yOf(lastPx, pMin, pMax, h);
      ctx.strokeStyle = "rgba(226,232,240,0.9)";
      ctx.lineWidth = 2;
      ctx.beginPath(); ctx.moveTo(0,y); ctx.lineTo(w,y); ctx.stroke();

      ctx.fillStyle = "rgba(226,232,240,0.9)";
      ctx.font = "24px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace";
      ctx.fillText(`TRADE ${lastPx.toFixed(1)}`, 24, clamp(y-10, 28, h-16));
    }
    if (midPx) {
      const y = yOf(midPx, pMin, pMax, h);
      ctx.strokeStyle = "rgba(147,197,253,0.45)";
      ctx.lineWidth = 2;
      ctx.setLineDash([10,10]);
      ctx.beginPath(); ctx.moveTo(0,y); ctx.lineTo(w,y); ctx.stroke();
      ctx.setLineDash([]);

      ctx.fillStyle = "rgba(147,197,253,0.9)";
      ctx.font = "24px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace";
      ctx.fillText(`MID   ${midPx.toFixed(1)}`, 24, clamp(y-10, 28, h-16));
    }

    // footer
    ctx.fillStyle = "rgba(148,163,184,0.95)";
    ctx.font = "22px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace";
    ctx.fillText(
      `Tspan=${timeSpanSec.toFixed(0)}s | Pspan=${viewSpan.toFixed(1)} | AutoExp=${autoExp?"ON":"OFF"} | Dim=${heatDim.toFixed(2)} Thr=${heatThr.toFixed(2)} Ctr=${heatCtr.toFixed(2)}`,
      24, h - 20
    );

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
    feed._update_bba_mid()
    return {
        "service": "quantdesk-bookmap-ui",
        "symbol": SYMBOL_WS,
        "ws_url": WS_URL,
        "ws_ok": feed.ws_ok,
        "last_px": feed.last_px,
        "mid_px": feed.mid_px,
        "best_bid": feed.best_bid,
        "best_ask": feed.best_ask,
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
        while True:
            await asyncio.sleep(10)
    except Exception:
        pass
    finally:
        await feed.remove_client(ws)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=PORT)
