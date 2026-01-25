# =========================================================
# QuantDesk Bookmap Service — FIX14_CLOB_SIDE_PARITY_INCREMENTAL_MERGE
# Replit runtime contract:
# - Single entrypoint: current_fix.py
# - FastAPI on 0.0.0.0:5000
# - Always stays up (no hard exit) even if WS feed fails
# - Provides:
#     /            (UI dashboard + render bridge demo)
#     /health.json (health summary)
#     /telemetry.json
#     /book.json   (best bid/ask + depth aggregates)
#     /tape.json   (last trades)
#     /render.json (render-bridge frame contract)
#     /render.ws   (WebSocket stream of render frames)
#     /raw.json    (raw ring buffer; debug)
#     /bootlog.txt (startup + runtime log)
#
# NOTE: This build does not claim Bookmap parity; it formalizes the
#       render-bridge contract required by the next rendering engine phase.
# =========================================================

from __future__ import annotations

import asyncio
import json
import os
import time
import traceback
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional, Tuple

from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse, PlainTextResponse, JSONResponse

# websockets is optional in some Replit envs; we degrade safely.
try:
    import websockets  # type: ignore
except Exception:  # pragma: no cover
    websockets = None  # type: ignore


# ----------------------------
# Runtime configuration
# ----------------------------
BUILD = "FIX14/P01"
SERVICE = "qd-bookmap"
SYMBOL = os.getenv("QD_SYMBOL", "BTC_USDT")
WS_URL = os.getenv("QD_WS_URL", "wss://contract.mexc.com/edge")  # contract WS

HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "5000"))

RAW_RING_MAX = int(os.getenv("QD_RAW_RING", "200"))
TAPE_MAX = int(os.getenv("QD_TAPE_MAX", "200"))
DEPTH_MAX_LEVELS = int(os.getenv("QD_DEPTH_MAX", "200"))  # per side

RENDER_DEFAULT_LEVELS = int(os.getenv("QD_RENDER_LEVELS", "120"))
RENDER_DEFAULT_STEP = float(os.getenv("QD_RENDER_STEP", "0.1"))
RENDER_FPS = float(os.getenv("QD_RENDER_FPS", "8"))  # ws push rate

BOOTLOG_PATH = os.getenv("QD_BOOTLOG_PATH", "bootlog.txt")


def _now() -> float:
    return time.time()


def _fmt_ts(ts: float) -> str:
    # lightweight iso-ish
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts)) + f".{int((ts%1)*1000):03d}Z"


def bootlog(msg: str) -> None:
    try:
        line = f"[{_fmt_ts(_now())}] {msg}\n"
        with open(BOOTLOG_PATH, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        # never crash for logging
        pass


def _as_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        if isinstance(x, str):
            x = x.strip()
            if not x:
                return None
            return float(x)
        return float(x)
    except Exception:
        return None


def _as_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        if isinstance(x, int):
            return int(x)
        if isinstance(x, float):
            return int(x)
        if isinstance(x, str):
            x = x.strip()
            if not x:
                return None
            return int(float(x))
        return int(x)
    except Exception:
        return None


@dataclass
class Trade:
    ts: float
    px: float
    qty: float
    side: str  # "buy" or "sell"


@dataclass
class Book:
    bids: Dict[float, float] = field(default_factory=dict)  # px -> qty
    asks: Dict[float, float] = field(default_factory=dict)  # px -> qty
    version: Optional[int] = None
    last_update_ts: Optional[float] = None

    best_bid: Optional[float] = None
    best_ask: Optional[float] = None

    bid_qty_total: float = 0.0
    ask_qty_total: float = 0.0

    def recompute(self) -> None:
        self.best_bid = max(self.bids.keys()) if self.bids else None
        self.best_ask = min(self.asks.keys()) if self.asks else None
        self.bid_qty_total = float(sum(self.bids.values())) if self.bids else 0.0
        self.ask_qty_total = float(sum(self.asks.values())) if self.asks else 0.0


@dataclass
class State:
    start_ts: float = field(default_factory=_now)

    # connector
    status: str = "INIT"  # INIT/CONNECTING/CONNECTED/ERROR
    frames: int = 0
    reconnects: int = 0
    last_error: str = ""
    last_connect_ts: Optional[float] = None
    last_event_ts: Optional[float] = None
    last_close_code: Optional[int] = None
    last_close_reason: str = ""

    # parsing stats
    parse_hits: int = 0
    parse_misses: int = 0

    # rings
    raw_ring: Deque[Dict[str, Any]] = field(default_factory=lambda: deque(maxlen=RAW_RING_MAX))
    trades: Deque[Trade] = field(default_factory=lambda: deque(maxlen=TAPE_MAX))

    # book
    book: Book = field(default_factory=Book)
    snapshots_applied: int = 0

    # depth merge stats / parity (FIX14)
    depth_updates_applied: int = 0
    last_bid_side_ts: Optional[float] = None
    last_ask_side_ts: Optional[float] = None
    last_parity_ok_ts: Optional[float] = None

    def uptime_s(self) -> float:
        return _now() - self.start_ts


STATE = State()

# ----------------------------
# Depth parsing & merge
# ----------------------------

def _apply_depth_update(obj: Dict[str, Any], ts: float) -> bool:
    """
    Applies MEXC contract depth payloads in a *parity-safe* way.

    Critical FIX14 change:
    - Do NOT overwrite the entire book when only one side (bids/asks) is present.
    - Support incremental merging semantics: qty<=0 deletes level; qty>0 sets level.

    We treat a payload as a "full snapshot" only when BOTH sides are present
    and the book is empty OR the payload looks large enough to plausibly be a snapshot.
    """
    data = obj.get("data")
    if not isinstance(data, dict):
        return False

    bids = data.get("bids")
    asks = data.get("asks")
    if not isinstance(bids, list) and not isinstance(asks, list):
        return False

    def _iter_levels(side_list: Any):
        if not isinstance(side_list, list):
            return
        for lvl in side_list[:DEPTH_MAX_LEVELS]:
            px = qty = None
            if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                px = _as_float(lvl[0])
                qty = _as_float(lvl[1])
            elif isinstance(lvl, dict):
                px = _as_float(lvl.get("price") or lvl.get("p"))
                qty = _as_float(lvl.get("quantity") or lvl.get("q"))
            if px is None or qty is None:
                continue
            yield float(px), float(qty)

    # Heuristic: treat as full snapshot only when both sides present and either
    # (a) we have no book yet, or (b) the payload size is large.
    both_present = isinstance(bids, list) and isinstance(asks, list)
    looks_like_snapshot = both_present and (
        (len(STATE.book.bids) == 0 and len(STATE.book.asks) == 0) or
        (len(bids) >= 50 and len(asks) >= 50)
    )

    if looks_like_snapshot:
        new_bids: Dict[float, float] = {}
        new_asks: Dict[float, float] = {}

        for px, qty in _iter_levels(bids):
            if qty > 0:
                new_bids[px] = qty
        for px, qty in _iter_levels(asks):
            if qty > 0:
                new_asks[px] = qty

        STATE.book.bids = new_bids
        STATE.book.asks = new_asks
        STATE.snapshots_applied += 1
        STATE.depth_updates_applied += 1

        STATE.last_bid_side_ts = ts if new_bids else STATE.last_bid_side_ts
        STATE.last_ask_side_ts = ts if new_asks else STATE.last_ask_side_ts
    else:
        changed = False

        if isinstance(bids, list):
            # incremental merge for bids
            for px, qty in _iter_levels(bids):
                if qty <= 0:
                    if px in STATE.book.bids:
                        del STATE.book.bids[px]
                        changed = True
                else:
                    if STATE.book.bids.get(px) != qty:
                        STATE.book.bids[px] = qty
                        changed = True
            STATE.last_bid_side_ts = ts

        if isinstance(asks, list):
            # incremental merge for asks
            for px, qty in _iter_levels(asks):
                if qty <= 0:
                    if px in STATE.book.asks:
                        del STATE.book.asks[px]
                        changed = True
                else:
                    if STATE.book.asks.get(px) != qty:
                        STATE.book.asks[px] = qty
                        changed = True
            STATE.last_ask_side_ts = ts

        if changed:
            STATE.depth_updates_applied += 1

    # version / timestamps
    STATE.book.version = _as_int(data.get("version") or obj.get("version") or obj.get("v"))
    STATE.book.last_update_ts = ts
    STATE.book.recompute()

    # parity signal
    if STATE.book.best_bid is not None and STATE.book.best_ask is not None and len(STATE.book.bids) > 0 and len(STATE.book.asks) > 0:
        STATE.last_parity_ok_ts = ts

    return True

def _extract_trade_events(obj: Dict[str, Any], ts: float) -> int:
    """
    Accepts MEXC contract deal push variants. Common patterns:
      - data is dict with fields p/v/S (price/vol/side)
      - data is list of dicts (batch)
    Side field varies; we map to buy/sell if possible, else unknown.
    """
    data = obj.get("data")
    if data is None:
        return 0

    events: List[Dict[str, Any]] = []
    if isinstance(data, list):
        events = [x for x in data if isinstance(x, dict)]
    elif isinstance(data, dict):
        # sometimes nested: {"deals":[...]}
        if isinstance(data.get("deals"), list):
            events = [x for x in data.get("deals") if isinstance(x, dict)]
        else:
            events = [data]
    else:
        return 0

    pushed = 0
    for ev in events:
        px = _as_float(ev.get("p") or ev.get("price") or ev.get("px"))
        qty = _as_float(ev.get("v") or ev.get("vol") or ev.get("qty") or ev.get("q"))
        if px is None or qty is None:
            continue

        side_raw = ev.get("S") or ev.get("side") or ev.get("t") or ev.get("T")
        side = "unknown"
        if isinstance(side_raw, str):
            s = side_raw.lower()
            if "buy" in s or s in ("b", "bid"):
                side = "buy"
            elif "sell" in s or s in ("s", "ask"):
                side = "sell"
        elif isinstance(side_raw, (int, float)):
            # Some feeds use 1/2, 1=buy 2=sell (cannot confirm for MEXC)
            if int(side_raw) == 1:
                side = "buy"
            elif int(side_raw) == 2:
                side = "sell"

        STATE.trades.append(Trade(ts=ts, px=px, qty=qty, side=side))
        pushed += 1

    return pushed


def _handle_ws_message(raw: str) -> None:
    ts = _now()
    try:
        obj = json.loads(raw)
    except Exception:
        STATE.parse_misses += 1
        return

    STATE.raw_ring.append({"ts": ts, "raw": obj})
    STATE.frames += 1
    STATE.last_event_ts = ts

    if isinstance(obj, dict):
        hit = False
        ch = obj.get("channel") or obj.get("c") or ""
        if isinstance(ch, str):
            ch_l = ch.lower()
        else:
            ch_l = ""

        # Depth: push.depth or any dict containing bids/asks
        if "depth" in ch_l or ("data" in obj and isinstance(obj.get("data"), dict) and ("bids" in obj["data"] or "asks" in obj["data"])):
            if _apply_depth_update(obj, ts):
                hit = True

        # Deals/trades: push.deal, push.trade, deals, etc.
        if ("deal" in ch_l) or ("trade" in ch_l) or ("deals" in ch_l):
            pushed = _extract_trade_events(obj, ts)
            if pushed > 0:
                hit = True

        if hit:
            STATE.parse_hits += 1
        else:
            STATE.parse_misses += 1
    else:
        STATE.parse_misses += 1


# ----------------------------
# Connector loop (MEXC contract WS)
# ----------------------------

async def _connector_loop() -> None:
    if websockets is None:
        STATE.status = "ERROR"
        STATE.last_error = "python package 'websockets' is not available in this environment"
        bootlog("ERROR: websockets module missing; connector disabled")
        return

    sub_depth = {"method": "sub.depth", "param": {"symbol": SYMBOL, "depth": 200}, "id": 1}
    sub_deals = {"method": "sub.deal", "param": {"symbol": SYMBOL}, "id": 2}

    while True:
        try:
            STATE.status = "CONNECTING"
            bootlog(f"Connecting WS: {WS_URL} symbol={SYMBOL}")
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20, close_timeout=10) as ws:
                STATE.status = "CONNECTED"
                STATE.last_connect_ts = _now()
                STATE.last_error = ""
                STATE.last_close_code = None
                STATE.last_close_reason = ""

                try:
                    await ws.send(json.dumps(sub_depth))
                    await ws.send(json.dumps(sub_deals))
                except Exception as e:
                    STATE.last_error = f"subscribe_send_failed: {e}"
                    bootlog(f"ERROR: subscribe send failed: {e}")

                while True:
                    raw = await ws.recv()
                    if isinstance(raw, bytes):
                        try:
                            raw = raw.decode("utf-8", errors="replace")
                        except Exception:
                            raw = str(raw)
                    _handle_ws_message(str(raw))
        except Exception as e:
            STATE.status = "ERROR"
            STATE.reconnects += 1
            STATE.last_error = f"{type(e).__name__}: {e}"
            bootlog(f"WS ERROR: {STATE.last_error}\n{traceback.format_exc()}")
            # bounded backoff
            await asyncio.sleep(min(10.0, 0.5 * STATE.reconnects))


# ----------------------------
# Render Bridge Contract
# ----------------------------

def _mid_px() -> Optional[float]:
    bb = STATE.book.best_bid
    ba = STATE.book.best_ask
    if bb is not None and ba is not None:
        return (bb + ba) / 2.0
    if STATE.trades:
        return STATE.trades[-1].px
    return bb or ba


def _render_frame(levels: int, step: float) -> Dict[str, Any]:
    ts = _now()
    mid = _mid_px()
    bb = STATE.book.best_bid
    ba = STATE.book.best_ask

    # Create a symmetric ladder around mid if possible
    prices: List[float] = []
    bid_qty: List[float] = []
    ask_qty: List[float] = []

    if mid is not None and step > 0 and levels > 0:
        center = round(mid / step) * step
        half = levels // 2
        start = center - half * step
        for i in range(levels):
            px = round(start + i * step, 10)
            prices.append(px)
            bid_qty.append(float(STATE.book.bids.get(px, 0.0)))
            ask_qty.append(float(STATE.book.asks.get(px, 0.0)))

    # Tape (last N, newest last)
    tape = [
        {"ts": t.ts, "px": t.px, "qty": t.qty, "side": t.side}
        for t in list(STATE.trades)[-50:]
    ]

    last_event_age = None
    if STATE.last_event_ts is not None:
        last_event_age = ts - STATE.last_event_ts

    return {
        "ts": ts,
        "service": SERVICE,
        "build": BUILD,
        "symbol": SYMBOL,
        "ws_url": WS_URL,
        "health": "GREEN" if STATE.status == "CONNECTED" else ("YELLOW" if STATE.status in ("INIT", "CONNECTING") else "RED"),
        "connector": {
            "status": STATE.status,
            "frames": STATE.frames,
            "reconnects": STATE.reconnects,
            "last_error": STATE.last_error,
            "last_event_age_s": last_event_age,
        },
        "book": {
            "best_bid": bb,
            "best_ask": ba,
            "version": STATE.book.version,
            "depth_counts": {"bids": len(STATE.book.bids), "asks": len(STATE.book.asks)},
            "totals": {"bid_qty": STATE.book.bid_qty_total, "ask_qty": STATE.book.ask_qty_total},
            "last_update_ts": STATE.book.last_update_ts,
        },
        "tape": {
            "trades_seen": len(STATE.trades),
            "last_trade_px": STATE.trades[-1].px if STATE.trades else None,
            "last_trade_qty": STATE.trades[-1].qty if STATE.trades else None,
            "last_trade_side": STATE.trades[-1].side if STATE.trades else None,
            "last_trade_ts": STATE.trades[-1].ts if STATE.trades else None,
            "items": tape,
        },
        "render": {
            "levels": levels,
            "step": step,
            "mid_px": mid,
            "prices": prices,
            "bid_qty": bid_qty,
            "ask_qty": ask_qty,
        },
    }


# ----------------------------
# FastAPI App (with lifespan)
# ----------------------------

app = FastAPI(title="QuantDesk Bookmap", version=BUILD)

@app.on_event("startup")  # OK for Replit; deprecation warning is acceptable for now
async def _startup() -> None:
    bootlog(f"Starting {SERVICE} {BUILD} on {HOST}:{PORT}")
    bootlog(f"Lifespan startup entered")
    # Start connector in background
    asyncio.create_task(_connector_loop())


@app.get("/bootlog.txt")
async def bootlog_txt() -> PlainTextResponse:
    try:
        with open(BOOTLOG_PATH, "r", encoding="utf-8") as f:
            return PlainTextResponse(f.read())
    except Exception:
        return PlainTextResponse("bootlog unavailable", status_code=404)


@app.get("/health.json")
async def health_json() -> JSONResponse:
    # Health semantics (matching prior workflow):
    # GREEN: server up + connector CONNECTED + fresh frames
    # YELLOW: server up but connector not connected yet (warmup) OR no frames yet
    # RED: server up but connector in ERROR
    now = _now()
    fresh = (STATE.last_event_ts is not None) and (now - STATE.last_event_ts < 5.0)
    parity_ok = (STATE.book.best_bid is not None) and (STATE.book.best_ask is not None) and (len(STATE.book.bids) > 0) and (len(STATE.book.asks) > 0)
    # Health semantics:
    # GREEN: server up + connector CONNECTED + fresh frames + book parity OK
    # YELLOW: warmup/degraded (e.g., parity not yet achieved) but not fatal
    # RED: connector ERROR OR prolonged parity failure while connected
    if STATE.status == "ERROR":
        h = "RED"
    else:
        if STATE.status == "CONNECTED" and fresh and STATE.frames > 0:
            if parity_ok:
                h = "GREEN"
            else:
                # allow warmup; after extended time, treat as RED because visuals would be misleading
                up = STATE.uptime_s()
                h = "YELLOW" if up < 30.0 else "RED"
        else:
            h = "YELLOW"
    return JSONResponse({
        "ts": now,
        "service": SERVICE,
        "build": BUILD,
        "symbol": SYMBOL,
        "ws_url": WS_URL,
        "health": h,
        "uptime_s": round(STATE.uptime_s(), 3),
        "connector": {
            "status": STATE.status,
            "frames": STATE.frames,
            "reconnects": STATE.reconnects,
            "last_error": STATE.last_error,
            "last_event_ts": STATE.last_event_ts,
            "last_connect_ts": STATE.last_connect_ts,
        },
        "book": {
            "best_bid": STATE.book.best_bid,
            "best_ask": STATE.book.best_ask,
            "depth_counts": {"bids": len(STATE.book.bids), "asks": len(STATE.book.asks)},
            "version": STATE.book.version,
            "last_update_ts": STATE.book.last_update_ts,
            "parity_ok": (STATE.book.best_bid is not None) and (STATE.book.best_ask is not None) and (len(STATE.book.bids) > 0) and (len(STATE.book.asks) > 0),
            "last_parity_ok_ts": STATE.last_parity_ok_ts,
            "last_bid_side_ts": STATE.last_bid_side_ts,
            "last_ask_side_ts": STATE.last_ask_side_ts,
        },
        "tape": {
            "trades_seen": len(STATE.trades),
            "last_trade_px": STATE.trades[-1].px if STATE.trades else None,
            "last_trade_ts": STATE.trades[-1].ts if STATE.trades else None,
        },
    })


@app.get("/telemetry.json")
async def telemetry_json() -> JSONResponse:
    return JSONResponse({
        "ts": _now(),
        "service": SERVICE,
        "build": BUILD,
        "symbol": SYMBOL,
        "ws_url": WS_URL,
        "uptime_s": round(STATE.uptime_s(), 3),
        "connector": {
            "status": STATE.status,
            "frames": STATE.frames,
            "reconnects": STATE.reconnects,
            "last_error": STATE.last_error,
            "last_close_code": STATE.last_close_code,
            "last_close_reason": STATE.last_close_reason,
            "last_connect_ts": STATE.last_connect_ts,
            "last_event_ts": STATE.last_event_ts,
        },
        "parser": {
            "parse_hits": STATE.parse_hits,
            "parse_misses": STATE.parse_misses,
            "snapshots_applied": STATE.snapshots_applied,
            "depth_updates_applied": STATE.depth_updates_applied,
            "last_bid_side_ts": STATE.last_bid_side_ts,
            "last_ask_side_ts": STATE.last_ask_side_ts,
            "last_parity_ok_ts": STATE.last_parity_ok_ts,
        },
        "rings": {
            "raw_ring_size": len(STATE.raw_ring),
            "tape_size": len(STATE.trades),
        },
        "host": HOST,
        "port": PORT,
    })


@app.get("/raw.json")
async def raw_json() -> JSONResponse:
    return JSONResponse({
        "ts": _now(),
        "build": BUILD,
        "raw_ring": list(STATE.raw_ring),
    })


@app.get("/book.json")
async def book_json() -> JSONResponse:
    return JSONResponse({
        "ts": _now(),
        "build": BUILD,
        "symbol": SYMBOL,
        "best_bid": STATE.book.best_bid,
        "best_ask": STATE.book.best_ask,
        "depth_counts": {"bids": len(STATE.book.bids), "asks": len(STATE.book.asks)},
        "totals": {"bid_qty": STATE.book.bid_qty_total, "ask_qty": STATE.book.ask_qty_total},
        "version": STATE.book.version,
        "last_update_ts": STATE.book.last_update_ts,
    })


@app.get("/tape.json")
async def tape_json() -> JSONResponse:
    return JSONResponse({
        "ts": _now(),
        "build": BUILD,
        "symbol": SYMBOL,
        "trades_seen": len(STATE.trades),
        "last_trade_px": STATE.trades[-1].px if STATE.trades else None,
        "last_trade_qty": STATE.trades[-1].qty if STATE.trades else None,
        "last_trade_side": STATE.trades[-1].side if STATE.trades else None,
        "last_trade_ts": STATE.trades[-1].ts if STATE.trades else None,
        "tape": [{"ts": t.ts, "px": t.px, "qty": t.qty, "side": t.side} for t in list(STATE.trades)],
    })


@app.get("/render.json")
async def render_json(levels: int = RENDER_DEFAULT_LEVELS, step: float = RENDER_DEFAULT_STEP) -> JSONResponse:
    # Contract: always returns a frame; empty arrays allowed during warmup.
    levels = max(10, min(400, int(levels)))
    step = float(step) if step and step > 0 else RENDER_DEFAULT_STEP
    return JSONResponse(_render_frame(levels=levels, step=step))


@app.websocket("/render.ws")
async def render_ws(ws: WebSocket) -> None:
    await ws.accept()
    fps = max(1.0, min(30.0, RENDER_FPS))
    period = 1.0 / fps
    try:
        while True:
            frame = _render_frame(levels=RENDER_DEFAULT_LEVELS, step=RENDER_DEFAULT_STEP)
            await ws.send_text(json.dumps(frame))
            await asyncio.sleep(period)
    except Exception:
        # client disconnect or send failure
        return


def _ui_html() -> str:
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>QuantDesk Bookmap</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 24px; }}
    .top {{ display:flex; gap:12px; align-items:center; flex-wrap:wrap; }}
    .pill {{ padding:6px 10px; border-radius:999px; border:1px solid #ddd; font-weight:600; }}
    .pill.green {{ border-color:#2e7d32; color:#2e7d32; }}
    .pill.yellow {{ border-color:#f9a825; color:#8a6d00; }}
    .pill.red {{ border-color:#c62828; color:#c62828; }}
    .muted {{ color:#666; }}
    .grid {{ display:grid; grid-template-columns: 1fr; gap:14px; margin-top:16px; max-width: 1100px; }}
    @media (min-width: 980px) {{
      .grid {{ grid-template-columns: 1.2fr 0.8fr; }}
    }}
    pre {{ background:#0b0b0c; color:#e8e8e8; padding:14px; border-radius:12px; overflow:auto; font-size:12px; line-height:1.25; }}
    canvas {{ width:100%; height:520px; background:#fff; border:1px solid #ddd; border-radius:12px; }}
    .links a {{ display:block; margin:4px 0; }}
  </style>
</head>
<body>
  <h1>QuantDesk Bookmap</h1>
  <div class="top">
    <span id="healthPill" class="pill yellow">HEALTH: ...</span>
    <span class="muted">Build: {BUILD}</span>
    <span class="muted">Symbol: {SYMBOL}</span>
    <span class="muted">WS: {WS_URL}</span>
  </div>

  <p class="muted">
    This page is a <b>render bridge contract demo</b>. It streams <code>/render.ws</code> frames and draws a minimal ladder.
    It is not a full Bookmap renderer yet.
  </p>

  <div class="grid">
    <div>
      <canvas id="cv" width="1100" height="520"></canvas>
      <p class="muted" style="margin-top:8px;">
        Ladder: green = bids, red = asks. Mid line shown when available.
      </p>
    </div>
    <div>
      <div class="links">
        <b>Endpoints</b>
        <a href="/health.json" target="_blank">/health.json</a>
        <a href="/telemetry.json" target="_blank">/telemetry.json</a>
        <a href="/book.json" target="_blank">/book.json</a>
        <a href="/tape.json" target="_blank">/tape.json</a>
        <a href="/render.json" target="_blank">/render.json</a>
        <a href="/raw.json" target="_blank">/raw.json</a>
        <a href="/bootlog.txt" target="_blank">/bootlog.txt</a>
      </div>
      <h3>Status (auto-refresh)</h3>
      <pre id="statusPre">{{}}</pre>
    </div>
  </div>

<script>
(function() {{
  const statusPre = document.getElementById('statusPre');
  const pill = document.getElementById('healthPill');
  const cv = document.getElementById('cv');
  const ctx = cv.getContext('2d');

  function setPill(h) {{
    pill.classList.remove('green','yellow','red');
    if (h === 'GREEN') pill.classList.add('green');
    else if (h === 'RED') pill.classList.add('red');
    else pill.classList.add('yellow');
    pill.textContent = 'HEALTH: ' + h;
  }}

  function draw(frame) {{
    // Clear
    ctx.clearRect(0,0,cv.width,cv.height);

    const prices = frame?.render?.prices || [];
    const bid = frame?.render?.bid_qty || [];
    const ask = frame?.render?.ask_qty || [];

    // Background
    ctx.fillStyle = '#ffffff';
    ctx.fillRect(0,0,cv.width,cv.height);

    if (!prices.length) {{
      ctx.fillStyle = '#666';
      ctx.font = '16px system-ui';
      ctx.fillText('Waiting for render frames...', 16, 28);
      return;
    }}

    const n = prices.length;
    const padL = 80, padR = 20, padT = 16, padB = 16;
    const W = cv.width - padL - padR;
    const H = cv.height - padT - padB;

    // Scale qty to bars
    let maxQ = 0;
    for (let i=0;i<n;i++) {{
      maxQ = Math.max(maxQ, bid[i]||0, ask[i]||0);
    }}
    maxQ = Math.max(1e-9, maxQ);

    // Draw ladder rows
    const rowH = H / n;
    for (let i=0;i<n;i++) {{
      const y = padT + i*rowH;
      const b = (bid[i]||0) / maxQ;
      const a = (ask[i]||0) / maxQ;

      // price labels
      if (i % Math.max(1, Math.floor(n/12)) === 0) {{
        ctx.fillStyle = '#333';
        ctx.font = '12px system-ui';
        ctx.fillText(prices[i].toFixed(1), 10, y + rowH*0.8);
        // light grid
        ctx.strokeStyle = '#eee';
        ctx.beginPath();
        ctx.moveTo(padL, y);
        ctx.lineTo(padL+W, y);
        ctx.stroke();
      }}

      // bid bar (left-to-right)
      if (b > 0) {{
        ctx.fillStyle = 'rgba(46, 125, 50, 0.45)';
        ctx.fillRect(padL, y, W * b, rowH);
      }}
      // ask bar (right-to-left)
      if (a > 0) {{
        ctx.fillStyle = 'rgba(198, 40, 40, 0.40)';
        ctx.fillRect(padL + (W * (1 - a)), y, W * a, rowH);
      }}
    }}

    // Mid line
    const mid = frame?.render?.mid_px;
    if (typeof mid === 'number') {{
      // find nearest row
      let bestI = 0, bestD = 1e18;
      for (let i=0;i<n;i++) {{
        const d = Math.abs(prices[i]-mid);
        if (d < bestD) {{ bestD = d; bestI = i; }}
      }}
      const y = padT + bestI*rowH + rowH/2;
      ctx.strokeStyle = '#111';
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(padL, y);
      ctx.lineTo(padL+W, y);
      ctx.stroke();
      ctx.lineWidth = 1;
    }}
  }}

  // Health poll
  async function poll() {{
    try {{
      const r = await fetch('/health.json', {{cache:'no-store'}});
      const j = await r.json();
      setPill(j.health || 'YELLOW');
    }} catch (e) {{
      setPill('YELLOW');
    }}
  }}
  setInterval(poll, 1000);
  poll();

  // Render WS
  const proto = (location.protocol === 'https:') ? 'wss' : 'ws';
  const wsUrl = proto + '://' + location.host + '/render.ws';
  const ws = new WebSocket(wsUrl);
  ws.onmessage = (ev) => {{
    try {{
      const frame = JSON.parse(ev.data);
      statusPre.textContent = JSON.stringify(frame, null, 2);
      draw(frame);
    }} catch (e) {{
      // ignore
    }}
  }};
  ws.onopen = () => {{
    // noop
  }};
  ws.onerror = () => {{
    // noop
  }};
}})();
</script>
</body>
</html>"""


@app.get("/")
async def index() -> HTMLResponse:
    return HTMLResponse(_ui_html())


# ----------------------------
# Entrypoint
# ----------------------------
if __name__ == "__main__":
    try:
        import uvicorn  # type: ignore
    except Exception as e:
        bootlog(f"FATAL: uvicorn import failed: {e}")
        raise

    uvicorn.run(app, host=HOST, port=PORT, log_level="info")
