# =========================================================
# QuantDesk Bookmap Service — FIX12_BUCKETS_FIX5_FULL (Replit)
#
# This is a FULL, runnable single-file FastAPI service for the Replit web app,
# preserving FOUNDATION_PASS and the subsequent Render/Heat/Band work,
# while fixing the broken FIX12 bucket integration that caused SyntaxErrors,
# NameErrors, and indentation issues in /snapshot.
#
# What this build guarantees (Release Gate):
# - Replit Run stays alive (no server crash due to frontend errors)
# - "/" loads (canvas UI), HUD updates
# - WS reconnect loop (MEXC)
# - Depth + trades flow (when WS available)
# - /snapshot NEVER throws (returns ok=false + error on exceptions)
# - Bucket scale selector works: bs=auto|fine|medium|coarse
#
# =========================================================

from __future__ import annotations

import os
import json
import time
import math
import asyncio
import heapq
from typing import Any, Dict, List, Tuple, Optional

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

try:
    import websockets  # type: ignore
except Exception:
    websockets = None  # allows service to stay up even if dep missing

# ---------------------------
# Config
# ---------------------------
SYMBOL = os.getenv("QD_SYMBOL_WS", "BTC_USDT").strip()
MEXC_WS = os.getenv("QD_WS_URL", "wss://contract.mexc.com/edge").strip()

PORT = int(os.getenv("PORT", os.getenv("QD_PORT", "5000")))

RANGE_USD = float(os.getenv("QD_RANGE_USD", "400"))          # half-range around mid for y mapping
BIN_USD = float(os.getenv("QD_BIN_USD", "5"))                # snapshot bin size (near-ladder)
TOPN_LEVELS = int(os.getenv("QD_TOPN_LEVELS", "200"))         # legacy top-N closest to market
SNAPSHOT_HZ = float(os.getenv("QD_SNAPSHOT_HZ", "2"))         # UI polling target

# --- Wide ladder (retain observed levels farther away) ---
WIDE_LADDER_ON = int(os.getenv("QD_WIDE_LADDER", "1")) == 1
WIDE_RANGE_USD = float(os.getenv("QD_WIDE_RANGE_USD", "2000"))
MAX_LEVELS_PER_SIDE = int(os.getenv("QD_MAX_LEVELS_PER_SIDE", "4000"))

# --- Buckets (historic band retention) ---
BUCKETS_ENABLED = int(os.getenv("QD_BUCKETS", "1")) == 1
BUCKET_SCALE_MODE_DEFAULT = os.getenv("QD_BUCKET_SCALE_MODE", "auto").strip().lower()
BUCKET_CAP_PER_SCALE = int(os.getenv("QD_BUCKET_CAP_PER_SCALE", "1200"))

# --- Global Ladder Memory Engine (FIX13) ---
# Purpose: retain a decaying memory of depth levels beyond current top-N view,
# so "bands" persist visually across time and across viewport changes.
MEM_ENABLE = os.getenv("MEM_ENABLE", "1").strip() not in ("0", "false", "False")
MEM_BIN_USD = float(os.getenv("MEM_BIN_USD", str(BIN_USD)))  # binning for memory; defaults to BIN_USD
MEM_HALFLIFE_SEC = float(os.getenv("MEM_HALFLIFE_SEC", "1800"))  # 30m half-life
MEM_CAP_BINS = int(os.getenv("MEM_CAP_BINS", "50000"))  # hard cap to prevent unbounded growth
MEM_MIN_QTY = float(os.getenv("MEM_MIN_QTY", "0"))  # ignore memory updates below this qty
MEM_ACTIVATION_USD = float(os.getenv("MEM_ACTIVATION_USD", "800"))  # distance scale for "wake-up" of global memory
MEM_ACTIVATION_MODE = (os.getenv("MEM_ACTIVATION_MODE", "rational") or "rational").strip().lower()  # rational|exp


# Fixed coarse layers used for persistence; we pick nearest based on viewport.
BUCKET_SCALES_USD = [25.0, 100.0, 250.0]
BUCKET_HALF_LIFE_SEC = {25.0: 6 * 60.0, 100.0: 18 * 60.0, 250.0: 60 * 60.0}

BUILD = "FIX13_GLOBAL_LADDER_MEMORY_ENGINE_FIX2"

app = FastAPI(title=f"QuantDesk Bookmap {BUILD}")

# ---------------------------
# Shared state (JSON-safe)
# ---------------------------
STATE: Dict[str, Any] = {
    "service": "quantdesk-bookmap",
    "build": BUILD,
    "symbol": SYMBOL,
    "ws_url": MEXC_WS,
    "ws_ok": False,
    "ws_last_rx_ms": None,
    "last_error": None,
    "last_trade_px": None,
    "best_bid": None,
    "best_ask": None,
    "mid": None,
    "bids_n": 0,
    "asks_n": 0,
    "depth_age_ms": None,
    "trade_age_ms": None,
    "prints_60s": 0,
}

STATE_LOCK = asyncio.Lock()

# ---------------------------
# In-memory orderbook + prints ring
# ---------------------------
BIDS: Dict[float, float] = {}
ASKS: Dict[float, float] = {}

LAST_DEPTH_MS: Optional[int] = None
LAST_TRADE_MS: Optional[int] = None

# Session-expanding ladder bounds used for wide mode.
LADDER_MIN_PX: Optional[float] = None
LADDER_MAX_PX: Optional[float] = None

_PRINT_TS: List[float] = []  # timestamps (seconds) of prints, rolling 60s


def _now_ms() -> int:
    return int(time.time() * 1000)


def _set_err(msg: str) -> None:
    # Never raise; just store
    STATE["last_error"] = str(msg)


def _track_print(now_s: float) -> None:
    _PRINT_TS.append(now_s)
    cutoff = now_s - 60.0
    i = 0
    for t0 in _PRINT_TS:
        if t0 >= cutoff:
            break
        i += 1
    if i:
        del _PRINT_TS[:i]
    STATE["prints_60s"] = len(_PRINT_TS)


def _recompute_tob() -> None:
    best_bid = max(BIDS.keys()) if BIDS else None
    best_ask = min(ASKS.keys()) if ASKS else None
    mid = None
    if best_bid is not None and best_ask is not None:
        mid = (best_bid + best_ask) / 2.0

    STATE["best_bid"] = best_bid
    STATE["best_ask"] = best_ask
    STATE["mid"] = mid
    STATE["bids_n"] = len(BIDS)
    STATE["asks_n"] = len(ASKS)

    now = _now_ms()
    STATE["depth_age_ms"] = (now - LAST_DEPTH_MS) if LAST_DEPTH_MS else None
    STATE["trade_age_ms"] = (now - LAST_TRADE_MS) if LAST_TRADE_MS else None


def _update_ladder_bounds() -> None:
    """Expand session-wide ladder bounds so distant liquidity remains visible when zooming out.

    This does not create new liquidity; it expands the visible domain to include extremes observed so far.
    """
    global LADDER_MIN_PX, LADDER_MAX_PX
    if not BIDS and not ASKS:
        return
    try:
        lo = min(BIDS.keys()) if BIDS else None
        hi = max(ASKS.keys()) if ASKS else None
        if lo is None and ASKS:
            lo = min(ASKS.keys())
        if hi is None and BIDS:
            hi = max(BIDS.keys())
        if lo is None or hi is None:
            return
        if hi <= lo:
            hi = lo + 1.0
        span = float(hi - lo)
        margin = max(5.0, span * 0.05)
        lo2 = float(lo) - margin
        hi2 = float(hi) + margin
        if LADDER_MIN_PX is None or lo2 < LADDER_MIN_PX:
            LADDER_MIN_PX = lo2
        if LADDER_MAX_PX is None or hi2 > LADDER_MAX_PX:
            LADDER_MAX_PX = hi2
    except Exception:
        return


def _prune_depth() -> None:
    """Prune retained depth for memory/performance.

    - If WIDE_LADDER_ON: keep levels within +/- WIDE_RANGE_USD around current center,
      and cap by size (largest qty) to MAX_LEVELS_PER_SIDE.
    - Else: keep TOPN_LEVELS closest-to-market per side.
    """
    center = None
    if BIDS and ASKS:
        center = (max(BIDS.keys()) + min(ASKS.keys())) * 0.5
    elif BIDS:
        center = max(BIDS.keys())
    elif ASKS:
        center = min(ASKS.keys())

    if WIDE_LADDER_ON and center is not None:
        lo = center - float(WIDE_RANGE_USD)
        hi = center + float(WIDE_RANGE_USD)

        for px in [p for p in BIDS.keys() if p < lo or p > hi]:
            BIDS.pop(px, None)
        for px in [p for p in ASKS.keys() if p < lo or p > hi]:
            ASKS.pop(px, None)

        if len(BIDS) > MAX_LEVELS_PER_SIDE:
            keep = set(px for px, _ in heapq.nlargest(MAX_LEVELS_PER_SIDE, BIDS.items(), key=lambda kv: kv[1]))
            for px in list(BIDS.keys()):
                if px not in keep:
                    BIDS.pop(px, None)
        if len(ASKS) > MAX_LEVELS_PER_SIDE:
            keep = set(px for px, _ in heapq.nlargest(MAX_LEVELS_PER_SIDE, ASKS.items(), key=lambda kv: kv[1]))
            for px in list(ASKS.keys()):
                if px not in keep:
                    ASKS.pop(px, None)
        return

    # Legacy: closest-to-market
    if len(BIDS) > TOPN_LEVELS:
        for px in sorted(BIDS.keys())[:-TOPN_LEVELS]:
            BIDS.pop(px, None)
    if len(ASKS) > TOPN_LEVELS:
        for px in sorted(ASKS.keys())[TOPN_LEVELS:]:
            ASKS.pop(px, None)


# ---------------------------
# Buckets engine (historic persistence)
# ---------------------------
# scale -> {bin_px: strength}
BUCKETS: Dict[float, Dict[float, float]] = {s: {} for s in BUCKET_SCALES_USD}
BUCKETS_LAST_MS: Dict[float, int] = {s: 0 for s in BUCKET_SCALES_USD}



# Global ladder memory: price_bin -> strength (qty); decays over time.
LADDER_MEM: Dict[float, float] = {}
LADDER_MEM_LAST_MS: int = 0

def _mem_bin(px: float) -> float:
    # bin to MEM_BIN_USD while keeping nice decimal (avoid float noise)
    if MEM_BIN_USD <= 0:
        return float(px)
    b = round(px / MEM_BIN_USD) * MEM_BIN_USD
    return float(f"{b:.2f}")

def _activation_weight(center: float, px: float, scale_usd: float) -> float:
    """Distance-based activation for global memory.

    - rational: 1/(1+(d/scale)^2) (slow tail, Bookmap-like reappearance)
    - exp: exp(-d/scale)
    """
    try:
        c = float(center)
        p = float(px)
        s = max(1e-6, float(scale_usd))
        d = abs(p - c)
        if MEM_ACTIVATION_MODE == "exp":
            return float(math.exp(-d / s))
        x = d / s
        return float(1.0 / (1.0 + x * x))
    except Exception:
        return 1.0

def _ladder_mem_decay(now_ms: int) -> None:
    global LADDER_MEM_LAST_MS, LADDER_MEM
    if not MEM_ENABLE:
        return
    if LADDER_MEM_LAST_MS <= 0:
        LADDER_MEM_LAST_MS = now_ms
        return
    dt = max(0.0, (now_ms - LADDER_MEM_LAST_MS) / 1000.0)
    if dt <= 0:
        return
    # Exponential decay with half-life
    hl = max(1.0, MEM_HALFLIFE_SEC)
    decay = 0.5 ** (dt / hl)
    if decay >= 0.999:
        LADDER_MEM_LAST_MS = now_ms
        return
    if LADDER_MEM:
        # In-place decay; prune near-zero
        to_del = []
        for k, v in LADDER_MEM.items():
            nv = v * decay
            if nv <= 1e-9:
                to_del.append(k)
            else:
                LADDER_MEM[k] = nv
        for k in to_del:
            LADDER_MEM.pop(k, None)
    LADDER_MEM_LAST_MS = now_ms

def ladder_mem_update_from_book(now_ms: int) -> None:
    # Called after applying a depth update; adds current book levels to memory.
    if not MEM_ENABLE:
        return
    _ladder_mem_decay(now_ms)
    # merge bids/asks into memory
    for px, qty in BIDS.items():
        if qty <= MEM_MIN_QTY:
            continue
        b = _mem_bin(px)
        prev = LADDER_MEM.get(b, 0.0)
        if qty > prev:
            LADDER_MEM[b] = float(qty)
    for px, qty in ASKS.items():
        if qty <= MEM_MIN_QTY:
            continue
        b = _mem_bin(px)
        prev = LADDER_MEM.get(b, 0.0)
        if qty > prev:
            LADDER_MEM[b] = float(qty)
    # cap size (keep strongest bins)
    if len(LADDER_MEM) > MEM_CAP_BINS:
        # keep top MEM_CAP_BINS by strength
        items = sorted(LADDER_MEM.items(), key=lambda kv: kv[1], reverse=True)[:MEM_CAP_BINS]
        LADDER_MEM = {k: v for k, v in items}

def _bucket_decay_factor(scale_usd: float, now_ms: int) -> float:
    last = int(BUCKETS_LAST_MS.get(scale_usd) or 0)
    if last <= 0:
        BUCKETS_LAST_MS[scale_usd] = now_ms
        return 1.0
    dt_ms = max(0, now_ms - last)
    BUCKETS_LAST_MS[scale_usd] = now_ms
    if dt_ms <= 0:
        return 1.0
    hl_s = float(BUCKET_HALF_LIFE_SEC.get(scale_usd, 600.0))
    hl_ms = max(1.0, hl_s * 1000.0)
    # exp(-ln2 * dt/hl) == 2^(-dt/hl)
    return float(pow(2.0, -dt_ms / hl_ms))


def _update_buckets_from_book(now_ms: int) -> None:
    """Aggregate current retained depth into coarse bucket layers with decay.

    This retains *observed* liquidity bands as price migrates, approximating Bookmap-like distant memory.
    """
    if not BUCKETS_ENABLED:
        return
    if not BIDS and not ASKS:
        return

    levels: List[Tuple[float, float]] = []
    if BIDS:
        levels.extend((float(px), float(q)) for px, q in BIDS.items())
    if ASKS:
        levels.extend((float(px), float(q)) for px, q in ASKS.items())
    if not levels:
        return

    for scale in BUCKET_SCALES_USD:
        d = BUCKETS.setdefault(scale, {})
        decay = _bucket_decay_factor(scale, now_ms)

        # decay existing
        if decay < 0.9999 and d:
            for k in list(d.keys()):
                v = d[k] * decay
                if v <= 1e-12:
                    d.pop(k, None)
                else:
                    d[k] = v

        # aggregate current snapshot into scale bins
        tmp: Dict[float, float] = {}
        for px, qty in levels:
            b = round(px / scale) * scale
            tmp[b] = tmp.get(b, 0.0) + float(qty)

        # merge with max-like persistence to avoid infinite accumulation
        for b, q in tmp.items():
            prev = d.get(b, 0.0)
            d[b] = max(prev, float(q))

        # cap by strength
        cap = int(BUCKET_CAP_PER_SCALE)
        if len(d) > cap:
            keep = set(sorted(d, key=lambda k: d[k], reverse=True)[:cap])
            for k in list(d.keys()):
                if k not in keep:
                    d.pop(k, None)


def choose_bucket_scale(half_range: float, mode: str = "auto") -> float:
    """Select which fixed scale to use for rendering based on viewport, with optional override.

    Returns one of BUCKET_SCALES_USD (nearest by heuristic).
    """
    hr = max(0.0, float(half_range or 0.0))
    mode = (mode or "auto").lower().strip()

    # Manual override maps directly to a reasonable fixed layer
    if mode == "fine":
        target = 25.0
    elif mode == "coarse":
        target = 250.0
    elif mode == "medium":
        target = 100.0
    else:
        # auto heuristic
        if hr <= 600:
            target = 25.0
        elif hr <= 2000:
            target = 100.0
        else:
            target = 250.0

    # Pick nearest available
    best = BUCKET_SCALES_USD[0]
    best_d = abs(best - target)
    for s in BUCKET_SCALES_USD[1:]:
        d = abs(s - target)
        if d < best_d:
            best, best_d = s, d
    return float(best)


def _build_bucket_bins(center: float, half_range: float, bucket_scale_mode: Optional[str] = None) -> Tuple[List[List[float]], float, float]:
    """Build bucket bins for the viewport from the selected bucket layer.

    Returns: (bins[[px, strength]...], max_strength, bucket_usd_used)
    """
    if not BUCKETS_ENABLED:
        return [], 0.0, 0.0

    mode = (bucket_scale_mode or BUCKET_SCALE_MODE_DEFAULT or "auto").lower()
    scale = choose_bucket_scale(half_range, mode=mode)

    d = BUCKETS.get(scale) or {}
    if not d:
        return [], 0.0, float(scale)

    vmin = float(center) - float(half_range)
    vmax = float(center) + float(half_range)

    out: List[List[float]] = []
    mx = 0.0
    for px, v in d.items():
        if vmin <= float(px) <= vmax and v > 0:
            fv = float(v)
            if fv > mx:
                mx = fv
            out.append([float(px), fv])

    out.sort(key=lambda x: x[0])
    return out, float(mx), float(scale)


# ---------------------------
# Snapshot (near-ladder bins)
# ---------------------------
def _bin_price(px: float, bin_usd: float) -> float:
    if bin_usd <= 0:
        return px
    return round(px / bin_usd) * bin_usd


def _build_heat_bins(center: float, half_range: float, bin_usd: float) -> Tuple[List[List[float]], float]:
    lo = center - half_range
    hi = center + half_range
    accum: Dict[float, float] = {}

    for p, q in BIDS.items():
        if lo <= p <= hi:
            pb = _bin_price(float(p), bin_usd)
            accum[pb] = accum.get(pb, 0.0) + float(q)
    for p, q in ASKS.items():
        if lo <= p <= hi:
            pb = _bin_price(float(p), bin_usd)
            accum[pb] = accum.get(pb, 0.0) + float(q)

    if not accum:
        return [], 0.0

    items = sorted(accum.items(), key=lambda x: x[0])
    heat_max = max(v for _, v in items) if items else 0.0
    heat_bins = [[float(p), float(v)] for p, v in items]
    return heat_bins, float(heat_max)


# ---------------------------
# MEXC WS consumer
# ---------------------------
async def mexc_consumer_loop() -> None:
    """MEXC perpetual WS consumer.

    Subscribes to:
      - depth
      - deal

    IMPORTANT: The exact channel naming can vary; we key off 'push.depth'/'push.deal' substring.
    """
    global LAST_DEPTH_MS, LAST_TRADE_MS

    if websockets is None:
        async with STATE_LOCK:
            STATE["ws_ok"] = False
            _set_err("Missing dependency: websockets (pip install websockets)")
        return

    backoff = 1.0
    while True:
        try:
            async with websockets.connect(MEXC_WS, ping_interval=20, ping_timeout=20) as ws:
                # Subscribe
                await ws.send(json.dumps({"method": "sub.depth", "param": {"symbol": SYMBOL, "depth": 20}}))
                await ws.send(json.dumps({"method": "sub.deal", "param": {"symbol": SYMBOL}}))

                async with STATE_LOCK:
                    STATE["ws_ok"] = True
                    STATE["last_error"] = None

                backoff = 1.0

                async for raw in ws:
                    now_ms = _now_ms()
                    async with STATE_LOCK:
                        STATE["ws_last_rx_ms"] = now_ms

                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    ch = (msg.get("channel") or msg.get("c") or "")

                    # Some acks might not include channel; ignore quickly
                    if not isinstance(ch, str):
                        ch = ""

                    if "push.depth" in ch:
                        data = msg.get("data") or {}
                        bids = data.get("bids") or []
                        asks = data.get("asks") or []

                        # Apply partial updates (levels [price, qty, count], qty=0 => remove)
                        for lvl in bids:
                            if not isinstance(lvl, (list, tuple)) or len(lvl) < 2:
                                continue
                            try:
                                p = float(lvl[0]); q = float(lvl[1])
                            except Exception:
                                continue
                            if q <= 0.0:
                                BIDS.pop(p, None)
                            else:
                                BIDS[p] = q

                        for lvl in asks:
                            if not isinstance(lvl, (list, tuple)) or len(lvl) < 2:
                                continue
                            try:
                                p = float(lvl[0]); q = float(lvl[1])
                            except Exception:
                                continue
                            if q <= 0.0:
                                ASKS.pop(p, None)
                            else:
                                ASKS[p] = q

                        LAST_DEPTH_MS = now_ms
                        _prune_depth()
                        _update_buckets_from_book(now_ms)
                        ladder_mem_update_from_book(now_ms)

                        async with STATE_LOCK:
                            _recompute_tob()
                            _update_ladder_bounds()

                    elif "push.deal" in ch:
                        deals = msg.get("data") or []
                        if isinstance(deals, dict):
                            deals = [deals]
                        if isinstance(deals, list) and deals:
                            d0 = deals[-1]
                            try:
                                p = d0.get("p", None)
                                if p is not None:
                                    STATE["last_trade_px"] = float(p)
                            except Exception:
                                pass
                            LAST_TRADE_MS = now_ms
                            _track_print(time.time())

                            async with STATE_LOCK:
                                _recompute_tob()
                                _update_ladder_bounds()

        except Exception as e:
            async with STATE_LOCK:
                STATE["ws_ok"] = False
                _set_err(f"WS error: {type(e).__name__}: {e}")
            await asyncio.sleep(backoff)
            backoff = min(30.0, backoff * 1.7)


# ---------------------------
# FastAPI lifecycle
# ---------------------------
@app.on_event("startup")
async def _startup() -> None:
    asyncio.create_task(mexc_consumer_loop())


# ---------------------------
# Routes
# ---------------------------
@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    # IMPORTANT: Keep HTML as raw string; do not paste JS into Python scope.
    html = r"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>QuantDesk Bookmap — __BUILD__</title>
  <style>
    html, body { margin:0; padding:0; height:100%; background:#000; color:#ddd;
      font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Arial,sans-serif; overflow:hidden; }
    #wrap { position:relative; width:100%; height:100%; }
    #c { width:100%; height:100%; display:block; touch-action:none; background:#000; }
    #hud {
      position:absolute; left:12px; top:10px; z-index:10;
      background:rgba(0,0,0,0.55); border:1px solid rgba(255,255,255,0.12);
      border-radius:10px; padding:10px 12px; font-size:12px; line-height:1.35;
      max-width:92vw; pointer-events:auto;
    }
    .row { white-space:nowrap; }
    .k { color:#9aa; }
    .v { color:#e6e6e6; }
    #btns { margin-top:8px; display:flex; gap:8px; flex-wrap:wrap; }
    button { background:rgba(255,255,255,0.08); color:#eee; border:1px solid rgba(255,255,255,0.15);
      border-radius:10px; padding:8px 10px; font-size:12px; }
    button:active { transform:scale(0.99); }
    #statusDot { display:inline-block; width:8px; height:8px; border-radius:50%;
      margin-right:6px; vertical-align:middle; background:#666; }
    #err { margin-top:8px; color:#ffb3b3; max-width:92vw; white-space:pre-wrap; }
  </style>
</head>
<body>
<div id="wrap">
  <canvas id="c"></canvas>
  <div id="hud">
    <div class="row"><span id="statusDot"></span><span class="k">Build:</span> <span class="v">__BUILD__</span></div>
    <div class="row"><span class="k">Status:</span> <span class="v" id="statusTxt">…</span></div>
    <div class="row"><span class="k">Last:</span> <span class="v" id="lastPx">—</span>
      <span class="k">Bid:</span> <span class="v" id="bestBid">—</span>
      <span class="k">Ask:</span> <span class="v" id="bestAsk">—</span>
      <span class="k">Mid:</span> <span class="v" id="midPx">—</span></div>
    <div class="row"><span class="k">Ages:</span> <span class="v" id="ages">—</span></div>
    <div class="row"><span class="k">Book:</span> <span class="v" id="bookInfo">—</span>
      <span class="k">Prints(60s):</span> <span class="v" id="prints60">—</span></div>

    <div id="btns">
      <button id="followBtn">Autofollow: ON</button>
      <button id="resetBtn">Reset view</button>
      <button id="testBtn">Test pattern: OFF</button>
      <button id="wideBtn">Wide ladder: ON</button>
      <button id="bucketScaleBtn">Scale: AUTO</button>
      <button id="gainBtn">Gain: 1.00</button>
      <button id="gammaBtn">Gamma: 0.65</button>
      <button id="floorBtn">Floor: 0.02</button>
      <button id="topNBtn">TopN: 70</button>
      <button id="smoothBtn">Smooth: ON</button>
      <button id="consumeBtn">Consume: ON</button>
      <button id="bandBtn">Bands: ON</button>
      <button id="gapBtn">Gap: 1</button>
      <button id="minBtn">Min: 2</button>
      <button id="domBtn">Dom: ON</button>
      <button id="domAgeBtn">DomAge: 0.35</button>
    </div>
    <div id="err"></div>
  </div>
</div>

<script>
(function() {
  const CANVAS = document.getElementById('c');
  const ctx = CANVAS.getContext('2d');

  const heat = document.createElement('canvas');
  const hctx = heat.getContext('2d');

  const elStatusDot = document.getElementById('statusDot');
  const elStatusTxt = document.getElementById('statusTxt');
  const elLast = document.getElementById('lastPx');
  const elBid = document.getElementById('bestBid');
  const elAsk = document.getElementById('bestAsk');
  const elMid = document.getElementById('midPx');
  const elAges = document.getElementById('ages');
  const elBook = document.getElementById('bookInfo');
  const elPrints = document.getElementById('prints60');
  const elErr = document.getElementById('err');

  const followBtn = document.getElementById('followBtn');
  const resetBtn = document.getElementById('resetBtn');
  const testBtn = document.getElementById('testBtn');
  const wideBtn = document.getElementById('wideBtn');
  const bucketScaleBtn = document.getElementById('bucketScaleBtn');
  const gainBtn = document.getElementById('gainBtn');
  const gammaBtn = document.getElementById('gammaBtn');
  const floorBtn = document.getElementById('floorBtn');
  const topNBtn = document.getElementById('topNBtn');
  const smoothBtn = document.getElementById('smoothBtn');
  const consumeBtn = document.getElementById('consumeBtn');
  const bandBtn = document.getElementById('bandBtn');
  const gapBtn = document.getElementById('gapBtn');
  const minBtn = document.getElementById('minBtn');
  const domBtn = document.getElementById('domBtn');
  const domAgeBtn = document.getElementById('domAgeBtn');

  let autofollow = true;
  let testPattern = false;
  let wideMode = true;
  let bucketScaleMode = "auto";

  let centerPx = null;
  let zoom = 1.0;
  let panPx = 0.0;
  let dragging = false;
  let lastY = 0;

  const COL_PX = 2;
  const DECAY_ALPHA = 0.015;   // slower fade (bands persist longer)
  const MAX_EMA_ALPHA = 0.08;

  let emaMax = 0.0;
    let heatPrimed = false;
  const MAXV_WIN = 240;
  const maxvHist = new Array(MAXV_WIN);
  let maxvIdx = 0;
  let maxvCount = 0;

  let heatGain = 1.00;
  let heatGamma = 0.65;
  let heatFloor = 0.02;

  let topNLevels = 70;
  let smoothBands = true;
  let consumeOnPrints = true;
  let consumeStrength = 0.18;
  let consumeCols = 6;
  let consumeMinNotional = 1500.0;

  let bandify = true;
  let bandGapBins = 1;
  let bandMinBins = 2;
  let bandEdgeSoft = 0.12;

  let domOn = true;
  let domAgeW = 0.35;
  const domBands = [];
  const DOM_MAX_BANDS = 256;
  const DOM_FORGET_MS = 60000;

  function bandsOverlap(a0,a1,b0,b1) { return !(a1 < b0 || b1 < a0); }

  function resize() {
    const dpr = window.devicePixelRatio || 1;
    const w = Math.floor(window.innerWidth * dpr);
    const h = Math.floor(window.innerHeight * dpr);
    if (CANVAS.width !== w || CANVAS.height !== h) {
      CANVAS.width = w; CANVAS.height = h;
    }
    if (heat.width !== CANVAS.width || heat.height !== CANVAS.height) {
      const prev = document.createElement('canvas');
      prev.width = heat.width; prev.height = heat.height;
      const pctx = prev.getContext('2d');
      if (prev.width>0 && prev.height>0) pctx.drawImage(heat, 0, 0);

      heat.width = CANVAS.width; heat.height = CANVAS.height;
      hctx.fillStyle = '#000'; hctx.fillRect(0,0,heat.width,heat.height);
      if (prev.width>0 && prev.height>0) {
        hctx.drawImage(prev, 0,0,prev.width,prev.height, 0,0,heat.width,heat.height);
      }
    }
  }
  window.addEventListener('resize', resize);
  resize();

  function fmt(x) {
    if (x === null || x === undefined) return '—';
    if (!isFinite(x)) return '—';
    return (Math.round(x * 100) / 100).toFixed(2);
  }

  function heatColor(t) {
    t = Math.max(0, Math.min(1, t));
    let r=0,g=0,b=0;
    if (t < 0.33) {
      const u = t / 0.33;
      r = Math.floor(10 * u);
      g = Math.floor(200 * u);
      b = Math.floor(120 + (255-120) * u);
    } else if (t < 0.66) {
      const u = (t - 0.33) / 0.33;
      r = Math.floor(0 + 255 * u);
      g = Math.floor(200 + (255-200) * u);
      b = Math.floor(255 - 255 * u);
    } else {
      const u = (t - 0.66) / 0.34;
      r = 255;
      g = Math.floor(255 - (255-40) * u);
      b = 0;
    }
    return `rgb(${r},${g},${b})`;
  }

    // Snapshot overlay (instant visual even before heat history accumulates)
    // Draw current snapshot bins as horizontal bands across the right edge.
    function drawSnapshotOverlay(snap, lo, hi) {
      if (!snap) return;
      let bins = (snap && snap.heat_bins) ? snap.heat_bins : [];
      let maxv = (snap && snap.heat_max) ? snap.heat_max : 0;
      let binUsd = (snap && snap.bin_usd) ? snap.bin_usd : __BIN_USD__;
      if (wideMode && snap && snap.wide_heat_bins && snap.wide_heat_bins.length) {
        bins = snap.wide_heat_bins;
        maxv = (snap.wide_heat_max) ? snap.wide_heat_max : maxv;
        if (snap.wide_bin_usd) binUsd = snap.wide_bin_usd;
      }
      // include bucket bins too (historic bands)
      let bucketBins = (snap && snap.bucket_bins) ? snap.bucket_bins : [];
      let bucketMax = (snap && snap.bucket_max) ? snap.bucket_max : 0;
      if (wideMode && snap && snap.wide_bucket_bins && snap.wide_bucket_bins.length) {
        bucketBins = snap.wide_bucket_bins;
        bucketMax = (snap.wide_bucket_max) ? snap.wide_bucket_max : bucketMax;
      }
      if (bucketBins && bucketBins.length) {
        bins = bins.concat(bucketBins);
        maxv = Math.max(maxv, bucketMax || 0);
      }

      // include ladder memory bins too (global decayed memory)
      let memBins = (snap && snap.mem_bins) ? snap.mem_bins : [];
      let memMax = (snap && snap.mem_max) ? snap.mem_max : 0;
      if (wideMode && snap && snap.wide_mem_bins && snap.wide_mem_bins.length) {
        memBins = snap.wide_mem_bins;
        memMax = (snap.wide_mem_max) ? snap.wide_mem_max : memMax;
      }
      if (memBins && memBins.length) {
        bins = bins.concat(memBins);
        maxv = Math.max(maxv, memMax || 0);
      }
      if (!bins || bins.length === 0 || !isFinite(maxv) || maxv <= 0) return;

      const W = CANVAS.width, H = CANVAS.height;
      const x0 = Math.floor(W * 0.86);
      const w = Math.max(12, W - x0);
      const scaleMax = Math.max(1e-9, maxv);

      for (let i=0; i<bins.length; i++) {
        const p = bins[i][0];
        const v = bins[i][1];
        if (!(p >= lo && p <= hi)) continue;
        let t = Math.max(0, Math.min(1, v / scaleMax));
        t = Math.max(0, (t - heatFloor) / Math.max(1e-6, (1 - heatFloor)));
        t = Math.pow(t, heatGamma);
        const y = yOfPrice(p, lo, hi, H);
        const y2 = yOfPrice(p + binUsd, lo, hi, H);
        const hStripe = Math.max(1, Math.abs(y2 - y));
        ctx.fillStyle = heatColor(t);
        ctx.globalAlpha = 0.55;
        ctx.fillRect(x0, y - hStripe*0.5, w, hStripe);
      }
      ctx.globalAlpha = 1.0;
    }

  function smoothArrayInPlace(a) {
    const n = a.length;
    if (n < 3) return;
    let prev = a[0], curr = a[1];
    for (let i=1; i<n-1; i++) {
      const next = a[i+1];
      const v = 0.25*prev + 0.50*curr + 0.25*next;
      prev = curr; curr = next;
      a[i] = v;
    }
  }

  function topNIndicesByValue(a, topN) {
    const idxs = [];
    for (let i=0;i<a.length;i++) {
      const v = a[i];
      if (v && isFinite(v) && v>0) idxs.push(i);
    }
    if (idxs.length <= topN) return idxs;
    idxs.sort((i,j)=>a[j]-a[i]);
    return idxs.slice(0, topN);
  }

  function computeBandRuns(arr, keepIdxs, gapBins, minBins) {
    const keep = new Uint8Array(arr.length);
    for (let k=0;k<keepIdxs.length;k++) keep[keepIdxs[k]] = 1;
    const runs = [];
    let i = 0;
    while (i < arr.length) {
      if (!keep[i] || !(arr[i] > 0)) { i++; continue; }
      let s=i, e=i, mx=arr[i], gap=0;
      i++;
      while (i < arr.length) {
        const ok = keep[i] && (arr[i] > 0);
        if (ok) { e=i; if (arr[i]>mx) mx=arr[i]; gap=0; }
        else { gap++; if (gap>gapBins) break; }
        i++;
      }
      e = Math.max(s, e);
      if ((e-s+1) >= Math.max(1, minBins)) runs.push([s,e,mx]);
    }
    return runs;
  }

  function setStatus(ok, txt) {
    elStatusTxt.textContent = txt;
    elStatusDot.style.background = ok ? '#2bd26b' : '#d24b2b';
  }

  function viewBounds(snap) {
    if (wideMode && snap && isFinite(snap.ladder_min_px) && isFinite(snap.ladder_max_px) && (snap.ladder_max_px > snap.ladder_min_px)) {
      const baseLo = snap.ladder_min_px;
      const baseHi = snap.ladder_max_px;
      const baseMid = (baseLo + baseHi) / 2.0;
      const baseHalf = (baseHi - baseLo) / 2.0;
      const effHalf = (baseHalf / zoom);
      const lo = (baseMid + panPx) - effHalf;
      const hi = (baseMid + panPx) + effHalf;
      return { lo, hi, halfRange: baseHalf, effHalf };
    }
    const halfRange = (snap && snap.half_range) ? snap.half_range : __RANGE_USD__;
    const effHalf = (halfRange / zoom);
    const c = (centerPx === null || !isFinite(centerPx)) ? 0.0 : centerPx;
    const lo = (c + panPx) - effHalf;
    const hi = (c + panPx) + effHalf;
    return { lo, hi, halfRange, effHalf };
  }

  function yOfPrice(p, lo, hi, H) { return (hi - p) / (hi - lo) * H; }

  function percentile(arr, n, q) {
    if (n <= 0) return 0;
    const tmp = [];
    for (let i=0; i<n; i++) {
      const v = arr[i];
      if (v !== undefined && v !== null && isFinite(v) && v > 0) tmp.push(v);
    }
    if (tmp.length === 0) return 0;
    tmp.sort((a,b)=>a-b);
    const idx = Math.max(0, Math.min(tmp.length-1, Math.floor(q * (tmp.length-1))));
    return tmp[idx];
  }

  function updateHeat(snap) {
    const W = heat.width, H = heat.height;
    if (W<=0 || H<=0) return;

    // shift existing heat left (time) WITHOUT allocating a temp canvas each tick
    // This is critical for iPad/Safari stability in a standalone tab.
    if (heatPrimed) {
      hctx.save();
      hctx.globalCompositeOperation = 'copy';
      hctx.drawImage(heat, -COL_PX, 0);
      hctx.restore();

      // fade old heat (time-decay)
      hctx.fillStyle = `rgba(0,0,0,${DECAY_ALPHA})`;
      hctx.fillRect(0, 0, W, H);

      // clear newest column region (right edge)
      hctx.fillStyle = '#000';
      hctx.fillRect(W - COL_PX, 0, COL_PX, H);
    } else {
      // first frame: clean slate so we can prefill the full width
      hctx.fillStyle = '#000';
      hctx.fillRect(0, 0, W, H);
    }

    let bins = (snap && snap.heat_bins) ? snap.heat_bins : [];
    let maxv = (snap && snap.heat_max) ? snap.heat_max : 0;
    let binUsd = (snap && snap.bin_usd) ? snap.bin_usd : __BIN_USD__;

    if (wideMode && snap && snap.wide_heat_bins && snap.wide_heat_bins.length) {
      bins = snap.wide_heat_bins;
      maxv = (snap.wide_heat_max) ? snap.wide_heat_max : maxv;
      if (snap.wide_bin_usd) binUsd = snap.wide_bin_usd;
    }

    // Bucket overlay
    let bucketBins = (snap && snap.bucket_bins) ? snap.bucket_bins : [];
    let bucketMax = (snap && snap.bucket_max) ? snap.bucket_max : 0;
    if (wideMode && snap && snap.wide_bucket_bins && snap.wide_bucket_bins.length) {
      bucketBins = snap.wide_bucket_bins;
      bucketMax = (snap.wide_bucket_max) ? snap.wide_bucket_max : bucketMax;
    }
    if (bucketBins && bucketBins.length) {
      bins = bins.concat(bucketBins);
      maxv = Math.max(maxv, bucketMax || 0);
    }

    const { lo, hi } = viewBounds(snap);

    if (testPattern) {
      bins = [];
      const steps = 48;
      for (let i=0;i<=steps;i++) {
        const p = lo + (hi-lo) * (i/steps);
        const t = i/steps;
        bins.push([p, t]);
      }
      maxv = 1.0;
    }

    if (maxv && isFinite(maxv) && maxv > 0) {
      maxvHist[maxvIdx] = maxv;
      maxvIdx = (maxvIdx + 1) % MAXV_WIN;
      maxvCount = Math.min(MAXV_WIN, maxvCount + 1);
      emaMax = (1 - MAX_EMA_ALPHA) * emaMax + (MAX_EMA_ALPHA) * maxv;
      if (emaMax < maxv) emaMax = maxv;
    }

    const p98 = percentile(maxvHist, maxvCount, 0.98);
    let scaleMax = Math.max(1e-9, Math.max(p98 || 0, emaMax || 0));
    scaleMax = scaleMax / Math.max(1e-6, heatGain);

    if (bins && bins.length > 0) {
      const nBins = Math.max(1, Math.floor((hi - lo) / Math.max(1e-9, binUsd)) + 1);
      const arr = new Float32Array(nBins);

      for (let i=0;i<bins.length;i++) {
        const p = bins[i][0];
        const v = bins[i][1];
        if (!(p >= lo && p <= hi)) continue;
        const idx = Math.max(0, Math.min(nBins-1, Math.floor((p - lo) / binUsd)));
        if (v > arr[idx]) arr[idx] = v;
      }

      if (smoothBands) smoothArrayInPlace(arr);
      const keepIdxs = topNIndicesByValue(arr, Math.max(1, Math.floor(topNLevels)));
      hctx.globalAlpha = 0.95;

      if (bandify) {
        const runs = computeBandRuns(arr, keepIdxs, Math.max(0, Math.floor(bandGapBins)), Math.max(1, Math.floor(bandMinBins)));
        const domRel = new Array(runs.length).fill(0.0);

        if (domOn) {
          const now = Date.now();
          for (let i=domBands.length-1; i>=0; i--) {
            if ((now - domBands[i].lastSeen) > DOM_FORGET_MS) domBands.splice(i, 1);
            else domBands[i].seen = false;
          }

          for (let r=0; r<runs.length; r++) {
            const s = runs[r][0], e = runs[r][1], mx = runs[r][2];
            let best = -1, bestOv = -1;
            const c = 0.5*(s+e);
            for (let i=0; i<domBands.length; i++) {
              const b = domBands[i];
              if (!bandsOverlap(s, e, b.s, b.e)) continue;
              const bc = 0.5*(b.s+b.e);
              if (Math.abs(c-bc) > 6) continue;
              const ov = Math.min(e,b.e) - Math.max(s,b.s);
              if (ov > bestOv) { bestOv = ov; best = i; }
            }
            if (best >= 0) {
              const b = domBands[best];
              b.s = s; b.e = e;
              b.str = 0.85*b.str + 0.15*mx;
              b.age = Math.min(5000, (b.age || 1) + 1);
              b.lastSeen = now;
              b.seen = true;
              runs[r].push(best);
            } else {
              domBands.push({s:s, e:e, str:mx, age:1, lastSeen:now, seen:true});
              runs[r].push(domBands.length-1);
            }
          }

          if (domBands.length > DOM_MAX_BANDS) {
            domBands.sort((a,b)=> (b.str||0) - (a.str||0));
            domBands.length = DOM_MAX_BANDS;
          }

          let domMax = 1e-6;
          for (let i=0;i<domBands.length;i++) {
            const b = domBands[i];
            if (!b.seen) continue;
            const ageN = Math.min(200, b.age || 1) / 200.0;
            const score = Math.log1p(Math.max(0, b.str || 0)) * (1.0 + domAgeW * ageN);
            if (score > domMax) domMax = score;
          }
          for (let r=0;r<runs.length;r++) {
            const bi = runs[r][3];
            const b = domBands[bi] || null;
            if (!b) continue;
            const ageN = Math.min(200, b.age || 1) / 200.0;
            const score = Math.log1p(Math.max(0, b.str || 0)) * (1.0 + domAgeW * ageN);
            domRel[r] = Math.max(0, Math.min(1, score / domMax));
          }
        }

        for (let r=0;r<runs.length;r++) {
          const s=runs[r][0], e=runs[r][1], mx=runs[r][2];
          let t = Math.max(0, Math.min(1, mx / scaleMax));
          t = Math.max(0, (t - heatFloor) / Math.max(1e-6, (1 - heatFloor)));
          t = Math.pow(t, heatGamma);
          if (domOn) t = Math.max(0, Math.min(1, t * (0.70 + 0.60 * domRel[r])));

          const p1 = lo + s * binUsd;
          const p2 = lo + (e+1) * binUsd;

          const y1 = yOfPrice(p1, lo, hi, H);
          const y2 = yOfPrice(p2, lo, hi, H);
          const yTop = Math.min(y1,y2);
          const yBot = Math.max(y1,y2);
          const hStripe = Math.max(1, (yBot - yTop));

          const rimAlpha = Math.max(0, Math.min(0.35, bandEdgeSoft));
          const rimPx = Math.max(1, Math.floor(hStripe * 0.18));

          hctx.fillStyle = heatColor(t);
          hctx.globalAlpha = 0.95;
          hctx.fillRect(W - COL_PX, yTop + rimPx, COL_PX, Math.max(1, hStripe - 2*rimPx));
          if (rimPx >= 1) {
            hctx.globalAlpha = 0.95 * rimAlpha;
            hctx.fillRect(W - COL_PX, yTop, COL_PX, rimPx);
            hctx.fillRect(W - COL_PX, yBot - rimPx, COL_PX, rimPx);
          }
        }
      } else {
        for (let k=0;k<keepIdxs.length;k++) {
          const idx = keepIdxs[k];
          const p = lo + idx * binUsd;
          const v = arr[idx];
          let t = Math.max(0, Math.min(1, v / scaleMax));
          t = Math.max(0, (t - heatFloor) / Math.max(1e-6, (1 - heatFloor)));
          t = Math.pow(t, heatGamma);
          const y = yOfPrice(p, lo, hi, H);
          const y2 = yOfPrice(p + binUsd, lo, hi, H);
          const hStripe = Math.max(1, Math.abs(y2 - y));
          hctx.fillStyle = heatColor(t);
          hctx.globalAlpha = 0.95;
          hctx.fillRect(W - COL_PX, y - hStripe*0.5, COL_PX, hStripe);
        }
      }
      hctx.globalAlpha = 1.0;
      if (!heatPrimed) {
        try {
          const col = hctx.getImageData(W - COL_PX, 0, COL_PX, H);
          for (let x = 0; x <= W - COL_PX; x += COL_PX) {
            hctx.putImageData(col, x, 0);
          }
        } catch (e) {
          // ignore (some browsers may restrict in rare cases)
        }
        heatPrimed = true;
      }
    }
  }

  function drawFrame(snap) {
    const W = CANVAS.width, H = CANVAS.height;
    ctx.fillStyle = '#000';
    ctx.fillRect(0, 0, W, H);

    const pxCenter = snap ? snap.center_px : null;
    if (autofollow && pxCenter !== null && isFinite(pxCenter)) {
      centerPx = pxCenter;
      panPx = 0.0;
    }
    if (centerPx === null) centerPx = pxCenter;

    ctx.drawImage(heat, 0, 0);

    const { lo, hi } = viewBounds(snap);

    if (snap && snap.last_px !== null && isFinite(snap.last_px)) {
      const y = yOfPrice(snap.last_px, lo, hi, H);
      ctx.strokeStyle = 'rgba(255,255,255,0.9)';
      ctx.lineWidth = Math.max(1, Math.floor((window.devicePixelRatio||1)));
      ctx.beginPath();
      ctx.moveTo(0, y);
      ctx.lineTo(W, y);
      ctx.stroke();
    }

    ctx.fillStyle = 'rgba(255,255,255,0.75)';
    ctx.font = `${Math.floor(12 * (window.devicePixelRatio||1))}px sans-serif`;
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';

    const ticks = 8;
    for (let i=0; i<=ticks; i++) {
      const p = lo + (hi - lo) * (i / ticks);
      const y = yOfPrice(p, lo, hi, H);
      ctx.fillText(fmt(p), W - 8, y);
      ctx.strokeStyle = 'rgba(255,255,255,0.07)';
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(0, y);
      ctx.lineTo(W, y);
      ctx.stroke();
    }
  }

  async function fetchSnapshot() {
    try {
      const r = await fetch(`/snapshot?bs=${encodeURIComponent(bucketScaleMode)}`, { cache: 'no-store' });
      if (!r.ok) throw new Error('bad http');
      return await r.json();
    } catch (_) {
      return null;
    }
  }

  function setHudFromSnap(snap) {
    const age = (snap.ws_msg_age_ms === null || snap.ws_msg_age_ms === undefined) ? null : Number(snap.ws_msg_age_ms);
    const wsUp = (age !== null) && isFinite(age) && (age < 2500);
    const depthAge = snap.depth_age_ms;
    const green = wsUp && depthAge !== null && depthAge < 2500 && snap.book_bids_n > 0 && snap.book_asks_n > 0;
    const txt = green ? 'HEALTH GREEN' : (wsUp ? 'HEALTH YELLOW/RED' : 'WS DOWN');
    setStatus(green, txt);

    elLast.textContent = fmt(snap.last_px);
    elBid.textContent = fmt(snap.best_bid);
    elAsk.textContent = fmt(snap.best_ask);
    elMid.textContent = fmt(snap.mid);

    const da = (depthAge === null || depthAge === undefined) ? '—' : `${Math.round(depthAge)}ms`;
    const ta = (snap.trade_age_ms === null || snap.trade_age_ms === undefined) ? '—' : `${Math.round(snap.trade_age_ms)}ms`;
    const wa = (age === null) ? '—' : `${Math.round(age)}ms`;
    elAges.textContent = `depth_age=${da} trade_age=${ta} ws_msg_age=${wa}`;

    elBook.textContent = `bids=${snap.book_bids_n} asks=${snap.book_asks_n}`;
    elPrints.textContent = `${snap.prints_60s || 0}`;
    elErr.textContent = (snap.ok === false && snap.error) ? String(snap.error) : '';
  }

  async function tick() {
    try {
      resize();
      const snap = await fetchSnapshot();
      if (!snap) {
        setStatus(false, 'API unreachable');
        return;
      }
      setHudFromSnap(snap);
      updateHeat(snap);
      drawFrame(snap);
    } catch (e) {
      elErr.textContent = String(e && e.stack ? e.stack : e);
    }
  }

  followBtn.addEventListener('click', () => {
    autofollow = !autofollow;
    followBtn.textContent = `Autofollow: ${autofollow ? 'ON' : 'OFF'}`;
  });
  resetBtn.addEventListener('click', () => {
    zoom = 1.0;
    panPx = 0.0;
    autofollow = true;
    followBtn.textContent = 'Autofollow: ON';
  });
  testBtn.addEventListener('click', () => {
    testPattern = !testPattern;
    testBtn.textContent = `Test pattern: ${testPattern ? 'ON' : 'OFF'}`;
  });
  wideBtn.addEventListener('click', () => {
    wideMode = !wideMode;
    wideBtn.textContent = `Wide ladder: ${wideMode ? 'ON' : 'OFF'}`;
    panPx = 0; zoom = 1.0;
  });
  bucketScaleBtn.addEventListener('click', () => {
    const modes = ['auto','fine','medium','coarse'];
    const i = modes.indexOf(bucketScaleMode);
    bucketScaleMode = modes[(i >= 0 ? i+1 : 0) % modes.length];
    bucketScaleBtn.textContent = 'Scale: ' + bucketScaleMode.toUpperCase();
  });

  function updateExposureLabels() {
    gainBtn.textContent = `Gain: ${heatGain.toFixed(2)}`;
    gammaBtn.textContent = `Gamma: ${heatGamma.toFixed(2)}`;
    floorBtn.textContent = `Floor: ${heatFloor.toFixed(2)}`;
    topNBtn.textContent = `TopN: ${topNLevels}`;
    smoothBtn.textContent = `Smooth: ${smoothBands ? 'ON' : 'OFF'}`;
    consumeBtn.textContent = `Consume: ${consumeOnPrints ? 'ON' : 'OFF'}`;
    bandBtn.textContent = `Bands: ${bandify ? 'ON' : 'OFF'}`;
    gapBtn.textContent = `Gap: ${bandGapBins}`;
    minBtn.textContent = `Min: ${bandMinBins}`;
    domBtn.textContent = `Dom: ${domOn ? 'ON' : 'OFF'}`;
    domAgeBtn.textContent = `DomAge: ${domAgeW.toFixed(2)}`;
  }
  updateExposureLabels();

  gainBtn.addEventListener('click', () => {
    const steps = [0.50,0.75,1.00,1.25,1.50,2.00];
    let idx = steps.indexOf(Number(heatGain.toFixed(2)));
    if (idx < 0) idx = 2;
    heatGain = steps[(idx + 1) % steps.length];
    updateExposureLabels();
  });
  gammaBtn.addEventListener('click', () => {
    const steps = [0.45,0.55,0.65,0.80,1.00];
    let idx = steps.indexOf(Number(heatGamma.toFixed(2)));
    if (idx < 0) idx = 2;
    heatGamma = steps[(idx + 1) % steps.length];
    updateExposureLabels();
  });
  floorBtn.addEventListener('click', () => {
    const steps = [0.00,0.01,0.02,0.04,0.06];
    let idx = steps.indexOf(Number(heatFloor.toFixed(2)));
    if (idx < 0) idx = 2;
    heatFloor = steps[(idx + 1) % steps.length];
    updateExposureLabels();
  });
  topNBtn.addEventListener('click', () => {
    const steps = [40, 55, 70, 90, 120];
    let idx = steps.indexOf(topNLevels);
    if (idx < 0) idx = 2;
    topNLevels = steps[(idx + 1) % steps.length];
    updateExposureLabels();
  });
  smoothBtn.addEventListener('click', () => { smoothBands = !smoothBands; updateExposureLabels(); });
  consumeBtn.addEventListener('click', () => { consumeOnPrints = !consumeOnPrints; updateExposureLabels(); });
  bandBtn.addEventListener('click', () => { bandify = !bandify; updateExposureLabels(); });

  gapBtn.addEventListener('click', () => {
    const steps = [0,1,2,3];
    let idx = steps.indexOf(bandGapBins);
    if (idx < 0) idx = 1;
    bandGapBins = steps[(idx + 1) % steps.length];
    updateExposureLabels();
  });
  minBtn.addEventListener('click', () => {
    const steps = [1,2,3,4,6];
    let idx = steps.indexOf(bandMinBins);
    if (idx < 0) idx = 1;
    bandMinBins = steps[(idx + 1) % steps.length];
    updateExposureLabels();
  });

  domBtn.addEventListener('click', () => { domOn = !domOn; updateExposureLabels(); });
  domAgeBtn.addEventListener('click', () => {
    const steps = [0.0, 0.15, 0.25, 0.35, 0.5, 0.7];
    let idx = steps.indexOf(domAgeW);
    if (idx < 0) idx = 3;
    domAgeW = steps[(idx + 1) % steps.length];
    updateExposureLabels();
  });

  CANVAS.addEventListener('pointerdown', (e) => {
    dragging = true;
    lastY = e.clientY;
    CANVAS.setPointerCapture(e.pointerId);
    autofollow = false;
    followBtn.textContent = 'Autofollow: OFF';
  });
  CANVAS.addEventListener('pointermove', (e) => {
    if (!dragging) return;
    const dy = (e.clientY - lastY);
    lastY = e.clientY;
    const pxPerScreen = (__RANGE_USD__ / zoom) * 2;
    const dPrice = (dy / window.innerHeight) * pxPerScreen;
    panPx += dPrice;
  });
  CANVAS.addEventListener('pointerup', (e) => {
    dragging = false;
    try { CANVAS.releasePointerCapture(e.pointerId); } catch(_) {}
  });
  CANVAS.addEventListener('wheel', (e) => {
    e.preventDefault();
    autofollow = false;
    followBtn.textContent = 'Autofollow: OFF';
    const delta = Math.sign(e.deltaY);
    const factor = (delta > 0) ? 1.08 : 0.92;
    zoom = Math.max(0.25, Math.min(6.0, zoom * factor));
  }, { passive: false });

  // Tick cadence (visibility-aware): avoids iOS background-throttle stalls and reduces CPU.
  const intervalMs = Math.max(250, Math.floor(1000 / Math.max(0.2, __SNAPSHOT_HZ__)));
  let tickTimer = null;

  async function scheduleLoop() {
    if (tickTimer) return;
    const loop = async () => {
      tickTimer = null;
      if (!document.hidden) {
        await tick();
      }
      tickTimer = setTimeout(loop, intervalMs);
    };
    tickTimer = setTimeout(loop, 0);
  }

  document.addEventListener('visibilitychange', () => {
    // When returning to the tab, force an immediate refresh.
    if (!document.hidden) {
      try { tick(); } catch (_) {}
    }
  });

  scheduleLoop();
})();
</script>
</body>
</html>
"""
    html = html.replace("__RANGE_USD__", str(RANGE_USD))
    html = html.replace("__BIN_USD__", str(BIN_USD))
    html = html.replace("__SNAPSHOT_HZ__", str(SNAPSHOT_HZ))
    html = html.replace("__BUILD__", BUILD)
    return html


@app.get("/snapshot")
async def snapshot(request: Request) -> JSONResponse:
    """UI snapshot.

    This handler must NEVER throw. On any exception it returns ok=false with a safe payload.
    """
    try:
        now_ms = _now_ms()
        bucket_mode = (request.query_params.get("bs") or BUCKET_SCALE_MODE_DEFAULT or "auto").strip().lower()

        async with STATE_LOCK:
            ws_ok = bool(STATE.get("ws_ok"))
            ws_last_rx_ms = STATE.get("ws_last_rx_ms")
            ws_msg_age_ms = (now_ms - int(ws_last_rx_ms)) if ws_last_rx_ms else None

            best_bid = STATE.get("best_bid")
            best_ask = STATE.get("best_ask")
            mid = STATE.get("mid")
            last_px = STATE.get("last_trade_px")

            center_px = mid
            if center_px is None:
                center_px = last_px
            if center_px is None and best_bid is not None and best_ask is not None:
                center_px = (float(best_bid) + float(best_ask)) / 2.0
            if center_px is None and best_bid is not None:
                center_px = float(best_bid)
            if center_px is None and best_ask is not None:
                center_px = float(best_ask)

            # View bounds anchored to price center (client pan/zoom is applied on the frontend).
            lo = float(center_px) - float(RANGE_USD) if center_px is not None else -float(RANGE_USD)
            hi = float(center_px) + float(RANGE_USD) if center_px is not None else float(RANGE_USD)

            # Near snapshot bins
            heat_bins: List[List[float]] = []
            heat_max = 0.0
            if center_px is not None:
                heat_bins, heat_max = _build_heat_bins(float(center_px), float(RANGE_USD), float(BIN_USD))

            # Wide ladder bins (session bounds)
            ladder_min = LADDER_MIN_PX
            ladder_max = LADDER_MAX_PX
            wide_heat_bins: List[List[float]] = []
            wide_heat_max = 0.0
            wide_bin_usd: Optional[float] = None

            if ladder_min is not None and ladder_max is not None and ladder_max > ladder_min:
                span = float(ladder_max - ladder_min)
                target_bins = 320.0
                wide_bin = max(float(BIN_USD), span / target_bins)
                if span / wide_bin > 500.0:
                    wide_bin = span / 500.0
                wide_bin_usd = float(wide_bin)
                wide_center = (float(ladder_min) + float(ladder_max)) / 2.0
                wide_range = span / 2.0
                wide_heat_bins, wide_heat_max = _build_heat_bins(wide_center, wide_range, float(wide_bin))

            # Buckets (single-layer projection; fixed nearest scale)
            bucket_bins: List[List[float]] = []
            bucket_max = 0.0
            bucket_usd = 0.0
            wide_bucket_bins: List[List[float]] = []
            wide_bucket_max = 0.0
            wide_bucket_usd: Optional[float] = None

            if center_px is not None:
                bucket_bins, bucket_max, bucket_usd = _build_bucket_bins(float(center_px), float(RANGE_USD), bucket_mode)
                if WIDE_LADDER_ON:
                    wide_bucket_bins, wide_bucket_max, wide_bucket_usd_val = _build_bucket_bins(float(center_px), float(WIDE_RANGE_USD), bucket_mode)
                    wide_bucket_usd = wide_bucket_usd_val
                # Distance activation: make far-away historic buckets dormant until price approaches.
                act_scale = float(MEM_ACTIVATION_USD)
                if act_scale > 0:
                    if bucket_bins:
                        mx2 = 0.0
                        for it in bucket_bins:
                            w = _activation_weight(float(center_px), float(it[0]), act_scale)
                            it[1] = float(it[1]) * w
                            if it[1] > mx2:
                                mx2 = it[1]
                        bucket_max = float(mx2)
                    if wide_bucket_bins:
                        mx3 = 0.0
                        for it in wide_bucket_bins:
                            w = _activation_weight(float(center_px), float(it[0]), act_scale)
                            it[1] = float(it[1]) * w
                            if it[1] > mx3:
                                mx3 = it[1]
                        wide_bucket_max = float(mx3)


        # Global ladder memory bins (decayed)
        mem_bins: List[List[float]] = []
        mem_max = 0.0
        wide_mem_bins: List[List[float]] = []
        wide_mem_max = 0.0
        wide_mem_usd: Optional[float] = None
        if MEM_ENABLE:
            _ladder_mem_decay(now_ms)
            # current view memory (distance-activated)
            act_scale = float(MEM_ACTIVATION_USD)
            for px, val in LADDER_MEM.items():
                if lo <= float(px) <= hi:
                    w = _activation_weight(float(center_px) if center_px is not None else float(px), float(px), act_scale)
                    vv = float(val) * w
                    if vv <= 0:
                        continue
                    mem_bins.append([float(px), float(vv)])
                    if vv > mem_max:
                        mem_max = float(vv)
            if center_px is not None:
                wlo = center_px - float(WIDE_RANGE_USD)
                whi = center_px + float(WIDE_RANGE_USD)
                for px, val in LADDER_MEM.items():
                    if wlo <= float(px) <= whi:
                        w = _activation_weight(float(center_px), float(px), float(MEM_ACTIVATION_USD))
                        vv = float(val) * w
                        if vv <= 0:
                            continue
                        wide_mem_bins.append([float(px), float(vv)])
                        if vv > wide_mem_max:
                            wide_mem_max = float(vv)
                wide_mem_usd = float(WIDE_RANGE_USD)
            payload = {
                "ok": True,
                "build": BUILD,
                "symbol": SYMBOL,
                "ws_ok": ws_ok,
                "ws_msg_age_ms": (float(ws_msg_age_ms) if ws_msg_age_ms is not None else None),
                "depth_age_ms": STATE.get("depth_age_ms"),
                "trade_age_ms": STATE.get("trade_age_ms"),
                "prints_60s": STATE.get("prints_60s", 0),
                "last_px": last_px,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "mid": mid,
                "book_bids_n": int(STATE.get("bids_n") or 0),
                "book_asks_n": int(STATE.get("asks_n") or 0),
                "center_px": center_px,
                "half_range": float(RANGE_USD),
                "bin_usd": float(BIN_USD),
                "heat_bins": heat_bins,
                "heat_max": float(heat_max),
                "bucket_mode": str(bucket_mode),
                "bucket_usd": float(bucket_usd),
                "bucket_bins": bucket_bins,
                "bucket_max": float(bucket_max),
                "ladder_min_px": (float(ladder_min) if ladder_min is not None else None),
                "ladder_max_px": (float(ladder_max) if ladder_max is not None else None),
                "wide_bin_usd": (float(wide_bin_usd) if wide_bin_usd is not None else None),
                "wide_heat_bins": wide_heat_bins,
                "wide_heat_max": float(wide_heat_max),
                "wide_bucket_usd": (float(wide_bucket_usd) if wide_bucket_usd is not None else None),
                "wide_bucket_bins": wide_bucket_bins,
                "wide_bucket_max": float(wide_bucket_max),
                "mem_bins": mem_bins,
                "mem_max": float(mem_max),
                "wide_mem_usd": (float(wide_mem_usd) if wide_mem_usd is not None else None),
                "wide_mem_bins": wide_mem_bins,
                "wide_mem_max": float(wide_mem_max),
                "error": STATE.get("last_error"),
            }
            return JSONResponse(payload)

    except Exception as e:
        payload = {
            "ok": False,
            "build": BUILD,
            "symbol": SYMBOL,
            "ws_ok": False,
            "ws_msg_age_ms": None,
            "depth_age_ms": None,
            "trade_age_ms": None,
            "prints_60s": 0,
            "last_px": None,
            "best_bid": None,
            "best_ask": None,
            "mid": None,
            "book_bids_n": 0,
            "book_asks_n": 0,
            "center_px": 0.0,
            "half_range": float(RANGE_USD),
            "bin_usd": float(BIN_USD),
            "heat_bins": [],
            "heat_max": 0.0,
            "bucket_mode": (request.query_params.get("bs") if request else None),
            "bucket_bins": [],
            "bucket_max": 0.0,
            "error": f"snapshot_exception: {type(e).__name__}: {e}",
        }
        return JSONResponse(payload)


# ---------------------------
# Local run (Replit uses: python app.py)
# ---------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, log_level="info")
