# =========================================================
# QuantDesk Bookmap Service — FOUNDATION FIX3 (Replit Stable)
# - No JavaScript embedded in Python execution
# - Uses FastAPI startup task (no threads)
# - Binds to Replit PORT env (falls back to 5000)
# - Provides / and /state for sanity verification
# =========================================================

import os
import json
import time
import asyncio
from typing import Any, Dict

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

try:
    import websockets  # type: ignore
except Exception:
    websockets = None  # allows app to stay up even if WS dep missing


# ---------------------------
# Config
# ---------------------------
SYMBOL = os.getenv("QD_SYMBOL_WS", "BTC_USDT")
MEXC_WS = os.getenv("QD_WS_URL", "wss://contract.mexc.com/edge")

# Replit exposes PORT; user wanted 5000 but we honor PORT if present.
PORT = int(os.getenv("PORT", os.getenv("QD_PORT", "5000")))

app = FastAPI(title="QuantDesk Bookmap Foundation")

STATE: Dict[str, Any] = {
    "service": "quantdesk-bookmap-foundation",
    "build": "FOUNDATION_FIX3",
    "symbol": SYMBOL,
    "ws_url": MEXC_WS,
    "ws_ok": False,
    "ws_last_rx_ms": None,
    "last_trade": None,
    "best_bid": None,
    "best_ask": None,
    "mid": None,
    "bids_n": 0,
    "asks_n": 0,
    "depth_age_ms": None,
    "trade_age_ms": None,
    "last_error": None,
}


# ---------------------------
# Helpers
# ---------------------------
def _now_ms() -> int:
    return int(time.time() * 1000)


def _set_err(msg: str) -> None:
    STATE["last_error"] = msg


def _update_top_of_book() -> None:
    # We only keep counts + top levels derived from current dicts.
    # Dicts are stored in closure globals below.
    pass


# ---------------------------
# In-memory orderbook (foundation)
# ---------------------------
BIDS: Dict[float, float] = {}
ASKS: Dict[float, float] = {}
LAST_DEPTH_MS: int | None = None
LAST_TRADE_MS: int | None = None


def _recompute_tob() -> None:
    global BIDS, ASKS
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


# ---------------------------
# MEXC WS consumer
# ---------------------------
async def mexc_consumer_loop() -> None:
    """
    Subscribes to:
      - push.depth (levels [price, qty, count], qty=0 means remove)
      - push.deal  (prints p, v, T(1 buy,2 sell), t ms)
    Keeps minimal state so we can verify stream is alive.
    """
    global LAST_DEPTH_MS, LAST_TRADE_MS

    if websockets is None:
        STATE["ws_ok"] = False
        _set_err("Missing dependency: websockets (pip install websockets)")
        return

    while True:
        try:
            async with websockets.connect(MEXC_WS, ping_interval=20, ping_timeout=20) as ws:
                # Subscribe
                await ws.send(json.dumps({"method": "sub.depth", "param": {"symbol": SYMBOL}}))
                await ws.send(json.dumps({"method": "sub.deal", "param": {"symbol": SYMBOL}}))

                STATE["ws_ok"] = True
                STATE["last_error"] = None

                async for raw in ws:
                    STATE["ws_last_rx_ms"] = _now_ms()

                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    # We key on msg['channel'] when present; otherwise string search.
                    ch = msg.get("channel") or msg.get("c") or ""
                    if not ch:
                        # fallback heuristic
                        s = json.dumps(msg)
                        if "push.depth" in s:
                            ch = "push.depth"
                        elif "push.deal" in s:
                            ch = "push.deal"

                    if "push.depth" in ch:
                        data = msg.get("data") or {}
                        bids = data.get("bids") or []
                        asks = data.get("asks") or []

                        # Apply updates without wiping (partial updates safe)
                        for lvl in bids:
                            if not isinstance(lvl, (list, tuple)) or len(lvl) < 2:
                                continue
                            p = float(lvl[0]); q = float(lvl[1])
                            if q <= 0.0:
                                BIDS.pop(p, None)
                            else:
                                BIDS[p] = q

                        for lvl in asks:
                            if not isinstance(lvl, (list, tuple)) or len(lvl) < 2:
                                continue
                            p = float(lvl[0]); q = float(lvl[1])
                            if q <= 0.0:
                                ASKS.pop(p, None)
                            else:
                                ASKS[p] = q

                        LAST_DEPTH_MS = _now_ms()
                        _recompute_tob()

                    elif "push.deal" in ch:
                        deals = msg.get("data") or []
                        # data can be list of prints
                        if isinstance(deals, dict):
                            deals = [deals]
                        if isinstance(deals, list) and deals:
                            d0 = deals[-1]
                            try:
                                STATE["last_trade"] = float(d0.get("p")) if d0.get("p") is not None else STATE["last_trade"]
                            except Exception:
                                pass
                            LAST_TRADE_MS = _now_ms()
                            _recompute_tob()

        except Exception as e:
            STATE["ws_ok"] = False
            _set_err(f"WS error: {type(e).__name__}: {e}")
            await asyncio.sleep(2.0)


# ---------------------------
# FastAPI lifecycle
# ---------------------------
@app.on_event("startup")
async def _startup() -> None:
    # Start background consumer as a task in the same event loop
    asyncio.create_task(mexc_consumer_loop())


# ---------------------------
# Routes
# ---------------------------
@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>QuantDesk Bookmap — Foundation</title>
  <style>
    body {{
      margin: 0;
      background: #0b0f14;
      color: #9ad;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      padding: 16px;
    }}
    .box {{
      border: 1px solid rgba(255,255,255,0.12);
      background: rgba(255,255,255,0.03);
      border-radius: 12px;
      padding: 12px 14px;
      max-width: 900px;
    }}
    .ok {{ color: #7CFC98; }}
    .bad {{ color: #FF6B6B; }}
    .muted {{ color: rgba(155,205,255,0.65); }}
  </style>
</head>
<body>
  <div class="box">
    <div><strong>QuantDesk Bookmap — FOUNDATION_FIX3</strong></div>
    <div class="muted">Symbol: {SYMBOL} | WS: {MEXC_WS}</div>
    <div id="s" style="margin-top:10px;">Loading…</div>
    <div class="muted" style="margin-top:8px;">This page is only a health probe. Heatmap comes after foundation PASS.</div>
  </div>

<script>
async function tick(){{
  try {{
    const r = await fetch('/state', {{cache:'no-store'}});
    const d = await r.json();
    const ok = d.ws_ok ? 'ok' : 'bad';
    const err = d.last_error ? ('<div class="bad">ERR: ' + d.last_error + '</div>') : '';
    document.getElementById('s').innerHTML =
      '<div class="'+ok+'">WS: ' + (d.ws_ok ? 'OK' : 'DOWN') + '</div>' +
      '<div>last_trade: ' + (d.last_trade ?? '-') + ' | bid: ' + (d.best_bid ?? '-') + ' | ask: ' + (d.best_ask ?? '-') + ' | mid: ' + (d.mid ?? '-') + '</div>' +
      '<div>bids_n: ' + d.bids_n + ' | asks_n: ' + d.asks_n + '</div>' +
      '<div>depth_age_ms: ' + (d.depth_age_ms ?? '-') + ' | trade_age_ms: ' + (d.trade_age_ms ?? '-') + '</div>' +
      err;
  }} catch(e) {{
    document.getElementById('s').innerHTML = '<div class="bad">UI fetch error: ' + e + '</div>';
  }}
}}
setInterval(tick, 500);
tick();
</script>
</body>
</html>
"""


@app.get("/state")
async def state() -> JSONResponse:
    return JSONResponse(STATE)


# ---------------------------
# Local run (Replit typically uses "python app.py")
# ---------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, log_level="info")
