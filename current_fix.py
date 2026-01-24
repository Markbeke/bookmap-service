QuantDesk Bookmap Service — current_fix.py
Build: FIX07/P01
Subsystem: FOUNDATION+STREAM+STATE_ENGINE (MEXC CONTRACT WS)
Date: 2026-01-25

FIX07 objective (Perpetuals clean restart):
- Switch from MEXC SPOT WS (wbs.mexc.com) to MEXC FUTURES/CONTRACT WS endpoint.
- Subscribe to BTC_USDT perpetuals market data using documented futures WS methods:
  - sub.depth  -> channel push.depth (order book)
  - sub.deal   -> channel push.deal  (recent trades)
- Parse push.depth into canonical aggregated book (top levels as provided) and mark engine ACTIVE.
- Keep FastAPI service + health/telemetry endpoints per Workflow v1.2.

Authoritative reference (MEXC Futures WebSocket API):
- Native endpoint: wss://contract.mexc.com/edge
- Ping: {"method":"ping"} (send every 10–20 seconds; server replies channel=pong)
- Deal subscription: {"method":"sub.deal","param":{"symbol":"BTC_USDT"}}
- Depth subscription: {"method":"sub.depth","param":{"symbol":"BTC_USDT"}}
- Depth payload: channel=push.depth, data:{asks:[[price, orderCount, qty]], bids:[[...]], version:...}
  Docs note: [price, orderNumbers, orderQuantity].
Sources: https://www.mexc.com/api-docs/futures/websocket-api (see Deal & Order book depth sections).

Notes / limits:
- This FIX intentionally targets BTC_USDT perpetuals only.
- This FIX treats each push.depth as a "snapshot" replace of current depth view (as provided by exchange).
  If MEXC later requires incremental maintenance rules, we will implement that in the next FIX.
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from dataclasses import dataclass, asdict, field
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

# -----------------------------
# Config
# -----------------------------
SERVICE = "qd-bookmap"
BUILD = "FIX07/P01"
SUBSYSTEM = "FOUNDATION+STREAM+STATE_ENGINE"

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "5000"))

WS_URL = os.environ.get("QD_MEXC_CONTRACT_WS", "wss://contract.mexc.com/edge").strip()

# Hard target (per user constraint): BTC_USDT perpetuals only
SYMBOL = os.environ.get("QD_SYMBOL", "BTC_USDT").strip().upper()

PING_SECS = float(os.environ.get("QD_WS_PING_SECS", "15"))  # 10–20s recommended by docs
BOOK_TOP_N = int(os.environ.get("QD_BOOK_TOP_N", "25"))

# Optional: request uncompressed trade aggregation (docs mention compress flag for deal stream)
DEAL_COMPRESS = os.environ.get("QD_DEAL_COMPRESS", "false").strip().lower() in ("1", "true", "yes", "y", "on")

RAW_RING_MAX = int(os.environ.get("QD_RAW_RING_MAX", "40"))

# -----------------------------
# Imports (dependency-safe)
# -----------------------------
IMPORT_ERRORS: List[str] = []
try:
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse, HTMLResponse
except Exception as e:
    FastAPI = None  # type: ignore
    JSONResponse = None  # type: ignore
    HTMLResponse = None  # type: ignore
    IMPORT_ERRORS.append(f"fastapi import failed: {type(e).__name__}: {e}")

try:
    import uvicorn
except Exception as e:
    uvicorn = None  # type: ignore
    IMPORT_ERRORS.append(f"uvicorn import failed: {type(e).__name__}: {e}")

try:
    import websockets
except Exception as e:
    websockets = None  # type: ignore
    IMPORT_ERRORS.append(f"websockets import failed: {type(e).__name__}: {e}")


# -----------------------------
# Utilities
# -----------------------------
START_TS = time.time()


def now_ts() -> float:
    return time.time()


def safe_exc(e: BaseException) -> str:
    s = f"{type(e).__name__}: {str(e)}".strip()
    return s[:800]


class Ring:
    def __init__(self, maxlen: int) -> None:
        self.maxlen = maxlen
        self._buf: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def push(self, item: Dict[str, Any]) -> None:
        with self._lock:
            self._buf.append(item)
            if len(self._buf) > self.maxlen:
                self._buf = self._buf[-self.maxlen :]

    def snapshot(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._buf)


RAW_RING = Ring(RAW_RING_MAX)


# -----------------------------
# State
# -----------------------------
@dataclass
class Connector:
    status: str = "DISCONNECTED"  # DISCONNECTED | CONNECTED | ERROR
    ws_url: str = WS_URL
    symbol: str = SYMBOL
    frames_total: int = 0
    reconnects: int = 0
    last_connect_ts: float = 0.0
    last_event_ts: float = 0.0
    last_error: str = ""
    last_close_code: Optional[int] = None
    last_close_reason: str = ""
    import_errors: List[str] = field(default_factory=list)


@dataclass
class Engine:
    status: str = "WARMING"  # WARMING | ACTIVE | ERROR
    snapshots_applied: int = 0
    updates_applied: int = 0
    parse_hits: int = 0
    parse_misses: int = 0
    last_apply_ts: float = 0.0
    last_msg_kind: str = ""
    invariants_ok: bool = True
    last_invariant_violation: str = ""
    last_error: str = ""


@dataclass
class TradeTape:
    last_trade_px: Optional[str] = None
    last_trade_side: Optional[str] = None  # BUY/SELL
    last_trade_qty: Optional[str] = None
    last_trade_ts: float = 0.0
    trades_seen: int = 0


CONNECTOR = Connector(import_errors=IMPORT_ERRORS[:])
ENGINE = Engine()
TAPE = TradeTape()

LOCK = threading.Lock()


class Book:
    def __init__(self) -> None:
        self.bids: Dict[Decimal, Decimal] = {}
        self.asks: Dict[Decimal, Decimal] = {}
        self.version: Optional[int] = None
        self.last_update_ts: float = 0.0

    def clear(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.version = None
        self.last_update_ts = 0.0

    def best_bid(self) -> Optional[Tuple[Decimal, Decimal]]:
        if not self.bids:
            return None
        p = max(self.bids.keys())
        return p, self.bids[p]

    def best_ask(self) -> Optional[Tuple[Decimal, Decimal]]:
        if not self.asks:
            return None
        p = min(self.asks.keys())
        return p, self.asks[p]

    def to_top_n(self, n: int) -> Dict[str, Any]:
        bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]
        asks = sorted(self.asks.items(), key=lambda x: x[0])[:n]
        return {
            "bids": [[str(p), str(q)] for p, q in bids],
            "asks": [[str(p), str(q)] for p, q in asks],
            "version": self.version,
            "ts": self.last_update_ts,
        }


BOOK = Book()


def to_decimal(x: Any) -> Optional[Decimal]:
    try:
        if x is None:
            return None
        if isinstance(x, Decimal):
            return x
        if isinstance(x, (int, float)):
            return Decimal(str(x))
        if isinstance(x, str):
            s = x.strip()
            if not s:
                return None
            return Decimal(s)
    except (InvalidOperation, ValueError):
        return None
    return None


def parse_depth_levels(side: Any) -> Optional[List[Tuple[Decimal, Decimal]]]:
    """
    MEXC futures push.depth format: [[price, orderNumbers, orderQuantity], ...]
    We use price and orderQuantity (index 0 and 2). If only [price, qty] is present,
    we accept that too.
    """
    if not isinstance(side, list):
        return None
    out: List[Tuple[Decimal, Decimal]] = []
    for row in side:
        if not isinstance(row, list) or len(row) < 2:
            return None
        p = to_decimal(row[0])
        q = to_decimal(row[2] if len(row) >= 3 else row[1])
        if p is None or q is None:
            return None
        out.append((p, q))
    return out


def apply_depth_snapshot(data: Dict[str, Any]) -> bool:
    asks = data.get("asks")
    bids = data.get("bids")
    ver = data.get("version")
    asks_lv = parse_depth_levels(asks)
    bids_lv = parse_depth_levels(bids)
    if asks_lv is None or bids_lv is None:
        return False

    # Replace book with latest view
    BOOK.asks = {p: q for p, q in asks_lv if q > 0}
    BOOK.bids = {p: q for p, q in bids_lv if q > 0}
    if isinstance(ver, int):
        BOOK.version = ver
    BOOK.last_update_ts = now_ts()

    ok, viol = check_invariants()
    with LOCK:
        ENGINE.invariants_ok = ok
        ENGINE.last_invariant_violation = viol
        if not ok:
            ENGINE.status = "ERROR"
            ENGINE.last_error = viol
    return ok


def check_invariants() -> Tuple[bool, str]:
    bb = BOOK.best_bid()
    ba = BOOK.best_ask()
    if bb and ba and bb[0] >= ba[0]:
        return False, f"crossed_book: best_bid={bb[0]} best_ask={ba[0]}"
    return True, ""


# -----------------------------
# WebSocket loop
# -----------------------------
def sub_depth_msg() -> Dict[str, Any]:
    return {"method": "sub.depth", "param": {"symbol": SYMBOL}}


def sub_deal_msg() -> Dict[str, Any]:
    # Docs show {"method":"sub.deal","param":{"symbol":"BTC_USDT"}}
    # Docs also mention `compress=false` to disable aggregation; we include when requested.
    param: Dict[str, Any] = {"symbol": SYMBOL}
    if DEAL_COMPRESS is False:
        param["compress"] = False
    return {"method": "sub.deal", "param": param}


async def ping_loop(ws) -> None:
    while True:
        try:
            await ws.send(json.dumps({"method": "ping"}))
        except Exception:
            return
        await asyncio.sleep(PING_SECS)


async def ws_once() -> None:
    if websockets is None:
        with LOCK:
            CONNECTOR.status = "ERROR"
            CONNECTOR.last_error = "websockets dependency missing"
        return

    async with websockets.connect(
        WS_URL,
        ping_interval=None,  # we handle ping explicitly per MEXC spec
        close_timeout=10,
        max_queue=128,
    ) as ws:
        with LOCK:
            CONNECTOR.status = "CONNECTED"
            CONNECTOR.last_connect_ts = now_ts()
            CONNECTOR.last_error = ""
            CONNECTOR.last_close_code = None
            CONNECTOR.last_close_reason = ""

        # Start ping task
        ptask = asyncio.create_task(ping_loop(ws))

        # Subscribe
        await ws.send(json.dumps(sub_depth_msg()))
        await ws.send(json.dumps(sub_deal_msg()))

        while True:
            msg = await ws.recv()
            ts = now_ts()
            with LOCK:
                CONNECTOR.frames_total += 1
                CONNECTOR.last_event_ts = ts

            # Decode
            if isinstance(msg, bytes):
                try:
                    msg_s = msg.decode("utf-8", errors="replace")
                except Exception:
                    msg_s = str(msg)
            else:
                msg_s = str(msg)

            RAW_RING.push({"ts": ts, "raw": msg_s[:4000]})

            # Parse JSON
            try:
                j = json.loads(msg_s)
            except Exception:
                with LOCK:
                    ENGINE.parse_misses += 1
                continue

            if not isinstance(j, dict):
                with LOCK:
                    ENGINE.parse_misses += 1
                continue

            ch = str(j.get("channel", ""))
            sym = str(j.get("symbol", ""))

            if sym and sym.upper() != SYMBOL.upper():
                # ignore other symbols
                continue

            if ch == "push.depth":
                data = j.get("data")
                if isinstance(data, dict):
                    ok = apply_depth_snapshot(data)
                    with LOCK:
                        ENGINE.parse_hits += 1
                        ENGINE.snapshots_applied += 1
                        ENGINE.updates_applied += 1
                        ENGINE.last_apply_ts = ts
                        ENGINE.last_msg_kind = "snapshot"
                        if ok and ENGINE.status != "ERROR":
                            ENGINE.status = "ACTIVE"
                            ENGINE.last_error = ""
                else:
                    with LOCK:
                        ENGINE.parse_misses += 1

            elif ch == "push.deal":
                # Trades
                data = j.get("data")
                if isinstance(data, list) and data:
                    first = data[0]
                    if isinstance(first, dict):
                        px = first.get("p")
                        qty = first.get("v")
                        side = first.get("T")  # 1 buy, 2 sell (per docs)
                        side_s = "BUY" if str(side) == "1" else ("SELL" if str(side) == "2" else None)
                        with LOCK:
                            TAPE.trades_seen += len(data)
                            TAPE.last_trade_px = str(px) if px is not None else TAPE.last_trade_px
                            TAPE.last_trade_qty = str(qty) if qty is not None else TAPE.last_trade_qty
                            TAPE.last_trade_side = side_s or TAPE.last_trade_side
                            TAPE.last_trade_ts = ts
                with LOCK:
                    ENGINE.parse_hits += 1
                    ENGINE.last_msg_kind = "trade"
                    ENGINE.last_apply_ts = ts

            elif ch == "pong":
                # keepalive response
                continue
            else:
                # ignore other channels for now
                continue

        ptask.cancel()


async def ws_forever() -> None:
    backoff = 0.5
    backoff_max = 15.0
    while True:
        try:
            await ws_once()
            backoff = 0.5
        except asyncio.CancelledError:
            raise
        except Exception as e:
            with LOCK:
                CONNECTOR.status = "ERROR"
                CONNECTOR.last_error = safe_exc(e)
                CONNECTOR.reconnects += 1
            await asyncio.sleep(backoff + random.random() * 0.2)
            backoff = min(backoff * 1.7, backoff_max)


def start_ws_thread() -> None:
    def runner() -> None:
        try:
            asyncio.run(ws_forever())
        except Exception as e:
            with LOCK:
                CONNECTOR.status = "ERROR"
                CONNECTOR.last_error = f"ws_thread_crashed: {safe_exc(e)}"

    threading.Thread(target=runner, name="qd_ws", daemon=True).start()


# -----------------------------
# FastAPI app
# -----------------------------
if FastAPI is None or JSONResponse is None or HTMLResponse is None:
    # Hard fail would violate foundation contract; provide minimal HTTP server via stdlib.
    import http.server
    import socketserver

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            body = json.dumps(
                {
                    "service": SERVICE,
                    "build": BUILD,
                    "subsystem": SUBSYSTEM,
                    "health": "RED",
                    "error": "FastAPI import missing",
                    "import_errors": IMPORT_ERRORS,
                }
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def main():
        with socketserver.TCPServer((HOST, PORT), Handler) as httpd:
            httpd.serve_forever()

else:
    app = FastAPI(title="QuantDesk Bookmap", version=BUILD)

    @app.on_event("startup")
    def _startup() -> None:
        start_ws_thread()

    def compute_health() -> str:
        with LOCK:
            # GREEN = engine ACTIVE + recent book update
            if ENGINE.status == "ACTIVE" and (now_ts() - BOOK.last_update_ts) < 5.0 and BOOK.best_bid() and BOOK.best_ask():
                return "GREEN"
            # YELLOW = service up but warming/no book yet
            if CONNECTOR.status in ("CONNECTED", "ERROR"):
                return "YELLOW"
            return "RED"

    @app.get("/")
    def root():
        h = compute_health()
        with LOCK:
            payload = {
                "service": SERVICE,
                "build": BUILD,
                "subsystem": SUBSYSTEM,
                "health": h,
                "uptime_s": round(now_ts() - START_TS, 3),
                "connector": asdict(CONNECTOR),
                "engine": asdict(ENGINE),
                "tape": asdict(TAPE),
                "best_bid_ask": {
                    "bid": [str(BOOK.best_bid()[0]), str(BOOK.best_bid()[1])] if BOOK.best_bid() else None,
                    "ask": [str(BOOK.best_ask()[0]), str(BOOK.best_ask()[1])] if BOOK.best_ask() else None,
                },
            }
        return HTMLResponse(f"<pre>{json.dumps(payload, indent=2)}</pre>")

    @app.get("/health.json")
    def health_json():
        return JSONResponse(
            {
                "service": SERVICE,
                "build": BUILD,
                "subsystem": SUBSYSTEM,
                "health": compute_health(),
                "uptime_s": round(now_ts() - START_TS, 3),
                "connector": asdict(CONNECTOR),
                "engine": asdict(ENGINE),
                "tape": asdict(TAPE),
            }
        )

    @app.get("/telemetry.json")
    def telemetry_json():
        with LOCK:
            return JSONResponse(
                {
                    "service": SERVICE,
                    "build": BUILD,
                    "subsystem": SUBSYSTEM,
                    "uptime_s": round(now_ts() - START_TS, 3),
                    "connector": asdict(CONNECTOR),
                    "engine": asdict(ENGINE),
                    "tape": asdict(TAPE),
                }
            )

    @app.get("/book.json")
    def book_json():
        return JSONResponse(
            {
                "service": SERVICE,
                "build": BUILD,
                "symbol": SYMBOL,
                "book": BOOK.to_top_n(BOOK_TOP_N),
                "best_bid": [str(BOOK.best_bid()[0]), str(BOOK.best_bid()[1])] if BOOK.best_bid() else None,
                "best_ask": [str(BOOK.best_ask()[0]), str(BOOK.best_ask()[1])] if BOOK.best_ask() else None,
            }
        )

    @app.get("/raw.json")
    def raw_json():
        return JSONResponse(
            {
                "service": SERVICE,
                "build": BUILD,
                "symbol": SYMBOL,
                "raw_ring": RAW_RING.snapshot(),
            }
        )

    def main():
        uvicorn.run(app, host=HOST, port=PORT, log_level="warning")


if __name__ == "__main__":
    main()
