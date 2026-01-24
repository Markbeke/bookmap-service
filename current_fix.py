# QuantDesk Bookmap Service - current_fix.py
# Build: FIX08/P01
# Subsystem: FOUNDATION+STREAM+GLOBAL_LADDER_STATE_ENGINE
# Venue: MEXC Futures (BTC_USDT Perpetuals)
# Date: 2026-01-25
#
# FIX08 objective:
# - Keep CONTRACT WS connectivity proven in FIX07.
# - Upgrade state engine from top-of-book to a global ladder (full depth view as delivered by push.depth).
# - Maintain deterministic, single-writer book state (replace-on-snapshot model per push.depth message).
# - Expose ladder state and invariants via JSON endpoints.
#
# Notes (truth/limits):
# - This build treats each push.depth as a snapshot replacement of current depth view.
# - If MEXC requires incremental maintenance (diffs), that will be a later FIX based on observed payloads.
# - This build does not implement heatmap/visualization yet.

import asyncio
import json
import os
import threading
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI
from fastapi.responses import JSONResponse, HTMLResponse
import websockets
import uvicorn

SERVICE = "qd-bookmap"
BUILD = "FIX08/P01"
SUBSYSTEM = "FOUNDATION+STREAM+GLOBAL_LADDER_STATE_ENGINE"

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 5000))

WS_URL = os.environ.get("QD_MEXC_CONTRACT_WS", "wss://contract.mexc.com/edge").strip()
SYMBOL = os.environ.get("QD_SYMBOL", "BTC_USDT").strip().upper()

PING_SECS = float(os.environ.get("QD_WS_PING_SECS", "15"))
TOP_N = int(os.environ.get("QD_BOOK_TOP_N", "25"))
RAW_RING_MAX = int(os.environ.get("QD_RAW_RING_MAX", "40"))
FRESH_SECS_GREEN = float(os.environ.get("QD_FRESH_SECS_GREEN", "5.0"))

DEAL_COMPRESS = os.environ.get("QD_DEAL_COMPRESS", "false").strip().lower() in ("1", "true", "yes", "y", "on")


def _now() -> float:
    return time.time()


def _safe_str(e: BaseException) -> str:
    return f"{type(e).__name__}: {str(e)}"[:800]


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
START_TS = _now()

LOCK = threading.Lock()

# -----------------------------
# Canonical global ladder book
# -----------------------------
class GlobalLadderBook:
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

    def depth_counts(self) -> Dict[str, int]:
        return {"bids": len(self.bids), "asks": len(self.asks)}

    def top_n(self, n: int) -> Dict[str, List[List[str]]]:
        bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]
        asks = sorted(self.asks.items(), key=lambda x: x[0])[:n]
        return {
            "bids": [[str(p), str(q)] for p, q in bids],
            "asks": [[str(p), str(q)] for p, q in asks],
        }

    def totals(self) -> Dict[str, str]:
        bid_qty = sum(self.bids.values(), Decimal("0"))
        ask_qty = sum(self.asks.values(), Decimal("0"))
        return {"bid_qty": str(bid_qty), "ask_qty": str(ask_qty)}


BOOK = GlobalLadderBook()


def _to_dec(x: Any) -> Optional[Decimal]:
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


def _parse_depth_side(side: Any) -> Optional[List[Tuple[Decimal, Decimal]]]:
    """
    MEXC futures push.depth format: [[price, orderCount, qty], ...]
    Use price (index 0) and qty (index 2). If only [price, qty] exists, accept (index 1).
    """
    if not isinstance(side, list):
        return None
    out: List[Tuple[Decimal, Decimal]] = []
    for row in side:
        if not isinstance(row, list) or len(row) < 2:
            return None
        p = _to_dec(row[0])
        q = _to_dec(row[2] if len(row) >= 3 else row[1])
        if p is None or q is None:
            return None
        out.append((p, q))
    return out


def _check_invariants() -> Tuple[bool, str]:
    bb = BOOK.best_bid()
    ba = BOOK.best_ask()
    if bb and ba and bb[0] >= ba[0]:
        return False, f"crossed_book best_bid={bb[0]} best_ask={ba[0]}"
    return True, ""


# -----------------------------
# Runtime state / telemetry
# -----------------------------
state: Dict[str, Any] = {
    "status": "DISCONNECTED",  # DISCONNECTED | CONNECTED | ERROR
    "frames": 0,
    "reconnects": 0,
    "last_error": "",
    "last_close_code": None,
    "last_close_reason": "",
    "last_connect_ts": 0.0,
    "last_event_ts": 0.0,
    "engine": "WARMING",  # WARMING | ACTIVE | ERROR
    "snapshots_applied": 0,
    "parse_hits": 0,
    "parse_misses": 0,
    "invariants_ok": True,
    "last_invariant_violation": "",
    "last_trade_px": None,
    "last_trade_side": None,
    "last_trade_qty": None,
    "last_trade_ts": 0.0,
    "trades_seen": 0,
}


def _health() -> str:
    # GREEN: ACTIVE + fresh + non-empty bid/ask
    with LOCK:
        fresh = (_now() - BOOK.last_update_ts) <= FRESH_SECS_GREEN
        bb = BOOK.best_bid()
        ba = BOOK.best_ask()
        if state["engine"] == "ACTIVE" and fresh and bb and ba and state["invariants_ok"]:
            return "GREEN"
        if state["status"] in ("CONNECTED", "ERROR"):
            return "YELLOW"
        return "RED"


def _sub_depth_msg() -> Dict[str, Any]:
    return {"method": "sub.depth", "param": {"symbol": SYMBOL}}


def _sub_deal_msg() -> Dict[str, Any]:
    param: Dict[str, Any] = {"symbol": SYMBOL}
    if DEAL_COMPRESS is False:
        param["compress"] = False
    return {"method": "sub.deal", "param": param}


async def _ping_loop(ws) -> None:
    while True:
        try:
            await ws.send(json.dumps({"method": "ping"}))
        except Exception:
            return
        await asyncio.sleep(PING_SECS)


def _apply_depth_snapshot(depth_data: Dict[str, Any]) -> None:
    asks_lv = _parse_depth_side(depth_data.get("asks"))
    bids_lv = _parse_depth_side(depth_data.get("bids"))
    ver = depth_data.get("version")

    if asks_lv is None or bids_lv is None:
        state["parse_misses"] += 1
        return

    # Replace-on-snapshot global ladder
    BOOK.asks = {p: q for p, q in asks_lv if q > 0}
    BOOK.bids = {p: q for p, q in bids_lv if q > 0}
    if isinstance(ver, int):
        BOOK.version = ver
    BOOK.last_update_ts = _now()

    ok, viol = _check_invariants()
    state["invariants_ok"] = ok
    state["last_invariant_violation"] = viol
    if not ok:
        state["engine"] = "ERROR"
        state["last_error"] = viol
    else:
        state["engine"] = "ACTIVE"
        state["last_error"] = ""
    state["snapshots_applied"] += 1
    state["parse_hits"] += 1


async def _ws_once() -> None:
    async with websockets.connect(
        WS_URL,
        ping_interval=None,
        close_timeout=10,
        max_queue=256,
    ) as ws:
        with LOCK:
            state["status"] = "CONNECTED"
            state["last_connect_ts"] = _now()
            state["last_error"] = ""
            state["last_close_code"] = None
            state["last_close_reason"] = ""

        ptask = asyncio.create_task(_ping_loop(ws))

        await ws.send(json.dumps(_sub_depth_msg()))
        await ws.send(json.dumps(_sub_deal_msg()))

        while True:
            msg = await ws.recv()
            ts = _now()
            with LOCK:
                state["frames"] += 1
                state["last_event_ts"] = ts

            if isinstance(msg, bytes):
                msg_s = msg.decode("utf-8", errors="replace")
            else:
                msg_s = str(msg)

            RAW_RING.push({"ts": ts, "raw": msg_s[:4000]})

            try:
                j = json.loads(msg_s)
            except Exception:
                with LOCK:
                    state["parse_misses"] += 1
                continue

            if not isinstance(j, dict):
                with LOCK:
                    state["parse_misses"] += 1
                continue

            ch = str(j.get("channel", ""))
            sym = str(j.get("symbol", "")).upper() if j.get("symbol") else ""

            if sym and sym != SYMBOL.upper():
                continue

            with LOCK:
                if ch == "push.depth":
                    data = j.get("data")
                    if isinstance(data, dict):
                        _apply_depth_snapshot(data)
                    else:
                        state["parse_misses"] += 1

                elif ch == "push.deal":
                    data = j.get("data")
                    if isinstance(data, list) and data:
                        first = data[0]
                        if isinstance(first, dict):
                            px = first.get("p")
                            qty = first.get("v")
                            side = first.get("T")  # 1 buy, 2 sell (per docs)
                            side_s = "BUY" if str(side) == "1" else ("SELL" if str(side) == "2" else None)
                            state["trades_seen"] += len(data)
                            state["last_trade_px"] = str(px) if px is not None else state["last_trade_px"]
                            state["last_trade_qty"] = str(qty) if qty is not None else state["last_trade_qty"]
                            state["last_trade_side"] = side_s or state["last_trade_side"]
                            state["last_trade_ts"] = ts
                    state["parse_hits"] += 1

                elif ch == "pong":
                    pass
                else:
                    # ignore other channels
                    pass

        ptask.cancel()


async def _ws_forever() -> None:
    backoff = 0.5
    backoff_max = 15.0
    while True:
        try:
            await _ws_once()
            backoff = 0.5
        except asyncio.CancelledError:
            raise
        except Exception as e:
            with LOCK:
                state["status"] = "ERROR"
                state["last_error"] = _safe_str(e)
                state["reconnects"] += 1
            await asyncio.sleep(backoff + (0.2 * (os.getpid() % 5)))
            backoff = min(backoff * 1.7, backoff_max)


def _start_ws_thread() -> None:
    def runner() -> None:
        try:
            asyncio.run(_ws_forever())
        except Exception as e:
            with LOCK:
                state["status"] = "ERROR"
                state["last_error"] = "ws_thread_crashed: " + _safe_str(e)

    threading.Thread(target=runner, name="qd_ws", daemon=True).start()


# -----------------------------
# FastAPI app
# -----------------------------
app = FastAPI(title="QuantDesk Bookmap", version=BUILD)


@app.on_event("startup")
def startup() -> None:
    _start_ws_thread()


@app.get("/")
def root() -> HTMLResponse:
    payload = {
        "service": SERVICE,
        "build": BUILD,
        "subsystem": SUBSYSTEM,
        "health": _health(),
        "uptime_s": round(_now() - START_TS, 3),
        "ws_url": WS_URL,
        "symbol": SYMBOL,
        "state": state,
        "book": {
            "best_bid": [str(BOOK.best_bid()[0]), str(BOOK.best_bid()[1])] if BOOK.best_bid() else None,
            "best_ask": [str(BOOK.best_ask()[0]), str(BOOK.best_ask()[1])] if BOOK.best_ask() else None,
            "depth_counts": BOOK.depth_counts(),
            "totals": BOOK.totals(),
            "version": BOOK.version,
            "last_update_ts": BOOK.last_update_ts,
        },
    }
    return HTMLResponse("<pre>" + json.dumps(payload, indent=2) + "</pre>")


@app.get("/health.json")
def health_json() -> JSONResponse:
    return JSONResponse(
        {
            "service": SERVICE,
            "build": BUILD,
            "subsystem": SUBSYSTEM,
            "health": _health(),
            "uptime_s": round(_now() - START_TS, 3),
            "ws_url": WS_URL,
            "symbol": SYMBOL,
        }
    )


@app.get("/telemetry.json")
def telemetry_json() -> JSONResponse:
    with LOCK:
        bb = BOOK.best_bid()
        ba = BOOK.best_ask()
        return JSONResponse(
            {
                "service": SERVICE,
                "build": BUILD,
                "subsystem": SUBSYSTEM,
                "health": _health(),
                "uptime_s": round(_now() - START_TS, 3),
                "connector": {
                    "status": state["status"],
                    "frames": state["frames"],
                    "reconnects": state["reconnects"],
                    "last_error": state["last_error"],
                    "last_connect_ts": state["last_connect_ts"],
                    "last_event_ts": state["last_event_ts"],
                },
                "engine": {
                    "status": state["engine"],
                    "snapshots_applied": state["snapshots_applied"],
                    "parse_hits": state["parse_hits"],
                    "parse_misses": state["parse_misses"],
                    "invariants_ok": state["invariants_ok"],
                    "last_invariant_violation": state["last_invariant_violation"],
                    "book_version": BOOK.version,
                    "book_last_update_ts": BOOK.last_update_ts,
                    "best_bid": [str(bb[0]), str(bb[1])] if bb else None,
                    "best_ask": [str(ba[0]), str(ba[1])] if ba else None,
                    "depth_counts": BOOK.depth_counts(),
                    "totals": BOOK.totals(),
                },
                "tape": {
                    "trades_seen": state["trades_seen"],
                    "last_trade_px": state["last_trade_px"],
                    "last_trade_side": state["last_trade_side"],
                    "last_trade_qty": state["last_trade_qty"],
                    "last_trade_ts": state["last_trade_ts"],
                },
            }
        )


@app.get("/book.json")
def book_json() -> JSONResponse:
    bb = BOOK.best_bid()
    ba = BOOK.best_ask()
    return JSONResponse(
        {
            "service": SERVICE,
            "build": BUILD,
            "symbol": SYMBOL,
            "health": _health(),
            "book": {
                "best_bid": [str(bb[0]), str(bb[1])] if bb else None,
                "best_ask": [str(ba[0]), str(ba[1])] if ba else None,
                "depth_counts": BOOK.depth_counts(),
                "totals": BOOK.totals(),
                "version": BOOK.version,
                "last_update_ts": BOOK.last_update_ts,
                "top": BOOK.top_n(TOP_N),
            },
        }
    )


@app.get("/raw.json")
def raw_json() -> JSONResponse:
    return JSONResponse(
        {
            "service": SERVICE,
            "build": BUILD,
            "symbol": SYMBOL,
            "raw_ring": RAW_RING.snapshot(),
        }
    )


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
