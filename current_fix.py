"""
QuantDesk Bookmap Service — current_fix.py
Build: FIX05/P01
Subsystem: FOUNDATION + STREAM + STATE_ENGINE
Date: 2026-01-24

FIX05 goal (PHASE B entry): Deterministic CLOB State Engine Foundation
- Preserve FIX04: server up, connector CONNECTED, frames flowing.
- Add a single-writer state engine that consumes WS messages via an internal queue.
- Implement best-effort parsing of depth messages into aggregated price levels.
- Enforce invariants and expose state via /book.json.
- Do NOT claim exact MEXC depth semantics unless present in message fields.

Truth/limits:
- MEXC message schemas can vary by market (spot vs contract) and API version.
- This FIX applies book updates only when payloads clearly contain bids/asks arrays.
- When schema is unknown, we record raw messages and keep health at YELLOW (warming),
  rather than fabricating book state.

Required endpoints:
- / (status page)
- /health.json
- /telemetry.json
Additional in FIX05:
- /book.json (top-of-book + top-N levels + invariants)
- /raw.json (last received WS message sample)
"""

from __future__ import annotations

import asyncio
import json
import os
import queue
import random
import threading
import time
from dataclasses import dataclass, asdict, field
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

# -----------------------------
# Config
# -----------------------------
SERVICE_NAME = "qd-bookmap"
BUILD = "FIX05/P01"
SUBSYSTEM = "FOUNDATION+STREAM+STATE_ENGINE"

# FIX04 validated MEXC Spot WS base (resolvable host)
DEFAULT_WS_URL = "wss://wbs.mexc.com/ws"
WS_URL = os.environ.get("QD_MEXC_WS_URL", DEFAULT_WS_URL).strip()

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", os.environ.get("QD_PORT", "5000")))

# Internal pipeline sizing
MSG_QUEUE_MAX = int(os.environ.get("QD_MSG_QUEUE_MAX", "5000"))
RAW_RING_MAX = int(os.environ.get("QD_RAW_RING_MAX", "20"))
BOOK_TOP_N = int(os.environ.get("QD_BOOK_TOP_N", "20"))

DEFAULT_STREAMS = [
    # These were already flowing under FIX04 in your environment.
    # Override: env QD_MEXC_STREAMS as JSON list or comma-separated.
    "spot@public.deals.v3.api@BTCUSDT",
    "spot@public.depth.v3.api@BTCUSDT@20",
]

START_TS = time.time()


def _now_ts() -> float:
    return time.time()


def _safe_exc(e: BaseException) -> str:
    s = f"{type(e).__name__}: {str(e)}".strip()
    return s[:700]


def _load_streams() -> List[str]:
    raw = os.environ.get("QD_MEXC_STREAMS", "").strip()
    if not raw:
        return DEFAULT_STREAMS[:]
    try:
        v = json.loads(raw)
        if isinstance(v, list) and all(isinstance(x, str) for x in v):
            out = [x.strip() for x in v if x.strip()]
            return out or DEFAULT_STREAMS[:]
    except Exception:
        pass
    parts = [p.strip() for p in raw.split(",")]
    out = [p for p in parts if p]
    return out or DEFAULT_STREAMS[:]


STREAMS = _load_streams()


# -----------------------------
# Dependency probes (do not crash)
# -----------------------------
def _try_imports() -> Tuple[Optional[Any], Optional[Any], Optional[Any], List[str]]:
    errors: List[str] = []
    fastapi_mod = None
    responses_mod = None
    websockets_mod = None

    try:
        from fastapi import FastAPI  # type: ignore
        from fastapi import responses as _responses  # type: ignore
        fastapi_mod = FastAPI
        responses_mod = _responses
    except Exception as e:
        errors.append(f"fastapi import failed: {_safe_exc(e)}")

    try:
        import websockets  # type: ignore
        websockets_mod = websockets
    except Exception as e:
        errors.append(f"websockets import failed: {_safe_exc(e)}")

    try:
        import uvicorn  # type: ignore
        _ = uvicorn
    except Exception as e:
        errors.append(f"uvicorn import failed: {_safe_exc(e)}")

    return fastapi_mod, responses_mod, websockets_mod, errors


FastAPI_cls, fastapi_responses, websockets, IMPORT_ERRORS = _try_imports()


# -----------------------------
# Ring buffer (thread-safe)
# -----------------------------
class Ring:
    def __init__(self, maxlen: int) -> None:
        self.maxlen = maxlen
        self._buf: List[Any] = []
        self._lock = threading.Lock()

    def push(self, item: Any) -> None:
        with self._lock:
            self._buf.append(item)
            if len(self._buf) > self.maxlen:
                self._buf = self._buf[-self.maxlen :]

    def snapshot(self) -> List[Any]:
        with self._lock:
            return list(self._buf)


RAW_RING = Ring(RAW_RING_MAX)

# -----------------------------
# Connector state / Telemetry
# -----------------------------
@dataclass
class ConnectorState:
    status: str = "DISCONNECTED"  # DISCONNECTED | CONNECTED | ERROR
    frames_total: int = 0
    last_event_ts: float = 0.0
    last_ack_ts: float = 0.0
    reconnects: int = 0

    ws_url: str = WS_URL
    streams: List[str] = field(default_factory=list)

    last_error: str = ""
    last_close_code: Optional[int] = None
    last_close_reason: str = ""
    last_connect_ts: float = 0.0

    import_errors: List[str] = field(default_factory=list)

    def to_public(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class StateEngineTelemetry:
    status: str = "WARMING"  # WARMING | ACTIVE | ERROR
    updates_applied: int = 0
    snapshots_applied: int = 0
    last_apply_ts: float = 0.0
    last_error: str = ""
    parse_hits: int = 0
    parse_misses: int = 0
    invariants_ok: bool = True
    last_invariant_violation: str = ""
    last_seq: Optional[int] = None  # only if message provides it
    last_msg_kind: str = ""         # snapshot|delta|trade|unknown

    # queue health
    qsize: int = 0
    dropped: int = 0


STATE = ConnectorState(streams=STREAMS[:], import_errors=IMPORT_ERRORS[:])
ENGINE = StateEngineTelemetry()

STATE_LOCK = threading.Lock()
ENGINE_LOCK = threading.Lock()

MSG_Q: "queue.Queue[str]" = queue.Queue(maxsize=MSG_QUEUE_MAX)


def _mexc_subscribe_payload(streams: List[str]) -> str:
    payload = {
        "method": "SUBSCRIPTION",
        "params": [s for s in streams if s.strip()],
        "id": int(_now_ts() * 1000) % 1_000_000_000,
    }
    return json.dumps(payload, separators=(",", ":"))


# -----------------------------
# Deterministic Book State (aggregated levels)
# -----------------------------
class Book:
    """
    Aggregated price levels only (not full order-by-order).
    Uses Decimal for correctness; this is sufficient for Bookmap-style level heatmaps.
    Single-writer: only state-engine thread mutates this object.
    """
    def __init__(self) -> None:
        self.bids: Dict[Decimal, Decimal] = {}
        self.asks: Dict[Decimal, Decimal] = {}
        self.last_update_ts: float = 0.0
        self.last_msg_kind: str = ""
        self.last_seq: Optional[int] = None

    def clear(self) -> None:
        self.bids.clear()
        self.asks.clear()

    def _set_side(self, side: Dict[Decimal, Decimal], levels: List[Tuple[Decimal, Decimal]]) -> None:
        # Snapshot: replace side completely with provided levels.
        side.clear()
        for p, q in levels:
            if q <= 0:
                continue
            side[p] = q

    def _apply_deltas(self, side: Dict[Decimal, Decimal], levels: List[Tuple[Decimal, Decimal]]) -> None:
        # Delta: q==0 removes; q>0 sets level qty.
        for p, q in levels:
            if q <= 0:
                side.pop(p, None)
            else:
                side[p] = q

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

    def top_n(self, n: int) -> Dict[str, List[List[str]]]:
        # Return as strings for JSON stability
        bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]
        asks = sorted(self.asks.items(), key=lambda x: x[0])[:n]
        return {
            "bids": [[str(p), str(q)] for p, q in bids],
            "asks": [[str(p), str(q)] for p, q in asks],
        }


BOOK = Book()


def _to_decimal(x: Any) -> Optional[Decimal]:
    """
    Convert numeric-like to Decimal reliably. Returns None if not convertible.
    """
    if x is None:
        return None
    try:
        if isinstance(x, Decimal):
            return x
        if isinstance(x, (int, float)):
            # float -> string to avoid binary artifacts in repr
            return Decimal(str(x))
        if isinstance(x, str):
            s = x.strip()
            if not s:
                return None
            return Decimal(s)
    except (InvalidOperation, ValueError):
        return None
    return None


def _extract_levels(obj: Any) -> Optional[List[Tuple[Decimal, Decimal]]]:
    """
    Accept levels like [[price, qty], ...] or [{"p":..., "q":...}, ...].
    Returns None if unrecognized.
    """
    if not isinstance(obj, list):
        return None
    out: List[Tuple[Decimal, Decimal]] = []

    for item in obj:
        p: Optional[Decimal] = None
        q: Optional[Decimal] = None

        if isinstance(item, list) and len(item) >= 2:
            p = _to_decimal(item[0])
            q = _to_decimal(item[1])
        elif isinstance(item, dict):
            # common key variants
            for pk in ("p", "price", "px"):
                if pk in item:
                    p = _to_decimal(item.get(pk))
                    break
            for qk in ("q", "qty", "quantity", "sz", "size", "vol", "volume"):
                if qk in item:
                    q = _to_decimal(item.get(qk))
                    break

        if p is None or q is None:
            return None  # schema mismatch → treat entire extraction as miss
        out.append((p, q))

    return out


def _find_book_payload(payload: Dict[str, Any]) -> Tuple[Optional[Any], Optional[Any], str, Optional[int]]:
    """
    Best-effort search for bids/asks arrays and optional sequence.
    Returns (bids_obj, asks_obj, kind, seq)

    kind is one of:
    - "snapshot" (full book snapshot)
    - "delta"    (incremental update)
    - "unknown"  (not a recognizable book payload)
    """
    seq: Optional[int] = None

    # Sequence candidates (only used if present)
    for sk in ("seq", "sequence", "u", "U", "lastUpdateId", "updateId"):
        v = payload.get(sk)
        if isinstance(v, int):
            seq = v
            break

    # Common nesting patterns: payload["data"] may contain book arrays
    candidates: List[Dict[str, Any]] = []
    if isinstance(payload, dict):
        candidates.append(payload)
        d = payload.get("data")
        if isinstance(d, dict):
            candidates.append(d)

    # Try known keys for bids/asks
    bid_keys = ("bids", "bid", "b", "buys")
    ask_keys = ("asks", "ask", "a", "sells")

    bids_obj = None
    asks_obj = None

    for c in candidates:
        for bk in bid_keys:
            if bk in c and isinstance(c[bk], list):
                bids_obj = c[bk]
                break
        for ak in ask_keys:
            if ak in c and isinstance(c[ak], list):
                asks_obj = c[ak]
                break
        if bids_obj is not None and asks_obj is not None:
            break

    if bids_obj is None or asks_obj is None:
        return None, None, "unknown", seq

    # Determine snapshot vs delta:
    # If payload explicitly says snapshot, treat as snapshot. Otherwise default:
    # - If book empty OR keys look like "snapshot"/"full" OR contains "version"/"checksum" → snapshot
    # - Else delta
    kind = "delta"
    for key in ("type", "action", "e", "event", "method"):
        v = payload.get(key)
        if isinstance(v, str) and v.lower() in ("snapshot", "full", "depthsnapshot"):
            kind = "snapshot"
            break

    if kind != "snapshot":
        # heuristic: if payload includes "snapshot" booleans or the book is empty, treat as snapshot
        if payload.get("snapshot") is True or (not BOOK.bids and not BOOK.asks):
            kind = "snapshot"

    return bids_obj, asks_obj, kind, seq


def _check_invariants() -> Tuple[bool, str]:
    """
    Invariants:
    - No negative quantities (enforced by insert logic)
    - Book not crossed: best_bid < best_ask (if both exist)
    """
    bb = BOOK.best_bid()
    ba = BOOK.best_ask()
    if bb and ba:
        if bb[0] >= ba[0]:
            return False, f"crossed_book: best_bid={bb[0]} best_ask={ba[0]}"
    return True, ""


# -----------------------------
# State engine worker (single-writer)
# -----------------------------
def _engine_loop() -> None:
    while True:
        try:
            raw = MSG_Q.get()  # blocking
            with ENGINE_LOCK:
                ENGINE.qsize = MSG_Q.qsize()

            RAW_RING.push({"ts": _now_ts(), "raw": raw[:4000]})

            try:
                payload = json.loads(raw)
                if not isinstance(payload, dict):
                    raise ValueError("payload_not_dict")
            except Exception as e:
                with ENGINE_LOCK:
                    ENGINE.parse_misses += 1
                    ENGINE.last_error = f"json_parse_failed: {_safe_exc(e)}"
                    ENGINE.status = "WARMING"
                    ENGINE.last_apply_ts = _now_ts()
                continue

            bids_obj, asks_obj, kind, seq = _find_book_payload(payload)
            if bids_obj is None or asks_obj is None:
                # Not a book message — could be trade/ack/etc.
                with ENGINE_LOCK:
                    ENGINE.parse_misses += 1
                    ENGINE.last_msg_kind = "unknown"
                    ENGINE.status = "WARMING" if ENGINE.updates_applied == 0 else ENGINE.status
                    ENGINE.last_apply_ts = _now_ts()
                continue

            bids_lv = _extract_levels(bids_obj)
            asks_lv = _extract_levels(asks_obj)
            if bids_lv is None or asks_lv is None:
                with ENGINE_LOCK:
                    ENGINE.parse_misses += 1
                    ENGINE.last_msg_kind = "unknown"
                    ENGINE.status = "WARMING" if ENGINE.updates_applied == 0 else ENGINE.status
                    ENGINE.last_apply_ts = _now_ts()
                continue

            # Apply deterministically
            if kind == "snapshot":
                BOOK._set_side(BOOK.bids, bids_lv)
                BOOK._set_side(BOOK.asks, asks_lv)
                with ENGINE_LOCK:
                    ENGINE.snapshots_applied += 1
            else:
                BOOK._apply_deltas(BOOK.bids, bids_lv)
                BOOK._apply_deltas(BOOK.asks, asks_lv)

            BOOK.last_update_ts = _now_ts()
            BOOK.last_msg_kind = kind
            BOOK.last_seq = seq

            ok, viol = _check_invariants()
            with ENGINE_LOCK:
                ENGINE.parse_hits += 1
                ENGINE.updates_applied += 1
                ENGINE.last_apply_ts = _now_ts()
                ENGINE.last_msg_kind = kind
                ENGINE.last_seq = seq
                ENGINE.invariants_ok = ok
                ENGINE.last_invariant_violation = viol
                if not ok:
                    ENGINE.status = "ERROR"
                    ENGINE.last_error = viol
                else:
                    ENGINE.status = "ACTIVE" if ENGINE.updates_applied > 0 else "WARMING"
                    if ENGINE.status == "ACTIVE":
                        ENGINE.last_error = ""
        except Exception as e:
            # Engine should not die silently; mark error and keep looping.
            with ENGINE_LOCK:
                ENGINE.status = "ERROR"
                ENGINE.last_error = f"engine_loop_exception: {_safe_exc(e)}"
            time.sleep(0.2)


def _start_engine_thread() -> None:
    t = threading.Thread(target=_engine_loop, name="qd_state_engine", daemon=True)
    t.start()


# -----------------------------
# WS loop (producer)
# -----------------------------
async def _ws_session(ws_url: str, streams: List[str]) -> None:
    assert websockets is not None

    async with websockets.connect(
        ws_url,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=10,
        max_queue=64,
    ) as ws:
        with STATE_LOCK:
            STATE.status = "CONNECTED"
            STATE.last_error = ""
            STATE.last_close_code = None
            STATE.last_close_reason = ""
            STATE.last_connect_ts = _now_ts()

        await ws.send(_mexc_subscribe_payload(streams))

        while True:
            msg = await ws.recv()
            ts = _now_ts()
            with STATE_LOCK:
                STATE.frames_total += 1
                STATE.last_event_ts = ts

            # enqueue for engine (drop oldest under pressure by dropping new)
            try:
                if isinstance(msg, bytes):
                    msg_s = msg.decode("utf-8", errors="replace")
                else:
                    msg_s = str(msg)
                MSG_Q.put_nowait(msg_s)
            except queue.Full:
                with ENGINE_LOCK:
                    ENGINE.dropped += 1

            # best-effort ack signal
            if isinstance(msg, str) and (
                '"code"' in msg or '"success"' in msg or '"ack"' in msg
            ):
                with STATE_LOCK:
                    STATE.last_ack_ts = ts


async def _ws_reconnect_forever() -> None:
    if websockets is None:
        with STATE_LOCK:
            STATE.status = "ERROR"
            STATE.last_error = "Missing dependency: websockets (see import_errors)"
        return

    backoff = 0.5
    backoff_max = 20.0

    while True:
        try:
            await _ws_session(STATE.ws_url, STATE.streams or [])
            backoff = 0.5
        except asyncio.CancelledError:
            raise
        except Exception as e:
            close_code = getattr(e, "code", None)
            close_reason = getattr(e, "reason", "")
            with STATE_LOCK:
                STATE.status = "ERROR"
                STATE.last_error = _safe_exc(e)
                STATE.last_close_code = int(close_code) if isinstance(close_code, int) else None
                STATE.last_close_reason = str(close_reason)[:300]
                STATE.reconnects += 1

            jitter = random.uniform(0.0, 0.25)
            await asyncio.sleep(min(backoff + jitter, backoff_max))
            backoff = min(backoff * 1.7, backoff_max)


def _start_ws_thread() -> None:
    def runner() -> None:
        try:
            asyncio.run(_ws_reconnect_forever())
        except Exception as e:
            with STATE_LOCK:
                STATE.status = "ERROR"
                STATE.last_error = f"WS thread crashed: {_safe_exc(e)}"

    t = threading.Thread(target=runner, name="qd_ws_thread", daemon=True)
    t.start()


# -----------------------------
# Health logic
# -----------------------------
def _connector_public() -> Dict[str, Any]:
    with STATE_LOCK:
        return STATE.to_public()


def _engine_public() -> Dict[str, Any]:
    with ENGINE_LOCK:
        return asdict(ENGINE)


def _health_payload() -> Dict[str, Any]:
    conn = _connector_public()
    eng = _engine_public()

    uptime = _now_ts() - START_TS
    last_event = conn.get("last_event_ts") or 0.0
    fresh_stream_s = _now_ts() - last_event if last_event else 1e9

    last_apply = eng.get("last_apply_ts") or 0.0
    fresh_engine_s = _now_ts() - last_apply if last_apply else 1e9

    import_errors = conn.get("import_errors") or []

    # Health rules (truth-first):
    # GREEN requires:
    # - connector CONNECTED and fresh stream
    # - engine ACTIVE and invariants_ok
    # YELLOW:
    # - server OK, connector OK but engine warming OR schema not yet recognized
    # RED:
    # - connector error OR engine error OR import errors
    if import_errors:
        health = "RED"
    elif conn["status"] == "ERROR":
        health = "RED"
    elif eng["status"] == "ERROR" or not bool(eng.get("invariants_ok", True)):
        health = "RED"
    else:
        if conn["status"] == "CONNECTED" and fresh_stream_s < 3.0:
            if eng["status"] == "ACTIVE" and fresh_engine_s < 3.0 and eng.get("updates_applied", 0) > 0:
                health = "GREEN"
            else:
                health = "YELLOW"
        else:
            health = "YELLOW" if fresh_stream_s < 30.0 else "RED"

    return {
        "ts": _now_ts(),
        "service": SERVICE_NAME,
        "build": BUILD,
        "subsystem": SUBSYSTEM,
        "health": health,
        "uptime_s": round(uptime, 3),
        "connector": conn,
        "engine": eng,
        "freshness_stream_s": round(fresh_stream_s, 3) if fresh_stream_s < 1e8 else None,
        "freshness_engine_s": round(fresh_engine_s, 3) if fresh_engine_s < 1e8 else None,
        "host": HOST,
        "port": PORT,
    }


def _book_payload() -> Dict[str, Any]:
    eng = _engine_public()
    bb = BOOK.best_bid()
    ba = BOOK.best_ask()

    spread = None
    if bb and ba:
        spread = str(ba[0] - bb[0])

    ok, viol = _check_invariants()

    return {
        "ts": _now_ts(),
        "build": BUILD,
        "engine_status": eng.get("status"),
        "updates_applied": eng.get("updates_applied"),
        "snapshots_applied": eng.get("snapshots_applied"),
        "parse_hits": eng.get("parse_hits"),
        "parse_misses": eng.get("parse_misses"),
        "dropped": eng.get("dropped"),
        "book": {
            "best_bid": [str(bb[0]), str(bb[1])] if bb else None,
            "best_ask": [str(ba[0]), str(ba[1])] if ba else None,
            "spread": spread,
            "levels": BOOK.top_n(BOOK_TOP_N),
            "counts": {"bids": len(BOOK.bids), "asks": len(BOOK.asks)},
            "last_update_ts": BOOK.last_update_ts or None,
            "last_msg_kind": BOOK.last_msg_kind,
            "last_seq": BOOK.last_seq,
        },
        "invariants": {
            "ok": ok,
            "violation": viol,
            "engine_violation": eng.get("last_invariant_violation", ""),
        },
        "limits": {
            "top_n": BOOK_TOP_N,
            "msg_queue_max": MSG_QUEUE_MAX,
            "raw_ring_max": RAW_RING_MAX,
        },
    }


# -----------------------------
# FastAPI path (preferred)
# -----------------------------
def _run_fastapi() -> None:
    assert FastAPI_cls is not None
    assert fastapi_responses is not None

    HTMLResponse = fastapi_responses.HTMLResponse
    JSONResponse = fastapi_responses.JSONResponse
    PlainTextResponse = fastapi_responses.PlainTextResponse

    app = FastAPI_cls(title="QuantDesk Bookmap Service", version=BUILD)

    @app.on_event("startup")
    async def _startup() -> None:
        _start_engine_thread()
        _start_ws_thread()

    @app.get("/health.json")
    async def health_json() -> Any:
        return JSONResponse(_health_payload())

    @app.get("/telemetry.json")
    async def telemetry_json() -> Any:
        return JSONResponse(_health_payload())

    @app.get("/book.json")
    async def book_json() -> Any:
        return JSONResponse(_book_payload())

    @app.get("/raw.json")
    async def raw_json() -> Any:
        return JSONResponse({"ts": _now_ts(), "build": BUILD, "raw_ring": RAW_RING.snapshot()})

    @app.get("/", response_class=HTMLResponse)
    async def status_page() -> Any:
        h = _health_payload()
        conn = h["connector"]
        eng = h["engine"]
        health = h["health"]
        health_color = {"GREEN": "#16a34a", "YELLOW": "#f59e0b", "RED": "#dc2626"}.get(
            health, "#6b7280"
        )

        def esc(s: Any) -> str:
            return (
                str(s)
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
            )

        bb = BOOK.best_bid()
        ba = BOOK.best_ask()
        bb_s = f"{bb[0]} x {bb[1]}" if bb else "—"
        ba_s = f"{ba[0]} x {ba[1]}" if ba else "—"
        ok, viol = _check_invariants()

        html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>QuantDesk Bookmap — Status</title>
  <style>
    body {{ font-family: -apple-system, system-ui, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 24px; }}
    .row {{ display: grid; grid-template-columns: 1fr; gap: 12px; max-width: 980px; }}
    .card {{ border: 1px solid #e5e7eb; border-radius: 14px; padding: 14px 16px; }}
    .h {{ display: flex; align-items: center; gap: 12px; flex-wrap:wrap; }}
    .pill {{ padding: 6px 10px; border-radius: 999px; color: white; font-weight: 700; background: {health_color}; }}
    .muted {{ color: #6b7280; }}
    .kv {{ display: grid; grid-template-columns: 190px 1fr; gap: 6px 12px; }}
    code {{ background: #f3f4f6; padding: 2px 6px; border-radius: 8px; }}
    a {{ color: #2563eb; text-decoration: none; }}
  </style>
</head>
<body>
  <div class="row">
    <div class="h">
      <h2 style="margin:0;">QuantDesk Bookmap — Status</h2>
      <span class="pill">HEALTH: {esc(health)}</span>
      <span class="muted">Service: {esc(SERVICE_NAME)} · Subsystem: {esc(SUBSYSTEM)} · Build: {esc(BUILD)} · Date: 2026-01-24</span>
    </div>

    <div class="card">
      <div style="display:flex; gap:16px; flex-wrap:wrap;">
        <a href="/health.json">health.json</a>
        <a href="/telemetry.json">telemetry.json</a>
        <a href="/book.json">book.json</a>
        <a href="/raw.json">raw.json</a>
      </div>
    </div>

    <div class="card">
      <div class="kv">
        <div class="muted">Server</div><div><code>{esc(h["host"])}:{esc(h["port"])}</code> · Uptime: <code>{esc(round(h["uptime_s"],1))}s</code></div>

        <div class="muted">Connector</div><div><b>{esc(conn.get("status",""))}</b> · Frames: <code>{esc(conn.get("frames_total",0))}</code> · Reconnects: <code>{esc(conn.get("reconnects",0))}</code></div>
        <div class="muted">WS URL</div><div><code>{esc(conn.get("ws_url",""))}</code></div>
        <div class="muted">Streams</div><div><code>{esc(", ".join(conn.get("streams",[])[:3]))}{esc("..." if len(conn.get("streams",[]))>3 else "")}</code></div>
        <div class="muted">Last error</div><div><code>{esc(conn.get("last_error","") or "—")}</code></div>

        <div class="muted">State engine</div><div><b>{esc(eng.get("status",""))}</b> · Applied: <code>{esc(eng.get("updates_applied",0))}</code> · Snapshots: <code>{esc(eng.get("snapshots_applied",0))}</code></div>
        <div class="muted">Parse hits/misses</div><div><code>{esc(eng.get("parse_hits",0))}</code> / <code>{esc(eng.get("parse_misses",0))}</code> · Dropped: <code>{esc(eng.get("dropped",0))}</code></div>
        <div class="muted">Best bid / ask</div><div><code>{esc(bb_s)}</code> / <code>{esc(ba_s)}</code></div>
        <div class="muted">Invariants</div><div><b>{esc("OK" if ok else "VIOLATION")}</b> <code>{esc(viol or eng.get("last_invariant_violation","") or "—")}</code></div>
        <div class="muted">Engine error</div><div><code>{esc(eng.get("last_error","") or "—")}</code></div>
      </div>
    </div>

    <div class="card">
      <div class="muted">Notes</div>
      <div>
        FIX05 introduces the deterministic state engine foundation. If engine remains <b>WARMING</b>, it means depth messages are flowing but schema is not yet recognized as bids/asks arrays.<br/>
        In that case, open <code>/raw.json</code> and provide one sample message so the next FIX can implement an exchange-accurate parser without guessing.
      </div>
    </div>
  </div>
</body>
</html>
"""
        return HTMLResponse(html)

    @app.get("/robots.txt")
    async def robots() -> Any:
        return PlainTextResponse("User-agent: *\nDisallow: /\n")

    import uvicorn  # type: ignore
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")


# -----------------------------
# Fallback HTTP server (keeps Replit alive)
# -----------------------------
def _run_fallback_http() -> None:
    from http.server import BaseHTTPRequestHandler, HTTPServer

    class Handler(BaseHTTPRequestHandler):
        def _send(self, status: int, body: str, ctype: str = "text/plain; charset=utf-8") -> None:
            b = body.encode("utf-8", errors="replace")
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)

        def do_GET(self) -> None:  # noqa: N802
            if self.path in ("/health.json", "/telemetry.json"):
                self._send(200, json.dumps(_health_payload(), indent=2), "application/json; charset=utf-8")
                return
            if self.path == "/book.json":
                self._send(200, json.dumps(_book_payload(), indent=2), "application/json; charset=utf-8")
                return
            if self.path == "/raw.json":
                self._send(200, json.dumps({"ts": _now_ts(), "build": BUILD, "raw_ring": RAW_RING.snapshot()}, indent=2),
                           "application/json; charset=utf-8")
                return

            if self.path == "/" or self.path.startswith("/?"):
                payload = _health_payload()
                errs = payload["connector"].get("import_errors") or []
                msg = [
                    "QuantDesk Bookmap — FALLBACK SERVER (FastAPI/Uvicorn not available)",
                    f"Build: {BUILD}",
                    f"Bind: {HOST}:{PORT}",
                    "",
                    "Import errors:",
                    *(f"- {e}" for e in errs),
                    "",
                    "Endpoints:",
                    "- /health.json",
                    "- /telemetry.json",
                    "- /book.json",
                    "- /raw.json",
                ]
                self._send(200, "\n".join(msg))
                return

            self._send(404, "Not found")

        def log_message(self, format: str, *args: Any) -> None:
            return

    httpd = HTTPServer((HOST, PORT), Handler)
    _start_engine_thread()
    _start_ws_thread()
    httpd.serve_forever()


def main() -> None:
    can_fastapi = FastAPI_cls is not None and fastapi_responses is not None
    has_uvicorn_error = any(e.startswith("uvicorn import failed") for e in IMPORT_ERRORS)
    has_fastapi_error = any(e.startswith("fastapi import failed") for e in IMPORT_ERRORS)

    if can_fastapi and not has_uvicorn_error and not has_fastapi_error:
        _run_fastapi()
    else:
        with STATE_LOCK:
            STATE.status = "ERROR"
            STATE.last_error = "Foundation fallback active (missing FastAPI/Uvicorn). See import_errors."
        _run_fallback_http()


if __name__ == "__main__":
    main()
