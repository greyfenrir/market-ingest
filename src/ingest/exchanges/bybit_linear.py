"""Bybit v5 linear collector: USDT and USDC perpetuals and futures.

Everything lives on one public route, wss://stream.bybit.com/v5/public/linear,
and is subscribed by message rather than in the URL. Two things are easy to
miss and both end in a connection that looks fine and is not:

- Bybit wants a JSON ping of its own every 20 s; without it the connection is
  cut after a few minutes, WebSocket pings or not;
- from 2026-11-26 a linear connection takes at most 100 topics. A
  subscription beyond that fails in a reply nobody reads. The collector
  refuses to start with more rather than collect a silent subset.

Streams keep Binance's names (agg_trade, mark_price, force_order, depth,
open_interest), so ops tables and alarms read the same for every venue:

    agg_trade      publicTrade.<sym>      every trade, id is a UUID
    mark_price     tickers.<sym>          deltas carry only what changed; a timer
                                          writes one row a second per symbol from
                                          the merged state
    force_order    allLiquidation.<sym>   every liquidation, not a sample
    depth          orderbook.1000.<sym>   snapshot + deltas on the socket
    open_interest  GET /v5/market/tickers once a minute: OI for the focus
                   symbols (one side, singleOpenInterest - openInterest
                   counts both), and the sampled mark price of everything else
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

from ..health import StreamStats, ch_ts, now_ms
from ..metrics import Statsd
from ..rest import RateLimited, RestClient
from .base import Collector, extra_fields as _extra
from .bybit_linear_depth import BybitBook

log = logging.getLogger(__name__)

WS = "wss://stream.bybit.com/v5/public/linear"
REST = "https://api.bybit.com"
EXCHANGE = "bybit_linear"
MAX_TOPICS = 100
PING = (20.0, json.dumps({"op": "ping"}))
# Bybit's own limit is 10 args per message on spot only; small messages are
# also kinder to the reply, which reports success for the whole message.
SUBSCRIBE_CHUNK = 10

# A focus symbol whose ticker has been silent this long gets no mark rows: the
# gap is then in the archive instead of a state nobody has confirmed.
MARK_MAX_AGE_MS = 60_000
# Rows are cut this long after the second ends, so that messages stamped inside
# it have arrived.
MARK_SETTLE_S = 0.05

KNOWN_TRADE = {"T", "s", "S", "v", "p", "i", "seq"}
# S stays in extra: it is Bybit's position side, kept as sent; `side` holds
# the liquidation order's side, as Binance reports it.
KNOWN_LIQ = {"T", "s", "v", "p"}


def _f(x) -> float:
    """Bybit sends numbers as strings, and "" for a field that does not apply
    (funding on a dated future)."""
    return float(x) if x not in (None, "") else 0.0


def subscribe_messages(stream: str, topics: list[str], op: str = "subscribe") -> list[str]:
    return [json.dumps({"op": op, "req_id": stream, "args": topics[i:i + SUBSCRIBE_CHUNK]})
            for i in range(0, len(topics), SUBSCRIBE_CHUNK)]


class BybitLinear(Collector):
    EXCHANGE = EXCHANGE
    WS_STREAMS = ("mark_price", "agg_trade", "force_order")

    def __init__(self, cfg: dict, sink, *, focus: list[str], sd: Statsd) -> None:
        super().__init__(cfg, sink, focus=focus, sd=sd)
        # 600 requests per 5 s per IP; this collector needs a few a minute.
        # A breach is a 403 and a ban of at least 10 minutes, so that is the
        # pause when no Retry-After says otherwise.
        self.rest = RestClient(REST, budget=int(cfg.get("rest_requests_per_5s", 300)),
                               window_s=5.0, limited_statuses=(403, 429),
                               default_pause_s=600.0, on_event=self._event)
        # Ticker state per focus symbol: deltas carry only the fields that
        # changed, so every value is read from the merged state.
        self._tickers: dict[str, dict] = {}
        self._ticker_ts: dict[str, float] = {}   # exchange time of the last message

        dcfg = cfg.get("depth", {})
        syms = dcfg.get("symbols", "focus")
        syms = self.focus if syms == "focus" else {s.upper() for s in syms}
        self.depth_levels = int(dcfg.get("levels", 1000))
        # A book without a snapshot this long after (re)subscribing is
        # subscribed again.
        self.depth_resubscribe_s = float(dcfg.get("resubscribe_after_seconds", 10.0))
        if dcfg.get("enabled", False):
            self._init_books({s: BybitBook(s, self.depth_levels) for s in sorted(syms)})
        self._depth_open = False

        for stream, n in (("agg_trade", len(self.focus)), ("depth", len(self.depth))):
            if n > MAX_TOPICS:
                raise ValueError(f"{stream}: {n} topics, Bybit takes {MAX_TOPICS} per connection")

    # ------------------------------------------------------------ ws plumbing

    def _unwrap(self, payload):
        """Replies to subscribe and ping carry `op`; data carries `topic`."""
        if isinstance(payload, dict) and "op" in payload:
            if not payload.get("success", True):
                stream = payload.get("req_id") or "ws"
                detail = f"{payload['op']} failed: {payload.get('ret_msg', '')}"
                self._event(stream, "error", detail)
                log.error("%s %s", stream, detail)
            return None
        return payload

    def _stream(self, stream: str, topic: str, symbols, handle, **kw):
        topics = [f"{topic}.{s}" for s in sorted(symbols)]
        return self._ws_loop(stream, WS, handle, subscribe=subscribe_messages(stream, topics),
                             app_ping=PING, **kw)

    # -------------------------------------------------------------- handlers

    def _handle_trade(self, payload, recv_ms: float, nbytes: int, st: StreamStats) -> None:
        lag = None
        good = []
        for t in payload.get("data") or []:
            # One bad trade costs only itself, not the rest of the batch:
            # Bybit has no contiguous ids to backfill a lost tail by.
            try:
                row = self._trade_row(t, recv_ms)
            except Exception as exc:  # noqa: BLE001
                self._bad_message("agg_trade", t, None, exc)
                continue
            self.sink.write("market.trades", row)
            good.append(t)
            lag = int(recv_ms - float(t["T"]))
        trades = good
        if trades:
            st.observe(nbytes=nbytes, rows=len(trades), lag_ms=lag, symbol=trades[-1]["s"])
            self._count_rows("agg_trade", len(trades))
            self.sd.gauge(f"lag_ms.{EXCHANGE}.agg_trade", max(0, lag))
        self.sd.incr(f"bytes.{EXCHANGE}.agg_trade", nbytes)

    @staticmethod
    def _trade_row(t: dict, recv_ms: float) -> dict:
        ev = float(t["T"])
        return {
            "exchange": EXCHANGE,
            "symbol": t["s"],
            "event_time": ch_ts(ev),
            "ingest_time": ch_ts(recv_ms),
            # No contiguous id exists, so loss cannot be proven per trade
            # as on Binance; seq keeps the order, trade_id the identity.
            "agg_id": int(t.get("seq", 0)),
            "trade_id": t.get("i", ""),
            "first_id": 0,
            "last_id": 0,
            "price": float(t["p"]),
            "qty": float(t["v"]),
            # S is the taker's side: a taker sell hits a resting buyer
            "is_buyer_maker": 1 if t.get("S") == "Sell" else 0,
            "extra": _extra(t, KNOWN_TRADE),
        }

    def _handle_ticker(self, payload, recv_ms: float, nbytes: int, st: StreamStats) -> None:
        d = payload.get("data") or {}
        sym = d.get("symbol")
        if not sym:
            return
        if payload.get("type") == "snapshot" or sym not in self._tickers:
            self._tickers[sym] = dict(d)
        else:
            self._tickers[sym].update(d)
        state = self._tickers[sym]
        ts = float(payload.get("ts") or recv_ms)
        self._ticker_ts[sym] = ts
        mark = _f(state.get("markPrice"))
        if mark:
            self._last_mark[sym] = mark
        # No row here: the ticker pushes every 100 ms, mostly for best bid and
        # ask, and leaves the mark out of a delta while it has not moved. The
        # rows are cut by _mark_loop, one per second.
        st.observe(nbytes=nbytes, rows=0, lag_ms=int(recv_ms - ts), symbol=sym)
        self.sd.incr(f"bytes.{EXCHANGE}.mark_price", nbytes)
        self.sd.gauge(f"lag_ms.{EXCHANGE}.mark_price", max(0, int(recv_ms - ts)))

    def _mark_reset(self) -> None:
        """On close and on open: while the socket is down nothing confirms the
        merged state, so the timer writes no rows - a gap in the archive, not
        an old mark passed off as current. The next connection starts with a
        snapshot per symbol."""
        self._tickers.clear()
        self._ticker_ts.clear()

    async def _mark_loop(self) -> None:
        while True:
            await asyncio.sleep(1.0 - time.time() % 1.0 + MARK_SETTLE_S)
            self._mark_pass(int(time.time()), now_ms())

    def _mark_pass(self, second: int, recv_ms: float) -> int:
        """One mark row per focus symbol for the second that just ended, from
        the merged ticker state - the cadence of Binance's markPrice@1s, so an
        unchanged mark is a row and not a gap. When the last message is older
        than a second the row is carried forward, and extra holds the time of
        that message."""
        at = second * 1000.0
        n = 0
        for sym, state in self._tickers.items():
            last = self._ticker_ts.get(sym, 0.0)
            if not _f(state.get("markPrice")) or at - last > MARK_MAX_AGE_MS:
                continue
            row = self._mark_row(state, at, recv_ms, sampled=0)
            if at - last >= 1000:
                row["extra"] = json.dumps({"ts": int(last)}, separators=(",", ":"))
            self.sink.write("market.mark_price", row)
            n += 1
        if n:
            # rows only: observe() would count it as a message and hide a stall
            self._stat("mark_price").rows += n
            self._count_rows("mark_price", n)
        return n

    def _mark_row(self, t: dict, ts: float, recv_ms: float, sampled: int) -> dict:
        return {
            "exchange": EXCHANGE,
            "symbol": t["symbol"],
            "event_time": ch_ts(ts),
            "ingest_time": ch_ts(recv_ms),
            "mark_price": _f(t.get("markPrice")),
            "index_price": _f(t.get("indexPrice")),
            "settle_price": 0.0,            # Bybit publishes no estimated settle price
            "funding_rate": _f(t.get("fundingRate")),
            "next_funding_time": ch_ts(_f(t.get("nextFundingTime"))),
            # 8, 4 or 1 by symbol; "" on a dated future, which pays none
            "funding_interval_h": int(_f(t.get("fundingIntervalHour"))),
            "sampled": sampled,
            # A ticker carries some thirty fields, mostly 24 h statistics that
            # are derivable from trades; none is kept.
            "extra": "",
        }

    def _handle_liquidation(self, payload, recv_ms: float, nbytes: int, st: StreamStats) -> None:
        items = payload.get("data") or []
        for it in items:
            ev = float(it["T"])
            qty = _f(it.get("v"))
            self.sink.write("market.liquidations", {
            "exchange": EXCHANGE,
            "symbol": it["s"],
            "event_time": ch_ts(ev),
            "ingest_time": ch_ts(recv_ms),
                "trade_time": ch_ts(ev),
                # Bybit names the position (Buy = a long was liquidated);
                # Binance names the liquidation order, which sells a long.
                "side": "SELL" if it.get("S") == "Buy" else "BUY",
                "order_type": "",
                "time_in_force": "",
                "order_status": "",
            "price": _f(it.get("p")),   # bankruptcy price
                "avg_price": 0.0,
                "orig_qty": qty,
                "last_filled_qty": qty,
                "cum_filled_qty": qty,
            "extra": _extra(it, KNOWN_LIQ),
            })
            st.observe(nbytes=nbytes, rows=1, lag_ms=int(recv_ms - ev), symbol=it["s"])
        if items:
            self._count_rows("force_order", len(items))
        self.sd.incr(f"bytes.{EXCHANGE}.force_order", nbytes)

    # ----------------------------------------------------------- order book

    def _depth_topic(self, symbol: str) -> str:
        return f"orderbook.{self.depth_levels}.{symbol}"

    def _depth_on_open(self) -> None:
        """A new connection brings a fresh snapshot for every book; until it
        lands, no book continues anything we can vouch for."""
        now = time.monotonic()
        for sync in self.depth.values():
            was_live = sync.live
            sync.reset("reconnect", now)
            if was_live:
                self._depth_resync(sync, "reconnect")
        self._depth_open = True

    def _depth_on_close(self) -> None:
        self._depth_open = False

    def _on_bad_message(self, stream: str, payload) -> None:
        """A depth message that broke halfway may have changed part of a live
        book: drop it and ask for a fresh snapshot. A book still waiting for
        one is left to the depth watchdog, which resubscribes it."""
        if stream != "depth" or not isinstance(payload, dict):
            return
        d = payload.get("data")
        sync = self.depth.get(d.get("s")) if isinstance(d, dict) else None
        if sync is not None and sync.live:
            mono = time.monotonic()
            self._depth_resync(sync, sync.reset("bad message", mono))
            hold = self._repair_hold(sync.symbol, mono)
            if hold == 0:
                self._resubscribe(sync, mono)
            else:
                # The watchdog resubscribes once the hold is over.
                sync.syncing_since_mono = mono + hold - self.depth_resubscribe_s

    def _handle_depth(self, payload, recv_ms: float, nbytes: int, st: StreamStats) -> None:
        d = payload.get("data") or {}
        sync = self.depth.get(d.get("s"))
        if sync is None:
            return
        mono = time.monotonic()
        reason = sync.on_message(payload, mono)
        if reason:
            self._depth_resync(sync, reason)
            self._resubscribe(sync, mono)
        if sync.just_live:
            sync.just_live = False
            self._event("depth", "synced", f"{sync.symbol} u={sync.book.last_update_id}")
        ts = float(payload.get("ts") or recv_ms)
        st.observe(nbytes=nbytes, rows=0, lag_ms=int(recv_ms - ts), symbol=sync.symbol)
        self.sd.incr(f"bytes.{EXCHANGE}.depth", nbytes)

    def _resubscribe(self, sync: BybitBook, mono: float) -> None:
        """Ask for a fresh snapshot of one book: the only repair Bybit offers.
        The other books on the connection are untouched."""
        ws = self._ws.get("depth")
        sync.syncing_since_mono = mono     # the watchdog waits for this attempt
        if ws is None:
            return                         # the next connection subscribes anyway
        topic = [self._depth_topic(sync.symbol)]
        msgs = (subscribe_messages("depth", topic, "unsubscribe")
                + subscribe_messages("depth", topic))
        t = asyncio.create_task(self._send_all(ws, msgs), name=f"resubscribe:{sync.symbol}")
        self._bg.add(t)
        t.add_done_callback(self._bg.discard)

    async def _send_all(self, ws, msgs: list[str]) -> None:
        try:
            for m in msgs:
                await ws.send(m)
        except Exception as exc:  # noqa: BLE001 - the connection is going; it resubscribes all
            log.warning("resubscribe failed: %s", exc)

    async def _depth_watch_loop(self) -> None:
        """Resubscribe a book still without a snapshot: after a sequence break
        or a crossed book, or when a subscribe reply never came."""
        while True:
            await asyncio.sleep(2.0)
            self._depth_watch_pass(time.monotonic())

    def _depth_watch_pass(self, now: float) -> int:
        if not self._depth_open:
            return 0
        n = 0
        for sync in self.depth.values():
            if not sync.live and now - sync.syncing_since_mono > self.depth_resubscribe_s:
                self._event("depth", "resubscribe", f"{sync.symbol}: no snapshot")
                self._resubscribe(sync, now)
                n += 1
        return n

    # ------------------------------------------------------------------ rest

    async def _get(self, path: str, params: dict, *, stream: str, what: str = "") -> dict:
        """GET a v5 endpoint. Bybit reports most errors as retCode with HTTP 200."""
        assert self.rest is not None
        d = await self.rest.get(path, params, stream=stream, what=what)
        code = d.get("retCode")
        if code == 10006:                  # too many visits
            self.rest.pause(stream, what or path, code, {})
            raise RateLimited(f"retCode {code}")
        if code != 0:
            raise RuntimeError(f"retCode {code}: {d.get('retMsg', '')}")
        return d

    async def _tickers_loop(self) -> None:
        """One request a minute covers every linear symbol: open interest for
        the focus symbols (Bybit has no OI stream), and the sampled mark price
        of all the others - the focus ones come from the ticker stream."""
        period = float(self.cfg.get("open_interest_poll_seconds", 60))
        which = self.cfg.get("open_interest_symbols", "focus")
        st = self._stat("open_interest")
        while True:
            t0 = time.monotonic()
            try:
                d = await self._get("/v5/market/tickers", {"category": "linear"},
                                    stream="open_interest", what="tickers")
            except RateLimited:
                d = None
            except Exception as exc:  # noqa: BLE001
                self._event("open_interest", "error", f"tickers: {exc}")
                d = None
            if d is not None:
                self._write_tickers(d, now_ms(), which, st)
            self._roll_health()
            await asyncio.sleep(max(1.0, period - (time.monotonic() - t0)))

    def _write_tickers(self, d: dict, recv_ms: float, which: str, st: StreamStats) -> None:
        ts = float(d.get("time") or recv_ms)
        marks = ois = 0
        for t in (d.get("result") or {}).get("list", []):
            sym = t.get("symbol")
            if not sym:
                continue
            if sym not in self.focus:
                self.sink.write("market.mark_price", self._mark_row(t, ts, recv_ms, sampled=1))
                marks += 1
            if which == "all" or sym in self.focus:
                self.sink.write("market.open_interest", self._oi_row(t, ts, recv_ms))
                ois += 1
        st.observe(nbytes=0, rows=ois, lag_ms=int(recv_ms - ts), symbol=None)
        self._count_rows("mark_price", marks)
        self._count_rows("open_interest", ois)

    @staticmethod
    def _oi_row(t: dict, ts: float, recv_ms: float) -> dict:
        """Open interest of one side, in the base coin, as Binance reports it.
        Bybit's openInterest is "the sum of both sides", exactly twice that:
        taken as is, it would double Bybit's weight in an OI-weighted median.
        singleOpenInterest exists since 2026-06-11. A ticker without it gets
        half the two-sided figure, which is the same number, flagged in extra."""
        single = t.get("singleOpenInterest")
        extra = {k: t[k] for k in ("openInterest", "openInterestValue", "singleOpenInterestValue")
                 if k in t}
        if single in (None, ""):
            oi = _f(t.get("openInterest")) / 2
            extra["derived"] = "openInterest/2"
        else:
            oi = float(single)
        return {
            "exchange": EXCHANGE,
            "symbol": t["symbol"],
            "event_time": ch_ts(ts),
            "ingest_time": ch_ts(recv_ms),
            "open_interest": oi,
            "extra": json.dumps(extra, separators=(",", ":")),
        }

    async def _validate_focus(self) -> None:
        status: dict[str, str] = {}
        cursor = ""
        try:
            while True:
                d = await self._get("/v5/market/instruments-info",
                                    {"category": "linear", "limit": 1000, "cursor": cursor},
                                    stream="config", what="instruments")
                res = d.get("result") or {}
                status.update({i["symbol"]: i.get("status", "") for i in res.get("list", [])})
                cursor = res.get("nextPageCursor") or ""
                if not cursor:
                    break
        except Exception as exc:  # noqa: BLE001 - never block startup on this
            self._event("config", "error", f"instruments-info unavailable: {exc}")
            log.warning("could not validate focus symbols: %s", exc)
            return
        self._report_focus(status, "Trading")

    # -------------------------------------------------------------------- run

    def _primed_counters(self) -> list[str]:
        names = [f"rows.{EXCHANGE}.{s}"
                 for s in ("agg_trade", "mark_price", "force_order", "open_interest")]
        return names + super()._primed_counters()

    async def _startup(self) -> None:
        await self._validate_focus()

    def _coroutines(self) -> dict:
        work = {
            "agg_trade": self._stream("agg_trade", "publicTrade", self.focus, self._handle_trade),
            "mark_price": self._stream("mark_price", "tickers", self.focus, self._handle_ticker,
                                       on_open=self._mark_reset, on_close=self._mark_reset),
            "mark_loop": self._mark_loop(),
            "force_order": self._stream("force_order", "allLiquidation", self.focus,
                                    self._handle_liquidation),
            "open_interest": self._tickers_loop(),
        }
        if self.depth:
            work["depth"] = self._stream("depth", f"orderbook.{self.depth_levels}", self.depth,
                                     self._handle_depth, on_open=self._depth_on_open,
                                     on_close=self._depth_on_close)
            work["depth_watch"] = self._depth_watch_loop()
            log.info("order book: %d symbols at %d levels", len(self.depth), self.depth_levels)
        return work
