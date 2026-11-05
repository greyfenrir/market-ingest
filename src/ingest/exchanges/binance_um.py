"""Binance USDⓈ-M futures collector.

Endpoint note, learned the hard way: on 2026-04-23 Binance split the futures
WebSocket into /public, /market and /private, and retired the legacy /ws/ path
for everything under /market. The old URLs still ACCEPT the connection and
still ACCEPT the subscription - they simply never send a message. So
`wss://fstream.binance.com/ws/btcusdt@aggTrade` looks healthy and delivers
nothing, forever. Streams below are therefore addressed as:

    /market/ws/<stream>   aggTrade, markPrice, forceOrder, kline, ticker
    /public/ws/<stream>   bookTicker, depth

That failure mode - an open socket that is silently empty - is also why every
stream here runs under a stall watchdog rather than trusting "connected".
"""

from __future__ import annotations

import asyncio
import logging
import time

from ..health import StreamStats, ch_ts, now_ms
from ..metrics import Statsd
from ..rest import RateLimited, RestClient
from .base import Collector, extra_fields as _extra
from .binance_um_depth import SYNCING, DepthSync

log = logging.getLogger(__name__)

WS_MARKET = "wss://fstream.binance.com/market"
WS_PUBLIC = "wss://fstream.binance.com/public"
REST = "https://fapi.binance.com"
EXCHANGE = "binance_um"
# The IP's request weight for the current minute, counted by Binance.
USED_WEIGHT = "X-MBX-USED-WEIGHT-1M"


def depth_weight(limit: int) -> int:
    """Request weight of GET /fapi/v1/depth at this limit."""
    if limit <= 50:
        return 2
    if limit <= 100:
        return 5
    if limit <= 500:
        return 10
    return 20

# Fields we model explicitly. Anything else the exchange sends is preserved
# verbatim in `extra` - Binance added `nq` and `ap` in the 2026 COIN-M merge,
# and an archive that drops unknown fields loses them permanently.
KNOWN_MARK = {"e", "E", "s", "p", "i", "P", "r", "T"}
KNOWN_AGG = {"e", "E", "s", "a", "p", "q", "f", "l", "T", "m", "M"}
KNOWN_FORCE = {"s", "S", "o", "f", "q", "p", "ap", "X", "l", "z", "T"}


class BinanceUM(Collector):
    EXCHANGE = EXCHANGE

    def __init__(self, cfg: dict, sink, *, focus: list[str], sd: Statsd) -> None:
        super().__init__(cfg, sink, focus=focus, sd=sd)
        # last mark-price observation per non-focus symbol, flushed once a minute
        self._pending_mark: dict[str, dict] = {}
        self._pending_minute: int = 0
        # Binance allows 2400 weight a minute per IP; the default leaves half
        # to whatever else runs from the same address.
        self.rest = RestClient(REST, budget=int(cfg.get("rest_weight_per_minute", 1200)),
                               used_header=USED_WEIGHT, on_event=self._event)

        # Order book (stream A of the early-collector spec): a local book per
        # symbol, written as one aggregate row per second. Off unless enabled.
        dcfg = cfg.get("depth", {})
        syms = dcfg.get("symbols", "focus")
        syms = self.focus if syms == "focus" else {s.upper() for s in syms}
        self.depth_limit = int(dcfg.get("snapshot_limit", 1000))
        self.depth_speed_ms = int(dcfg.get("update_speed_ms", 100))
        self.depth_parallel = int(dcfg.get("max_parallel_snapshots", 2))
        # Refresh a book once either side keeps less than this share of the
        # coverage its snapshot had, but not more often than every N seconds.
        self.depth_refresh_ratio = float(dcfg.get("refresh_ratio", 0.5))
        self.depth_refresh_min_s = float(dcfg.get("refresh_min_seconds", 60.0))
        if dcfg.get("enabled", False):
            self._init_books({s: DepthSync(s, self.depth_limit) for s in sorted(syms)})
        self._snap_sem = asyncio.Semaphore(self.depth_parallel)
        # True while the depth WebSocket is connected. A snapshot taken with no
        # stream behind it has no events to align with and is thrown away by
        # the reset at the next open - 20 weight per book, every start.
        self._depth_open = False

    # ------------------------------------------------------------ ws plumbing

    def _unwrap(self, payload):
        """Combined streams (/stream?streams=) wrap each message."""
        if isinstance(payload, dict) and "data" in payload and "stream" in payload:
            return payload["data"]
        return payload

    # -------------------------------------------------------------- handlers

    def _handle_mark(self, payload, recv_ms: float, nbytes: int, st: StreamStats) -> None:
        items = payload if isinstance(payload, list) else [payload]
        minute = int(recv_ms // 60000)
        if self._pending_minute == 0:
            self._pending_minute = minute

        lag = None
        for it in items:
            if it.get("e") != "markPriceUpdate":
                continue
            sym = it["s"]
            ev = float(it["E"])
            lag = int(recv_ms - ev)
            row = {
                "exchange": EXCHANGE,
                "symbol": sym,
                "event_time": ch_ts(ev),
                "ingest_time": ch_ts(recv_ms),
                "mark_price": float(it.get("p", 0) or 0),
                "index_price": float(it.get("i", 0) or 0),
                "settle_price": float(it.get("P", 0) or 0),
                "funding_rate": float(it.get("r", 0) or 0),
                "next_funding_time": ch_ts(float(it.get("T", 0) or 0)),
                "sampled": 0,
                "extra": _extra(it, KNOWN_MARK),
            }
            if sym in self.focus:
                self.sink.write("market.mark_price", row)
                self._last_mark[sym] = row["mark_price"]
            else:
                # Keep only the newest observation; it is written once the
                # minute closes. 935 symbols x 1/s would be 65M rows a day,
                # and none of the factors downstream need that resolution.
                row["sampled"] = 1
                self._pending_mark[sym] = row

        if minute != self._pending_minute:
            for row in self._pending_mark.values():
                self.sink.write("market.mark_price", row)
            self._pending_mark.clear()
            self._pending_minute = minute

        st.observe(nbytes=nbytes, rows=len(items), lag_ms=lag, symbol=None)
        st.symbols.update(i.get("s", "") for i in items)
        self._count_rows("mark_price", len(items))
        self.sd.incr(f"bytes.{EXCHANGE}.mark_price", nbytes)
        if lag is not None:
            self.sd.gauge(f"lag_ms.{EXCHANGE}.mark_price", max(0, lag))

    def _handle_agg(self, payload, recv_ms: float, nbytes: int, st: StreamStats) -> None:
        if payload.get("e") != "aggTrade":
            return
        sym = payload["s"]
        ev = float(payload["E"])
        agg_id = int(payload["a"])

        missing, spans_reconnect = self.seq.check(sym, agg_id)
        if missing:
            st.gaps += 1
            self.sink.write("ops.sequence_gaps", {
                "detected_at": ch_ts(recv_ms),
                "exchange": EXCHANGE,
                # Distinguishes "we were restarted" from "the feed dropped
                # messages while we were connected" - very different problems.
                "stream": "agg_trade/reconnect" if spans_reconnect else "agg_trade",
                "symbol": sym,
                "session_id": self.session_id,
                "expected_id": agg_id - missing, "received_id": agg_id,
                "missing": missing,
            })

        self.sink.write("market.trades", {
            "exchange": EXCHANGE,
            "symbol": sym,
            "event_time": ch_ts(float(payload.get("T", ev))),
            "ingest_time": ch_ts(recv_ms),
            "agg_id": agg_id,
            "first_id": int(payload.get("f", 0)),
            "last_id": int(payload.get("l", 0)),
            "price": float(payload["p"]),
            "qty": float(payload["q"]),
            "is_buyer_maker": 1 if payload.get("m") else 0,
            "extra": _extra(payload, KNOWN_AGG),
        })
        st.observe(nbytes=nbytes, rows=1, lag_ms=int(recv_ms - ev), symbol=sym)
        self._count_rows("agg_trade", 1)
        self.sd.incr(f"bytes.{EXCHANGE}.agg_trade", nbytes)
        self.sd.gauge(f"lag_ms.{EXCHANGE}.agg_trade", max(0, int(recv_ms - ev)))
        if missing:
            self.sd.incr(f"gaps.{EXCHANGE}.agg_trade", missing)

    def _handle_force(self, payload, recv_ms: float, nbytes: int, st: StreamStats) -> None:
        if payload.get("e") != "forceOrder":
            return
        o = payload.get("o") or {}
        ev = float(payload["E"])
        self.sink.write("market.liquidations", {
            "exchange": EXCHANGE,
            "symbol": o.get("s", ""),
            "event_time": ch_ts(ev),
            "ingest_time": ch_ts(recv_ms),
            "trade_time": ch_ts(float(o.get("T", ev) or ev)),
            "side": o.get("S", ""),
            "order_type": o.get("o", ""),
            "time_in_force": o.get("f", ""),
            "order_status": o.get("X", ""),
            "price": float(o.get("p", 0) or 0),
            "avg_price": float(o.get("ap", 0) or 0),
            "orig_qty": float(o.get("q", 0) or 0),
            "last_filled_qty": float(o.get("l", 0) or 0),
            "cum_filled_qty": float(o.get("z", 0) or 0),
            "extra": _extra(o, KNOWN_FORCE),
        })
        st.observe(nbytes=nbytes, rows=1, lag_ms=int(recv_ms - ev), symbol=o.get("s"))
        self._count_rows("force_order", 1)
        self.sd.incr(f"bytes.{EXCHANGE}.force_order", nbytes)

    # ----------------------------------------------------------- order book

    def _depth_report(self, sync: DepthSync) -> None:
        """Log what DepthSync flagged since the last call."""
        if sync.just_live:
            sync.just_live = False
            self._event("depth", "synced", f"{sync.symbol} u={sync.book.last_update_id}")
        if sync.just_refreshed:
            sync.just_refreshed = False
            self._event("depth", "refresh", f"{sync.symbol} done: u={sync.book.last_update_id}")
            self.sd.incr(f"refreshes.{EXCHANGE}.depth", 1)

    def _depth_on_open(self) -> None:
        """A new connection continues no sequence we can vouch for: events
        missed between the two sessions are unknown. Start every book over."""
        for sync in self.depth.values():
            was = sync.state
            sync.reset("reconnect")
            if was != SYNCING:
                self._depth_resync(sync, "reconnect")
        self._depth_open = True

    def _depth_on_close(self) -> None:
        """No stream, no snapshots: they could not be aligned with anything.
        Books keep their last state until the next open resets them, and the
        per-second rows report them as stale meanwhile."""
        self._depth_open = False

    def _handle_depth(self, payload, recv_ms: float, nbytes: int, st: StreamStats) -> None:
        if payload.get("e") != "depthUpdate":
            return
        sync = self.depth.get(payload.get("s"))
        if sync is None:
            return
        reason = sync.on_event(payload, time.monotonic())
        if reason:
            self._depth_resync(sync, reason)
        self._depth_report(sync)
        # rows=0: deltas are not stored, only the per-second aggregate is
        st.observe(nbytes=nbytes, rows=0, lag_ms=int(recv_ms - float(payload["E"])),
                   symbol=sync.symbol)
        self.sd.incr(f"bytes.{EXCHANGE}.depth", nbytes)

    async def _depth_snapshot_loop(self) -> None:
        """Starts a snapshot fetch for every book that needs one. Kept apart
        from the message handler so that handling a message never has to
        spawn tasks, and so that a burst of resyncs is paced by one place."""
        while True:
            self._depth_request_snapshots()
            await asyncio.sleep(0.2)

    def _depth_request_snapshots(self) -> int:
        """One pass of the scheduler. Returns how many fetches it started."""
        if not self._depth_open:
            return 0
        started = 0
        for sync in self.depth.values():
            # the book itself, or the shadow a refresh is building for it
            for target, owner in ((sync, None), (sync.shadow, sync)):
                if target is not None and target.needs_snapshot:
                    target.snapshot_pending = True
                    t = asyncio.create_task(self._fetch_snapshot(target, owner),
                                            name=f"snapshot:{sync.symbol}")
                    self._bg.add(t)
                    t.add_done_callback(self._bg.discard)
                    started += 1
        return started

    async def _fetch_snapshot(self, sync: DepthSync, owner: DepthSync | None = None) -> None:
        """Fetch /fapi/v1/depth until it aligns with the buffered events.

        `sync` is a book, or - with `owner` set - the shadow a refresh is
        building for `owner`. A shadow's misalignment is not a resync: the
        live book is untouched, so it is logged as a refresh retry instead.

        A 1000-level snapshot weighs 20, and at most `max_parallel_snapshots`
        are in flight, so a mass reconnect of every book costs a few hundred
        weight spread over seconds rather than one burst. The weight comes out
        of the collector's REST budget, which also obeys 429 and 418.

        Gives up as soon as the stream closes. snapshot_pending stays set, and
        the reset at the next open clears it and lets the scheduler start over."""
        assert self.rest is not None
        gen = sync.generation
        delay = 1.0

        def wanted() -> bool:
            # a shadow is dropped when its owner resets or swaps it in
            return (sync.generation == gen and self._depth_open
                    and (owner is None or owner.shadow is sync))

        while wanted():
            snap = None
            async with self._snap_sem:
                try:
                    snap = await self.rest.get(
                        "/fapi/v1/depth", {"symbol": sync.symbol, "limit": self.depth_limit},
                        weight=depth_weight(self.depth_limit), stream="depth",
                        what=sync.symbol, still_wanted=wanted)
                    if snap is None:
                        return      # reset or dropped while waiting for its turn
                except RateLimited:
                    continue        # the next call waits out the pause
                except Exception as exc:  # noqa: BLE001
                    self._event("depth", "error", f"{sync.symbol} snapshot: {type(exc).__name__}: {exc}")
                    log.warning("%s depth snapshot failed: %s", sync.symbol, exc)
            if snap is None:
                await asyncio.sleep(delay)
                delay = min(30.0, delay * 2)
                continue
            if not wanted():
                return  # the book was reset, or the shadow dropped, meanwhile
            reason = sync.on_snapshot(snap, time.monotonic())
            if reason is None:
                if owner is None:
                    self._event("depth", "snapshot", f"{sync.symbol} lastUpdateId={snap['lastUpdateId']}")
                    self._depth_report(sync)    # live already, if the buffer spanned it
                else:
                    owner.try_swap()
                    self._depth_report(owner)
                return
            if owner is None:
                self._depth_resync(sync, reason)
            else:
                self._event("depth", "refresh", f"{sync.symbol} retry: {reason}")
            # This task keeps the job; stop the scheduler from starting another.
            gen = sync.generation
            sync.snapshot_pending = True
            await asyncio.sleep(delay)
            delay = min(30.0, delay * 2)

    def _book_measured(self, sync: DepthSync, agg: dict, mono: float) -> None:
        """Start a refresh once the snapshot's reach has worn thin on either
        side. The first aggregate of a fresh or refreshed book records how far
        it reaches for this symbol; later ones are measured against it, so a
        book whose snapshot spans +-10 % is not refreshed every time BTC's would be."""
        bid, ask = agg["covered_bid_pct"], agg["covered_ask_pct"]
        if sync.cover0 is None:
            sync.cover0 = (bid, ask)
            return
        bid0, ask0 = sync.cover0
        r = self.depth_refresh_ratio
        worn = (bid0 > 0 and bid < bid0 * r) or (ask0 > 0 and ask < ask0 * r)
        if (worn and mono - sync.last_refresh_mono >= self.depth_refresh_min_s
                and sync.start_refresh(mono)):
            self._event("depth", "refresh",
                        f"{sync.symbol} started: covered {bid:.3f}/{ask:.3f}% "
                        f"of {bid0:.3f}/{ask0:.3f}% at snapshot")

    # ---------------------------------------------------------- open interest

    async def _open_interest_loop(self) -> None:
        """No WebSocket exists for OI; REST polling is the only option. OI is
        the weight in "median across exchanges weighted by OI", so this is a
        first-class feed, not an afterthought."""
        period = float(self.cfg.get("open_interest_poll_seconds", 60))
        which = self.cfg.get("open_interest_symbols", "focus")
        st = self._stat("open_interest")
        assert self.rest is not None
        while True:
            symbols = sorted(self.focus) if which == "focus" else sorted(self.focus)
            t0 = time.monotonic()
            for sym in symbols:
                try:
                    d = await self.rest.get("/fapi/v1/openInterest", {"symbol": sym},
                                            stream="open_interest", what=sym)
                except RateLimited:
                    continue
                except Exception as exc:  # noqa: BLE001
                    self._event("open_interest", "error", f"{sym}: {exc}")
                    continue
                try:
                    recv_ms = now_ms()
                    ev = float(d.get("time", recv_ms))
                    self.sink.write("market.open_interest", {
                        "exchange": EXCHANGE,
                        "symbol": d["symbol"],
                        "event_time": ch_ts(ev),
                        "ingest_time": ch_ts(recv_ms),
                        "open_interest": float(d["openInterest"]),
                        "extra": _extra(d, {"symbol", "openInterest", "time"}),
                    })
                    st.observe(nbytes=len(str(d)), rows=1,
                               lag_ms=int(recv_ms - ev), symbol=d["symbol"])
                    self._count_rows("open_interest", 1)
                except Exception as exc:  # noqa: BLE001
                    self._event("open_interest", "error", f"{sym}: {exc}")
                await asyncio.sleep(0.05)  # a poll is not a burst
            self._roll_health()
            await asyncio.sleep(max(1.0, period - (time.monotonic() - t0)))

    # ------------------------------------------------------- config validation

    async def _validate_focus(self) -> None:
        """Check every configured focus symbol actually trades."""
        assert self.rest is not None
        try:
            info = await self.rest.get("/fapi/v1/exchangeInfo", stream="config")
        except Exception as exc:  # noqa: BLE001 - never block startup on this
            self._event("config", "error", f"exchangeInfo unavailable: {exc}")
            log.warning("could not validate focus symbols: %s", exc)
            return
        self._report_focus({d["symbol"]: d.get("status", "") for d in info.get("symbols", [])},
                           "TRADING")

    # -------------------------------------------------------------------- run

    def _primed_counters(self) -> list[str]:
        names = [
            f"gaps.{EXCHANGE}.agg_trade",
            f"rows.{EXCHANGE}.agg_trade",
            f"rows.{EXCHANGE}.mark_price",
            f"rows.{EXCHANGE}.force_order",
            f"rows.{EXCHANGE}.open_interest",
        ]
        if self.depth:
            names.append(f"refreshes.{EXCHANGE}.depth")
        return names + super()._primed_counters()

    async def _startup(self) -> None:
        await self._validate_focus()

    def _coroutines(self) -> dict:
        focus_streams = "/".join(f"{s.lower()}@aggTrade" for s in sorted(self.focus))
        work = {
            "mark_price": self._ws_loop(
                "mark_price", f"{WS_MARKET}/ws/!markPrice@arr@1s", self._handle_mark),
            "agg_trade": self._ws_loop(
                "agg_trade", f"{WS_MARKET}/stream?streams={focus_streams}", self._handle_agg),
            "force_order": self._ws_loop(
                "force_order", f"{WS_MARKET}/ws/!forceOrder@arr", self._handle_force),
            "open_interest": self._open_interest_loop(),
        }
        if self.depth:
            # Separate connection on /public: depth is not served on /market,
            # and a stream on the wrong route is accepted and stays silent.
            depth_streams = "/".join(f"{s.lower()}@depth@{self.depth_speed_ms}ms"
                                     for s in self.depth)
            work["depth"] = self._ws_loop(
                "depth", f"{WS_PUBLIC}/stream?streams={depth_streams}", self._handle_depth,
                on_open=self._depth_on_open, on_close=self._depth_on_close)
            work["depth_snapshots"] = self._depth_snapshot_loop()
            log.info("order book: %d symbols at %d ms", len(self.depth), self.depth_speed_ms)
        return work
