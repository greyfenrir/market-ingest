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
import json
import logging
import math
import os
import random
import time

import aiohttp
import websockets

from ..book import BANDS_PCT, empty_aggregate
from ..metrics import Statsd
from .binance_um_depth import SYNCING, DepthSync
from ..health import (
    RollingShare,
    SequenceTracker,
    StreamStats,
    ch_ts,
    connection_event,
    new_session_id,
    now_ms,
)

log = logging.getLogger(__name__)

WS_MARKET = "wss://fstream.binance.com/market"
WS_PUBLIC = "wss://fstream.binance.com/public"
REST = "https://fapi.binance.com"
EXCHANGE = "binance_um"
# Window of the per-book quality share that the Netdata alarm watches.
BOOK_QUALITY_WINDOW_S = 600

# Fields we model explicitly. Anything else the exchange sends is preserved
# verbatim in `extra` - Binance added `nq` and `ap` in the 2026 COIN-M merge,
# and an archive that drops unknown fields loses them permanently.
KNOWN_MARK = {"e", "E", "s", "p", "i", "P", "r", "T"}
KNOWN_AGG = {"e", "E", "s", "a", "p", "q", "f", "l", "T", "m", "M"}
KNOWN_FORCE = {"s", "S", "o", "f", "q", "p", "ap", "X", "l", "z", "T"}


def _extra(payload: dict, known: set[str]) -> str:
    rest = {k: v for k, v in payload.items() if k not in known}
    return json.dumps(rest, separators=(",", ":")) if rest else ""


class BinanceUM:
    def __init__(self, cfg: dict, sink, *, focus: list[str], sd: Statsd) -> None:
        self.sink = sink
        self.sd = sd
        self.cfg = cfg
        self.focus = {s.upper() for s in focus}
        self.session_id = new_session_id()
        self.stall = cfg.get("stall_seconds", {})
        self.reconnect_after = cfg.get("reconnect_after_hours", 23) * 3600

        self.seq = SequenceTracker()
        self.stats: dict[str, StreamStats] = {}
        # last mark-price observation per non-focus symbol, flushed once a minute
        self._pending_mark: dict[str, dict] = {}
        self._pending_minute: int = 0
        # latest mark price per focus symbol: depth is priced in USD at mark
        self._last_mark: dict[str, float] = {}

        # Order book (stream A of the early-collector spec): a local book per
        # symbol, written as one aggregate row per second. Off unless enabled.
        dcfg = cfg.get("depth", {})
        syms = dcfg.get("symbols", "focus")
        syms = self.focus if syms == "focus" else {s.upper() for s in syms}
        self.depth_limit = int(dcfg.get("snapshot_limit", 1000))
        self.depth_speed_ms = int(dcfg.get("update_speed_ms", 100))
        self.depth_prune_pct = float(dcfg.get("prune_pct", 10.0))
        self.depth_stale_s = float(dcfg.get("stale_seconds", 5.0))
        self.depth_parallel = int(dcfg.get("max_parallel_snapshots", 2))
        # Refresh a book once either side keeps less than this share of the
        # coverage its snapshot had, but not more often than every N seconds.
        self.depth_refresh_ratio = float(dcfg.get("refresh_ratio", 0.5))
        self.depth_refresh_min_s = float(dcfg.get("refresh_min_seconds", 60.0))
        self.depth: dict[str, DepthSync] = (
            {s: DepthSync(s, self.depth_limit) for s in sorted(syms)}
            if dcfg.get("enabled", False) else {}
        )
        # Seconds not `ok`, per book. The worst book is what gets published:
        # one book broken all the time is 5 % of twenty, easy to miss in a sum.
        self._book_quality = {s: RollingShare(BOOK_QUALITY_WINDOW_S) for s in self.depth}
        self.collector_version = os.environ.get("COLLECTOR_VERSION", "")
        self._rest: aiohttp.ClientSession | None = None
        self._snap_sem: asyncio.Semaphore | None = None
        self._bg: set[asyncio.Task] = set()
        # True while the depth WebSocket is connected. A snapshot taken with no
        # stream behind it has no events to align with and is thrown away by
        # the reset at the next open - 20 weight per book, every start.
        self._depth_open = False
        # Binance limits REST by IP, so a 429/418 is a pause for every REST
        # caller here, not just the one that got it (monotonic seconds).
        self._rest_pause_until = 0.0

    # ------------------------------------------------------------------ util

    async def _rest_wait(self) -> None:
        """Hold a REST call until any pause ordered by a 429/418 is over."""
        while (left := self._rest_pause_until - time.monotonic()) > 0:
            await asyncio.sleep(left)

    def _rest_pause(self, stream: str, what: str, status: int, headers) -> None:
        """Obey a 429/418 for every REST caller: Binance turns requests sent
        after a 429 into an IP ban, and a ban into a longer one."""
        try:
            wait = float(headers.get("Retry-After") or 60)
        except ValueError:
            wait = 60.0
        self._rest_pause_until = max(self._rest_pause_until, time.monotonic() + wait)
        self._event(stream, "rate_limit", f"{what} HTTP {status}, retry after {wait:.0f}s")
        log.error("REST rate-limited (HTTP %d on %s), pausing all REST for %.0fs",
                  status, what, wait)

    def _stat(self, stream: str) -> StreamStats:
        st = self.stats.get(stream)
        if st is None:
            st = self.stats[stream] = StreamStats(
                exchange=EXCHANGE, connector=EXCHANGE, stream=stream,
                session_id=self.session_id,
            )
        return st

    def _event(self, stream: str, event: str, detail: str = "") -> None:
        self.sink.write(
            "ops.connection_events",
            connection_event(
                exchange=EXCHANGE, connector=EXCHANGE, stream=stream,
                session_id=self.session_id, event=event, detail=detail,
            ),
        )

    def _roll_health(self) -> None:
        minute = int(time.time() // 60)
        for st in self.stats.values():
            row = st.roll(minute)
            if row:
                row["insert_errors"] = self.sink.insert_errors
                self.sink.write("ops.stream_health", row)
        # Sink health belongs on the same dashboard as throughput: a growing
        # queue is the earliest visible sign that ClickHouse is in trouble.
        self.sd.gauge(f"queue_rows.{EXCHANGE}", self.sink.queued_rows())
        self.sd.gauge(f"insert_errors.{EXCHANGE}", self.sink.insert_errors)
        self.sd.gauge(f"dropped_rows.{EXCHANGE}", self.sink.dropped_rows)

    # ------------------------------------------------------------ ws plumbing

    async def _ws_loop(self, stream: str, url: str, handle, on_open=None, on_close=None) -> None:
        """Connect, consume, and never give up. Backoff is exponential with
        jitter so that a Binance-side outage does not turn our reconnects into
        a self-inflicted IP ban. on_open runs on every (re)connect, before the
        first message: state that does not survive a new connection is reset
        there. on_close runs whenever an attempt ends, connected or not."""
        attempt = 0
        while True:
            started = time.monotonic()
            st = self._stat(stream)
            try:
                self._event(stream, "start", url)
                async with websockets.connect(
                    url, open_timeout=20, ping_interval=20, ping_timeout=20,
                    max_size=16 * 1024 * 1024,
                ) as ws:
                    attempt = 0
                    st.last_msg_monotonic = time.monotonic()
                    self._event(stream, "open")
                    log.info("%s connected", stream)
                    if on_open:
                        on_open()
                    await self._consume(ws, stream, st, handle, started)
            except asyncio.CancelledError:
                self._event(stream, "stop")
                raise
            except Exception as exc:  # noqa: BLE001
                self._event(stream, "error", f"{type(exc).__name__}: {exc}")
                log.warning("%s error: %s", stream, exc)
            finally:
                if on_close:
                    on_close()
            # Keep the sequence state: the jump across this reconnect is the
            # measurement of what the outage cost us.
            self.seq.mark_reconnect()
            attempt += 1
            delay = min(60.0, (2 ** min(attempt, 6)) * 0.5) * (0.5 + random.random())
            self._event(stream, "reconnect", f"attempt={attempt} in {delay:.1f}s")
            await asyncio.sleep(delay)

    async def _consume(self, ws, stream: str, st: StreamStats, handle, started: float) -> None:
        stall_after = float(self.stall.get(stream, 300))
        while True:
            if time.monotonic() - started > self.reconnect_after:
                # Binance force-closes at 24h. Rotating first turns an abrupt
                # drop into a controlled handover.
                self._event(stream, "close", "scheduled rotation before 24h limit")
                return
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
            except asyncio.TimeoutError:
                if st.silent_for() > stall_after:
                    self._event(
                        stream, "stall",
                        f"no messages for {st.silent_for():.0f}s (limit {stall_after:.0f}s)",
                    )
                    log.error("%s stalled for %.0fs, reconnecting", stream, st.silent_for())
                    return
                self._roll_health()
                continue
            recv_ms = now_ms()
            payload = json.loads(raw)
            if isinstance(payload, dict) and "data" in payload and "stream" in payload:
                payload = payload["data"]
            handle(payload, recv_ms, len(raw), st)
            self._roll_health()

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
        self.sd.incr(f"rows.{EXCHANGE}.mark_price", len(items))
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
        self.sd.incr(f"rows.{EXCHANGE}.agg_trade", 1)
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
        self.sd.incr(f"rows.{EXCHANGE}.force_order", 1)
        self.sd.incr(f"bytes.{EXCHANGE}.force_order", nbytes)

    # ----------------------------------------------------------- order book

    def _depth_resync(self, sync: DepthSync, reason: str) -> None:
        self._event("depth", "resync", f"{sync.symbol}: {reason}")
        log.warning("%s depth resync: %s", sync.symbol, reason)
        self.sd.incr(f"resyncs.{EXCHANGE}.depth", 1)

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
        weight spread over seconds rather than one burst. 429 and 418 are
        obeyed: Binance turns ignored 429s into an IP ban.

        Gives up as soon as the stream closes. snapshot_pending stays set, and
        the reset at the next open clears it and lets the scheduler start over."""
        assert self._rest is not None and self._snap_sem is not None
        gen = sync.generation
        delay = 1.0

        def wanted() -> bool:
            # a shadow is dropped when its owner resets or swaps it in
            return (sync.generation == gen and self._depth_open
                    and (owner is None or owner.shadow is sync))

        while wanted():
            snap = None
            async with self._snap_sem:
                await self._rest_wait()
                if not wanted():
                    return
                try:
                    async with self._rest.get(
                        f"{REST}/fapi/v1/depth",
                        params={"symbol": sync.symbol, "limit": self.depth_limit},
                    ) as r:
                        if r.status in (418, 429):
                            # the next pass waits in _rest_wait, as does
                            # every other snapshot and the OI poll
                            self._rest_pause("depth", sync.symbol, r.status, r.headers)
                            continue
                        if r.status != 200:
                            raise RuntimeError(f"HTTP {r.status}: {(await r.text())[:200]}")
                        snap = await r.json()
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

    def _depth_maybe_refresh(self, sync: DepthSync, agg: dict, mono: float) -> None:
        """Start a refresh once the snapshot's reach has worn thin on either
        side. The first aggregate of a fresh book records what a snapshot
        reaches for this symbol; later ones are measured against it, so a book
        whose snapshot spans +-10 % is not refreshed every time BTC's would be."""
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

    def _emit_book_rows(self, second: int, mono: float) -> None:
        """One market.book_agg row per book for the second ending at `second`.

        A row is written for every book every second, valid or not: a second
        without a usable book is recorded as such (state=resync, zeros, every
        band invalid), so the archive never has to be read for silence."""
        ingest = ch_ts(now_ms())
        event_time = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(second))
        not_ok = 0
        for sync in self.depth.values():
            counters = sync.take_second()
            agg = None
            if sync.live:
                sync.book.prune(self.depth_prune_pct)
                if sync.book.crossed():
                    self._depth_resync(sync, sync.reset("crossed book"))
                else:
                    agg = sync.book.aggregate(BANDS_PCT, mark=self._last_mark.get(sync.symbol))
                    if agg is None:
                        # live but a side is empty: nothing to measure, and no
                        # delta will refill a side the book has lost track of
                        self._depth_resync(sync, sync.reset("empty side"))
                    else:
                        self._depth_maybe_refresh(sync, agg, mono)
            if agg is None:
                state = "resync"
            elif counters["resynced"] or not agg["covered_bid_pct"] or not agg["covered_ask_pct"]:
                # resynced within the second, or price has left the range the
                # snapshot covered: the numbers exist but are not the whole truth
                state = "degraded"
            elif mono - sync.last_applied_mono > self.depth_stale_s:
                state = "stale"
            else:
                state = "ok"
            not_ok += state != "ok"
            self._book_quality[sync.symbol].add(state != "ok")
            row = {
                "exchange": EXCHANGE,
                "symbol": sync.symbol,
                "event_time": event_time,
                "ingest_time": ingest,
                "last_update_time": ch_ts(sync.last_event_ms),
                "seq_first": counters["seq_first"],
                "seq_last": counters["seq_last"],
                "n_updates": counters["n_updates"],
                "state": state,
                "collector_version": self.collector_version,
            }
            row.update(agg or empty_aggregate(BANDS_PCT))
            self.sink.write("market.book_agg", row)
        self.sd.incr(f"rows.{EXCHANGE}.book_agg", len(self.depth))
        self.sd.gauge(f"book_not_ok.{EXCHANGE}", not_ok)
        if self._book_quality:
            self.sd.gauge(f"book_not_ok_pct.{EXCHANGE}",
                          round(max(q.pct() for q in self._book_quality.values()), 2))

    async def _depth_tick_loop(self) -> None:
        """Fires just after each wall-clock second. The clock is the local one,
        kept honest by chrony; a second the loop oversleeps is skipped rather
        than invented, because the book's state at that instant is gone."""
        while True:
            now = time.time()
            boundary = math.floor(now) + 1
            await asyncio.sleep(boundary - now + 0.05)
            self._emit_book_rows(int(boundary), time.monotonic())

    # ---------------------------------------------------------- open interest

    async def _open_interest_loop(self) -> None:
        """No WebSocket exists for OI; REST polling is the only option. OI is
        the weight in "median across exchanges weighted by OI", so this is a
        first-class feed, not an afterthought."""
        period = float(self.cfg.get("open_interest_poll_seconds", 60))
        which = self.cfg.get("open_interest_symbols", "focus")
        st = self._stat("open_interest")
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            while True:
                symbols = sorted(self.focus) if which == "focus" else sorted(self.focus)
                t0 = time.monotonic()
                for sym in symbols:
                    await self._rest_wait()
                    try:
                        async with s.get(f"{REST}/fapi/v1/openInterest", params={"symbol": sym}) as r:
                            if r.status in (418, 429):
                                self._rest_pause("open_interest", sym, r.status, r.headers)
                                continue
                            if r.status != 200:
                                self._event("open_interest", "error", f"{sym} HTTP {r.status}")
                                continue
                            d = await r.json()
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
                        self.sd.incr(f"rows.{EXCHANGE}.open_interest", 1)
                    except Exception as exc:  # noqa: BLE001
                        self._event("open_interest", "error", f"{sym}: {exc}")
                    await asyncio.sleep(0.05)  # stay well inside the REST weight limit
                self._roll_health()
                await asyncio.sleep(max(1.0, period - (time.monotonic() - t0)))

    # ------------------------------------------------------- config validation

    async def _validate_focus(self) -> None:
        """Check every configured focus symbol actually trades.

        A delisted or settling symbol is the quietest possible failure: it is
        spelled correctly, the subscription is accepted, and it simply never
        produces a row. Caught in the field with TONUSDT, whose status had moved
        to SETTLING - the collector looked perfectly healthy while silently
        covering 19 symbols instead of 20.
        """
        timeout = aiohttp.ClientTimeout(total=30)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get(f"{REST}/fapi/v1/exchangeInfo") as r:
                    r.raise_for_status()
                    info = await r.json()
        except Exception as exc:  # noqa: BLE001 - never block startup on this
            self._event("config", "error", f"exchangeInfo unavailable: {exc}")
            log.warning("could not validate focus symbols: %s", exc)
            return

        status = {d["symbol"]: d.get("status", "") for d in info.get("symbols", [])}
        for sym in sorted(self.focus):
            st = status.get(sym)
            if st is None:
                self._event("config", "error", f"{sym} does not exist on {EXCHANGE}")
                log.error("focus symbol %s does not exist on this venue", sym)
            elif st != "TRADING":
                self._event("config", "error", f"{sym} status={st}, expect no data")
                log.error("focus symbol %s is %s, not TRADING - it will produce no data", sym, st)
        tradable = sum(1 for s in self.focus if status.get(s) == "TRADING")
        self._event("config", "subscribed", f"{tradable}/{len(self.focus)} focus symbols tradable")
        log.info("focus universe: %d/%d symbols tradable", tradable, len(self.focus))

    # -------------------------------------------------------------------- run

    async def run(self) -> None:
        # Publish the quality counters at zero from the start, so their charts
        # and the alarms watching them exist before anything goes wrong.
        self.sd.prime(
            f"gaps.{EXCHANGE}.agg_trade",
            f"rows.{EXCHANGE}.agg_trade",
            f"rows.{EXCHANGE}.mark_price",
            f"rows.{EXCHANGE}.force_order",
            f"rows.{EXCHANGE}.open_interest",
        )
        if self.depth:
            self.sd.prime(f"rows.{EXCHANGE}.book_agg", f"resyncs.{EXCHANGE}.depth",
                          f"refreshes.{EXCHANGE}.depth")
        await self._validate_focus()
        focus_streams = "/".join(f"{s.lower()}@aggTrade" for s in sorted(self.focus))
        tasks = [
            asyncio.create_task(
                self._ws_loop("mark_price", f"{WS_MARKET}/ws/!markPrice@arr@1s", self._handle_mark),
                name="mark_price"),
            asyncio.create_task(
                self._ws_loop("agg_trade", f"{WS_MARKET}/stream?streams={focus_streams}", self._handle_agg),
                name="agg_trade"),
            asyncio.create_task(
                self._ws_loop("force_order", f"{WS_MARKET}/ws/!forceOrder@arr", self._handle_force),
                name="force_order"),
            asyncio.create_task(self._open_interest_loop(), name="open_interest"),
        ]
        if self.depth:
            # Separate connection on /public: depth is not served on /market,
            # and a stream on the wrong route is accepted and stays silent.
            self._rest = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
            self._snap_sem = asyncio.Semaphore(self.depth_parallel)
            depth_streams = "/".join(f"{s.lower()}@depth@{self.depth_speed_ms}ms"
                                     for s in self.depth)
            tasks += [
                asyncio.create_task(
                    self._ws_loop("depth", f"{WS_PUBLIC}/stream?streams={depth_streams}",
                                  self._handle_depth, on_open=self._depth_on_open,
                                  on_close=self._depth_on_close),
                    name="depth"),
                asyncio.create_task(self._depth_snapshot_loop(), name="depth_snapshots"),
                asyncio.create_task(self._depth_tick_loop(), name="depth_tick"),
            ]
            log.info("order book: %d symbols at %d ms", len(self.depth), self.depth_speed_ms)
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in [*tasks, *self._bg]:
                t.cancel()
            if self._rest:
                await self._rest.close()
