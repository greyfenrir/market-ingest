"""What every exchange collector shares.

A subclass names its exchange, says which WebSocket streams to open and how to
read their messages, and owns whatever is peculiar to its venue: how its order
book is synchronised, how open interest is fetched. Everything that must work
the same way for every venue lives here, because a difference in it would be a
difference in the archive:

- ops.connection_events and ops.stream_health rows, and the StatsD gauges;
- the WebSocket loop: backoff, stall watchdog, rotation before a forced close;
- one REST client (self.rest): a shared request budget and one pause when limited;
- one market.book_agg row per book per second, and the book quality gauge;
- the report of focus symbols that do not trade.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import time
from typing import Awaitable, Callable

import websockets

from ..book import BANDS_PCT, empty_aggregate
from ..health import (
    RollingShare,
    SequenceTracker,
    StreamStats,
    ch_ts,
    connection_event,
    new_session_id,
    now_ms,
)
from ..metrics import Statsd
from ..rest import RestClient

log = logging.getLogger(__name__)

# Window of the per-book quality share that the Netdata alarm watches.
BOOK_QUALITY_WINDOW_S = 600


def extra_fields(payload: dict, known: set[str]) -> str:
    """Fields the schema does not model, kept verbatim: an archive that drops
    unknown fields loses them permanently."""
    rest = {k: v for k, v in payload.items() if k not in known}
    return json.dumps(rest, separators=(",", ":")) if rest else ""


class Collector:
    """Base of one exchange collector. Subclasses set EXCHANGE, fill
    self.depth via _init_books(), and implement _coroutines()."""

    EXCHANGE = ""

    def __init__(self, cfg: dict, sink, *, focus: list[str], sd: Statsd) -> None:
        self.sink = sink
        self.sd = sd
        self.cfg = cfg
        self.focus = {s.upper() for s in focus}
        self.session_id = new_session_id()
        self.stall = cfg.get("stall_seconds", {})
        self.reconnect_after = cfg.get("reconnect_after_hours", 23) * 3600
        self.collector_version = os.environ.get("COLLECTOR_VERSION", "")

        self.seq = SequenceTracker()
        self.stats: dict[str, StreamStats] = {}
        # latest mark price per symbol: depth is priced in USD at mark
        self._last_mark: dict[str, float] = {}
        self._bg: set[asyncio.Task] = set()
        # The open connection of each stream, while it is open. A venue that
        # subscribes by message needs it to resubscribe one topic in place.
        self._ws: dict[str, object] = {}
        # Every REST call goes through this; the subclass creates it.
        self.rest: RestClient | None = None

        dcfg = cfg.get("depth", {})
        self.depth_prune_pct = float(dcfg.get("prune_pct", 10.0))
        self.depth_stale_s = float(dcfg.get("stale_seconds", 5.0))
        # Books by symbol. Each is an object with: symbol, live, book
        # (a LocalBook), last_applied_mono, live_since_mono, last_event_ms,
        # take_second() and reset(reason) -> reason, and `bounded`: True when
        # the venue itself keeps the book to its top N levels, so there is
        # nothing to prune. Empty unless the subclass enables depth.
        self.depth: dict = {}
        self._book_quality: dict[str, RollingShare] = {}

    def _init_books(self, books: dict) -> None:
        self.depth = books
        # Seconds not `ok`, per book. The worst book is what gets published:
        # one book broken all the time is 5 % of twenty, easy to miss in a sum.
        self._book_quality = {s: RollingShare(BOOK_QUALITY_WINDOW_S) for s in books}

    # ------------------------------------------------------------------ util

    def _count_rows(self, stream: str, n: int) -> None:
        """Rows produced, per stream and for the whole exchange. The total is
        what the stall alarm watches: per stream it would fire on every quiet
        liquidation feed, summed over all exchanges it would miss one going down."""
        self.sd.incr(f"rows.{self.EXCHANGE}.{stream}", n)
        self.sd.incr(f"rows_total.{self.EXCHANGE}", n)

    def _stat(self, stream: str) -> StreamStats:
        st = self.stats.get(stream)
        if st is None:
            st = self.stats[stream] = StreamStats(
                exchange=self.EXCHANGE, connector=self.EXCHANGE, stream=stream,
                session_id=self.session_id,
            )
        return st

    def _event(self, stream: str, event: str, detail: str = "") -> None:
        self.sink.write(
            "ops.connection_events",
            connection_event(
                exchange=self.EXCHANGE, connector=self.EXCHANGE, stream=stream,
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
        ex = self.EXCHANGE
        self.sd.gauge(f"queue_rows.{ex}", self.sink.queued_rows())
        self.sd.gauge(f"insert_errors.{ex}", self.sink.insert_errors)
        self.sd.gauge(f"dropped_rows.{ex}", self.sink.dropped_rows)
        if self.rest:
            self.sd.gauge(f"rest_used_pct.{ex}", round(100 * self.rest.used() / self.rest.budget))

    # ------------------------------------------------------------ ws plumbing

    def _unwrap(self, payload):
        """Turn a decoded message into what the stream's handler expects."""
        return payload

    async def _ws_loop(self, stream: str, url: str, handle,
                       on_open: Callable[[], None] | None = None,
                       on_close: Callable[[], None] | None = None,
                       subscribe: list[str] | None = None,
                       app_ping: tuple[float, str] | None = None) -> None:
        """Connect, consume, and never give up. Backoff is exponential with
        jitter so that a venue-side outage does not turn our reconnects into
        a self-inflicted IP ban. on_open runs on every (re)connect, before the
        first message: state that does not survive a new connection is reset
        there. on_close runs whenever an attempt ends, connected or not.

        subscribe: messages sent right after on_open, for venues that take
        subscriptions as messages rather than in the URL. app_ping: (interval,
        message) for venues that want a ping of their own on top of the
        WebSocket one (Bybit drops a connection without it)."""
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
                    self._ws[stream] = ws
                    if on_open:
                        on_open()
                    for msg in subscribe or ():
                        await ws.send(msg)
                    await self._consume(ws, stream, st, handle, started, app_ping)
            except asyncio.CancelledError:
                self._event(stream, "stop")
                raise
            except Exception as exc:  # noqa: BLE001
                self._event(stream, "error", f"{type(exc).__name__}: {exc}")
                log.warning("%s error: %s", stream, exc)
            finally:
                self._ws.pop(stream, None)
                if on_close:
                    on_close()
            # Keep the sequence state: the jump across this reconnect is the
            # measurement of what the outage cost us.
            self.seq.mark_reconnect()
            attempt += 1
            delay = min(60.0, (2 ** min(attempt, 6)) * 0.5) * (0.5 + random.random())
            self._event(stream, "reconnect", f"attempt={attempt} in {delay:.1f}s")
            await asyncio.sleep(delay)

    async def _consume(self, ws, stream: str, st: StreamStats, handle, started: float,
                       app_ping: tuple[float, str] | None = None) -> None:
        stall_after = float(self.stall.get(stream, 300))
        last_ping = time.monotonic()
        while True:
            if app_ping and time.monotonic() - last_ping >= app_ping[0]:
                await ws.send(app_ping[1])
                last_ping = time.monotonic()
            if time.monotonic() - started > self.reconnect_after:
                # Venues force-close long-lived connections (Binance at 24h).
                # Rotating first turns an abrupt drop into a controlled one.
                self._event(stream, "close", "scheduled rotation before the venue's limit")
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
            payload = self._unwrap(json.loads(raw))
            if payload is not None:     # None: a control message, already handled
                handle(payload, recv_ms, len(raw), st)
            self._roll_health()

    # ----------------------------------------------------------- order book

    def _depth_resync(self, sync, reason: str) -> None:
        self._event("depth", "resync", f"{sync.symbol}: {reason}")
        log.warning("%s depth resync: %s", sync.symbol, reason)
        self.sd.incr(f"resyncs.{self.EXCHANGE}.depth", 1)

    def _contract_size(self, symbol: str) -> float:
        """Base-asset quantity of one contract; 1 for venues quoting in coins."""
        return 1.0

    def _book_measured(self, sync, agg: dict, mono: float) -> None:
        """Called with every aggregate of a live book; a venue that must
        refresh its book's reach does it here."""

    def _emit_book_rows(self, second: int, mono: float) -> None:
        """One market.book_agg row per book for the second ending at `second`.

        A row is written for every book every second, valid or not: a second
        without a usable book is recorded as such (state=resync, zeros), so the
        archive never has to be read for silence."""
        ex = self.EXCHANGE
        ingest = ch_ts(now_ms())
        event_time = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(second))
        not_ok = 0
        for sync in self.depth.values():
            counters = sync.take_second()
            agg = None
            if sync.live:
                if not sync.bounded:
                    sync.book.prune(self.depth_prune_pct)
                if sync.book.crossed():
                    self._depth_resync(sync, sync.reset("crossed book"))
                else:
                    agg = sync.book.aggregate(BANDS_PCT, mark=self._last_mark.get(sync.symbol),
                                              contract_size=self._contract_size(sync.symbol))
                    if agg is None:
                        # live but a side is empty: nothing to measure, and no
                        # delta will refill a side the book has lost track of
                        self._depth_resync(sync, sync.reset("empty side"))
                    else:
                        self._book_measured(sync, agg, mono)
            if agg is None:
                state = "resync"
            elif counters["resynced"] or not agg["covered_bid_pct"] or not agg["covered_ask_pct"]:
                # resynced within the second, or price has left the range the
                # book is known to cover: the numbers exist but are not the
                # whole truth
                state = "degraded"
            elif mono - sync.last_applied_mono > self.depth_stale_s:
                state = "stale"
            else:
                state = "ok"
            not_ok += state != "ok"
            self._book_quality[sync.symbol].add(state != "ok")
            row = {
                "exchange": ex,
                "symbol": sync.symbol,
                "event_time": event_time,
                "ingest_time": ingest,
                "last_update_time": ch_ts(sync.last_event_ms),
                "seq_first": counters["seq_first"],
                "seq_last": counters["seq_last"],
                "n_updates": counters["n_updates"],
                # Depth beyond the snapshot is learned from deltas over time, so
                # the first minutes of a book understate it; this lets a reader
                # cut them out.
                "book_age_s": max(0, int(mono - sync.live_since_mono)) if sync.live else 0,
                "state": state,
                "collector_version": self.collector_version,
            }
            row.update(agg or empty_aggregate(BANDS_PCT))
            self.sink.write("market.book_agg", row)
        self._count_rows("book_agg", len(self.depth))
        self.sd.gauge(f"book_not_ok.{ex}", not_ok)
        if self._book_quality:
            self.sd.gauge(f"book_not_ok_pct.{ex}",
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

    # ------------------------------------------------------- config validation

    def _report_focus(self, status: dict[str, str], trading: str) -> None:
        """Log every focus symbol that does not exist or does not trade.

        A delisted or settling symbol is the quietest possible failure: it is
        spelled correctly, the subscription is accepted, and it simply never
        produces a row. Caught in the field with TONUSDT on Binance, whose
        status had moved to SETTLING - the collector looked perfectly healthy
        while silently covering 19 symbols instead of 20.
        """
        for sym in sorted(self.focus):
            st = status.get(sym)
            if st is None:
                self._event("config", "error", f"{sym} does not exist on {self.EXCHANGE}")
                log.error("focus symbol %s does not exist on this venue", sym)
            elif st != trading:
                self._event("config", "error", f"{sym} status={st}, expect no data")
                log.error("focus symbol %s is %s, not %s - it will produce no data",
                          sym, st, trading)
        tradable = sum(1 for s in self.focus if status.get(s) == trading)
        self._event("config", "subscribed", f"{tradable}/{len(self.focus)} focus symbols tradable")
        log.info("focus universe: %d/%d symbols tradable", tradable, len(self.focus))

    # -------------------------------------------------------------------- run

    def _primed_counters(self) -> list[str]:
        """Counters published at zero from the start, so their charts and the
        alarms watching them exist before anything goes wrong."""
        ex = self.EXCHANGE
        names = [f"rows_total.{ex}"]
        if self.depth:
            names += [f"rows.{ex}.book_agg", f"resyncs.{ex}.depth"]
        return names

    async def _startup(self) -> None:
        """Runs before any stream opens."""

    async def _shutdown(self) -> None:
        """Runs after every task has been cancelled."""

    def _coroutines(self) -> dict[str, Awaitable]:
        """The collector's long-running work, by task name."""
        raise NotImplementedError

    async def run(self) -> None:
        self.sd.prime(*self._primed_counters())
        await self._startup()
        work = self._coroutines()
        if self.depth:
            work["depth_tick"] = self._depth_tick_loop()
        tasks = [asyncio.create_task(c, name=n) for n, c in work.items()]
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in [*tasks, *self._bg]:
                t.cancel()
            await self._shutdown()
            if self.rest:
                await self.rest.close()
