import asyncio, sys, time, types
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
try:
    import websockets  # noqa: F401
except ModuleNotFoundError:  # only imported at module level, never used here
    sys.modules["websockets"] = types.ModuleType("websockets")

from ingest.exchanges.binance_um import BinanceUM
from ingest.health import StreamStats, ch_ts

fails = []
def check(name, got, want):
    if got == want: print(f"  PASS  {name}")
    else: print(f"  FAIL  {name}: got {got!r}, want {want!r}"); fails.append(name)


class FakeSink:
    insert_errors = dropped_rows = 0
    def __init__(self): self.rows = []
    def write(self, table, row): self.rows.append((table, row))
    def queued_rows(self): return 0
    def unknown_columns(self): return 0
    def of(self, table): return [r for t, r in self.rows if t == table]


class FakeStatsd:
    def incr(self, *a, **k): pass
    def gauge(self, *a, **k): pass


def make(cfg=None, focus=("BTCUSDT",)):
    sink = FakeSink()
    return BinanceUM(cfg or {}, sink, focus=list(focus), sd=FakeStatsd()), sink


def stats(c, stream):
    return c._stat(stream)


MIN = 60_000
T0 = 1_789_029_900_000 // MIN * MIN  # start of a minute, epoch ms

print("== mark price: focus at full rate, the rest thinned to one per minute")
c, sink = make()
def tick(recv_ms, sym, price):
    it = {"e": "markPriceUpdate", "E": recv_ms - 5, "s": sym, "p": str(price),
          "i": "0", "P": "0", "r": "0.0001", "T": recv_ms + 3_600_000}
    c._handle_mark([it], recv_ms, 100, stats(c, "mark_price"))
for s in range(3):
    tick(T0 + s * 1000, "BTCUSDT", 100 + s)
    tick(T0 + s * 1000, "ETHUSDT", 10 + s)
rows = sink.of("market.mark_price")
check("focus symbol written every tick",  [r["symbol"] for r in rows], ["BTCUSDT"] * 3)
check("focus rows are sampled=0",         {r["sampled"] for r in rows}, {0})
tick(T0 + MIN, "BTCUSDT", 103)
tick(T0 + MIN, "ETHUSDT", 13)
eth = [r for r in sink.of("market.mark_price") if r["symbol"] == "ETHUSDT"]
check("one ETH row after minute rolls",   len(eth), 1)
check("thinned row is sampled=1",         eth[0]["sampled"], 1)

print("== one bad entry in the markPrice array costs only itself")
c, sink = make(focus=("BTCUSDT", "ETHUSDT", "SOLUSDT"))
def entry(sym, price):
    return {"e": "markPriceUpdate", "E": T0 - 5, "s": sym, "p": price,
            "i": "0", "P": "0", "r": "0", "T": T0}
st = stats(c, "mark_price")
c._handle_mark([entry("BTCUSDT", "1"), entry("ETHUSDT", "bad"), entry("SOLUSDT", "3")], T0, 100, st)
check("the entries around it written",
      [r["symbol"] for r in sink.of("market.mark_price")], ["BTCUSDT", "SOLUSDT"])
check("the stream still counts as alive", st.rows, 2)
before = st.rows
c._handle_mark([entry("ETHUSDT", "bad")], T0 + 1000, 100, st)
check("nothing usable: the silence is left for the watchdog", st.rows, before)

print("== agg trade: forward id jump is logged as a gap, trade still stored")
c, sink = make()
def agg(agg_id, recv_ms=T0):
    p = {"e": "aggTrade", "E": recv_ms - 2, "s": "BTCUSDT", "a": agg_id, "p": "100.5",
         "q": "0.1", "f": agg_id, "l": agg_id, "T": recv_ms - 3, "m": True}
    c._handle_agg(p, recv_ms, 80, stats(c, "agg_trade"))
agg(100); agg(101)
check("contiguous ids: no gap rows", sink.of("ops.sequence_gaps"), [])
agg(105)
gap = sink.of("ops.sequence_gaps")[0]
check("gap counts missing ids",   (gap["expected_id"], gap["received_id"], gap["missing"]), (101 + 1, 105, 3))
check("in-session gap stream",    gap["stream"], "agg_trade")
check("all trades still stored",  len(sink.of("market.trades")), 3)
c.seq.mark_reconnect()
agg(110)
check("gap across reconnect is tagged",
      sink.of("ops.sequence_gaps")[1]["stream"], "agg_trade/reconnect")

print("== watchdog: silence beyond stall_seconds ends the session")
class SilentWS:
    """recv() times out `n` times, then fails the test loop out."""
    def __init__(self, n): self.n = n
    async def recv(self):
        if self.n == 0:
            raise RuntimeError("test escape")
        self.n -= 1
        raise asyncio.TimeoutError
def consume(cfg, silent_for, timeouts):
    c, sink = make(cfg)
    st = stats(c, "agg_trade")
    st.last_msg_monotonic = time.monotonic() - silent_for
    async def go():
        try:
            await c._consume(SilentWS(timeouts), "agg_trade", st, lambda *a: None, time.monotonic())
            return "returned"
        except RuntimeError:
            return "kept waiting"
    return asyncio.run(go()), [r["event"] for r in sink.of("ops.connection_events")]
res, events = consume({"stall_seconds": {"agg_trade": 30}}, silent_for=60, timeouts=1)
check("silent past limit: reconnects", res, "returned")
check("stall event recorded",          events, ["stall"])
res, events = consume({"stall_seconds": {"agg_trade": 30}}, silent_for=5, timeouts=2)
check("silent within limit: waits on", res, "kept waiting")
check("no stall event",                events, [])

print("== gap backfill: the trades the stream skipped are fetched over REST")
from ingest.rest import HttpError, RateLimited

class FakeRest:
    """aggTrades from a store of ids lo..hi; `script` items are answered
    first: an exception to raise, or None for a normal answer."""
    budget = 1200
    def __init__(self, lo, hi, script=(), used=0):
        self.lo, self.hi, self.script = lo, hi, list(script)
        self.calls, self._used = [], used
    def used(self): return self._used
    def window_left(self): return 7.0
    async def get(self, path, params=None, *, weight=1, stream="", what="", still_wanted=None):
        self.calls.append((path, dict(params), weight))
        if self.script:
            s = self.script.pop(0)
            if s is not None:
                raise s
        self._used += weight
        start = max(params["fromId"], self.lo)
        ids = range(start, min(self.hi, start + params["limit"] - 1) + 1)
        return [{"a": a, "p": "100.5", "q": "0.1", "f": a, "l": a, "T": T0 + a, "m": False}
                for a in ids]

def backfill(c, sym, first, last):
    pauses = []
    async def go():
        real = asyncio.sleep
        async def sleep(s, *a):
            pauses.append(s)
            c.rest._used = 0            # the budget window turned over
            await real(0)
        asyncio.sleep = sleep
        try:
            return await c._backfill_gap(sym, first, last)
        finally:
            asyncio.sleep = real
    return asyncio.run(go()), pauses

def backfill_events(sink):
    return [r["detail"] for r in sink.of("ops.connection_events") if r["event"] == "backfill"]

c, sink = make()
agg(100); agg(101); agg(105)
check("a gap is queued with its exact ids", c._gaps.get_nowait(), ("BTCUSDT", 102, 104))
c.rest = FakeRest(0, 10**6)
got, _ = backfill(c, "BTCUSDT", 102, 104)
rows = sink.of("market.trades")[3:]
check("missing trades written", ([r["agg_id"] for r in rows], got), ([102, 103, 104], 3))
check("asked from the first missing id, no more than needed",
      c.rest.calls, [("/fapi/v1/aggTrades", {"symbol": "BTCUSDT", "fromId": 102, "limit": 3}, 20)])
check("marked as fetched over REST", {r["extra"] for r in rows}, {'{"src":"rest"}'})
check("event time from the trade", rows[0]["event_time"], ch_ts(T0 + 102))
check("reported", backfill_events(sink), ["BTCUSDT 102..104: 3/3 recovered in 1 requests"])

c, sink = make()
c.rest = FakeRest(0, 10**6)
got, _ = backfill(c, "BTCUSDT", 1000, 3499)
check("a long gap in pages of 1000",
      [(p["fromId"], p["limit"]) for _, p, _ in c.rest.calls], [(1000, 1000), (2000, 1000), (3000, 500)])
ids = [r["agg_id"] for r in sink.of("market.trades")]
check("every id once, nothing beyond the gap", (got, ids == list(range(1000, 3500))), (2500, True))

c, sink = make()
c.rest = FakeRest(0, 101)       # Binance has nothing past 101
got, _ = backfill(c, "BTCUSDT", 100, 104)
check("what exists is written, then it stops", (got, len(c.rest.calls)), (2, 2))
check("the shortfall is reported", backfill_events(sink), ["BTCUSDT 100..104: 2/5 recovered in 2 requests"])

c, sink = make({"backfill_max_trades_per_gap": 1000})
c.rest = FakeRest(0, 10**6)
got, _ = backfill(c, "BTCUSDT", 1, 5000)
check("a gap over the limit is not fetched", (got, c.rest.calls), (0, []))
check("and says so", backfill_events(sink), ["BTCUSDT 1..5000: 5000 trades, over the limit of 1000; not recovered"])

c, sink = make()
c.rest = FakeRest(0, 10**6, script=[HttpError(500, "x"), RateLimited("HTTP 429"), None])
got, pauses = backfill(c, "BTCUSDT", 10, 11)
check("errors and limits are waited out", (got, len(c.rest.calls), pauses), (2, 3, [2]))
c, sink = make()
c.rest = FakeRest(0, 10**6, script=[HttpError(500, "x")] * 5)
got, pauses = backfill(c, "BTCUSDT", 10, 11)
check("gives up after 5 failures in a row", (got, len(c.rest.calls), pauses), (0, 5, [2, 4, 8, 16]))
check("giving up is reported", backfill_events(sink)[0].startswith("BTCUSDT 10..11: 0/2 recovered, gave up at 10"), True)

c, sink = make()
c.rest = FakeRest(0, 10**6, used=590)
got, pauses = backfill(c, "BTCUSDT", 10, 11)
check("leaves half the budget to snapshots: waits for the next window", pauses, [7.1])
check("then fetches", got, 2)

c, sink = make({"backfill_queue_max": 2})
for i in range(3):
    c._queue_backfill("BTCUSDT", i * 10, i * 10 + 5)
check("a full queue refuses, and says so",
      (c._gaps.qsize(), backfill_events(sink)), (2, ["BTCUSDT 20..25: queue full (2), not recovered"]))

c, sink = make({"backfill_gaps": False})
agg(100); agg(105)
work = c._coroutines()
check("off: nothing queued, no worker", (c._gaps.qsize(), "agg_backfill" in work), (0, False))
for co in work.values():
    co.close()
c, _ = make()
work = c._coroutines()
check("on by default", "agg_backfill" in work, True)
for co in work.values():
    co.close()

print("== a depth message that broke halfway starts its book over")
from ingest.exchanges.binance_um_depth import LIVE, SYNCING
c, sink = make({"depth": {"enabled": True}})
sync = c.depth["BTCUSDT"]
sync.state = LIVE
c._on_bad_message("depth", {"e": "depthUpdate", "s": "BTCUSDT"})
check("book reset", sync.state, SYNCING)
check("resync recorded", [r["detail"] for r in sink.of("ops.connection_events")], ["BTCUSDT: bad message"])
c._on_bad_message("depth", {"s": "BTCUSDT"})
c._on_bad_message("depth", None)
c._on_bad_message("agg_trade", {"s": "BTCUSDT"})
check("a book already waiting, an undecodable message, another stream: untouched",
      len(sink.of("ops.connection_events")), 1)

print("== a book that keeps getting bad messages is repaired 1, 2, 4 ... s apart")
import ingest.exchanges.binance_um as bm
clock = [500.0]
real_time = bm.time
bm.time = types.SimpleNamespace(**{**{k: getattr(time, k) for k in dir(time) if not k.startswith("_")},
                                   "monotonic": lambda: clock[0]})
c, sink = make({"depth": {"enabled": True}})
sync = c.depth["BTCUSDT"]
c._depth_open = True
def requested():
    async def go():
        n = c._depth_request_snapshots()
        for t in list(c._bg):
            t.cancel()
        await asyncio.gather(*c._bg, return_exceptions=True)
        sync.snapshot_pending = False
        return n
    return asyncio.run(go())
sync.state = LIVE
c._on_bad_message("depth", {"s": "BTCUSDT"})
sync.buffer.append({"U": 1, "u": 2, "pu": 0})
check("first repair: a snapshot at once", requested(), 1)
sync.state = LIVE
clock[0] += 0.25
c._on_bad_message("depth", {"s": "BTCUSDT"})
sync.buffer.append({"U": 1, "u": 2, "pu": 0})
check("second within a second: the snapshot waits", requested(), 0)
clock[0] += 0.8
check("and goes once the pause is over", requested(), 1)
check("every repair still recorded",
      len([r for r in sink.of("ops.connection_events") if r["event"] == "resync"]), 2)
bm.time = real_time

print("== a buffered event the replay cannot take: the book starts over, the task lives")
class CountingStatsd(FakeStatsd):
    def __init__(self): self.counts = {}
    def incr(self, name, n=1): self.counts[name] = self.counts.get(name, 0) + n
class SnapRest:
    budget = 1200
    def __init__(self): self.calls = 0
    async def get(self, path, params=None, **kw):
        self.calls += 1
        return {"lastUpdateId": 10, "bids": [["100", "1"]], "asks": [["101", "1"]]}
sink = FakeSink()
c = BinanceUM({"depth": {"enabled": True}}, sink, focus=["BTCUSDT"], sd=CountingStatsd())
c.rest = SnapRest()
c._depth_open = True
sync = c.depth["BTCUSDT"]
sync.buffer = [{"U": 9, "u": 11, "pu": 8, "b": [["100", "oops"]], "a": []}]
pauses = []
async def fetch():
    real = asyncio.sleep
    async def sleep(s, *a):
        pauses.append(s)
        await real(0)
    asyncio.sleep = sleep
    try:
        await c._fetch_snapshot(sync)
    finally:
        asyncio.sleep = real
asyncio.run(fetch())
check("counted as a bad message", c.sd.counts.get("bad_messages.binance_um.depth"), 1)
check("resync recorded",
      [r["detail"][:30] for r in sink.of("ops.connection_events") if r["event"] == "resync"],
      ["BTCUSDT: bad buffered event: V"])
check("waited, then a fresh snapshot", (pauses, c.rest.calls), ([1.0], 2))

print("== a trade that cannot become a row is left missing, and backfilled")
c, sink = make()
agg(100)
bad = {"e": "aggTrade", "E": T0, "s": "BTCUSDT", "a": 101, "p": None, "q": "0.1",
       "f": 101, "l": 101, "T": T0, "m": True}
try:
    c._handle_agg(bad, T0, 80, stats(c, "agg_trade"))
except Exception:
    pass
agg(102)
check("the trades around it stored", [r["agg_id"] for r in sink.of("market.trades")], [100, 102])
check("its id is a gap", [(g["expected_id"], g["missing"]) for g in sink.of("ops.sequence_gaps")], [(101, 1)])
check("queued for the backfill", c._gaps.get_nowait(), ("BTCUSDT", 101, 101))

print("== bad_messages counters primed for every stream")
c, _ = make({"depth": {"enabled": True}})
check("all four", sorted(n for n in c._primed_counters() if n.startswith("bad_messages")),
      ["bad_messages.binance_um.agg_trade", "bad_messages.binance_um.depth",
       "bad_messages.binance_um.force_order", "bad_messages.binance_um.mark_price"])

print("== proxy: reaches websockets.connect only when configured")
import ingest.exchanges.base as base
class _WS:
    async def recv(self): raise RuntimeError("dropped")
class _Conn:
    async def __aenter__(self): return _WS()
    async def __aexit__(self, *a): return False
def connect_kwargs(cfg):
    c, _ = make(cfg)
    seen = []
    real_connect = getattr(base.websockets, "connect", None)
    real_sleep = asyncio.sleep
    base.websockets.connect = lambda *a, **k: (seen.append(k), _Conn())[1]
    async def stop(*_): raise asyncio.CancelledError   # end the loop at the backoff
    asyncio.sleep = stop
    async def go():
        try: await c._ws_loop("agg_trade", "wss://x", lambda *a: None)
        except asyncio.CancelledError: pass
    try: asyncio.run(go())
    finally: base.websockets.connect, asyncio.sleep = real_connect, real_sleep
    return c, seen[0]
c, k = connect_kwargs({"proxy": "http://u:p@h:3128"})
check("ws: proxy passed", k.get("proxy"), "http://u:p@h:3128")
check("rest: same proxy", c.rest.proxy, "http://u:p@h:3128")
c, k = connect_kwargs({})
check("ws: no proxy kwarg without config", "proxy" in k, False)
check("rest: no proxy without config", c.rest.proxy, None)
c, k = connect_kwargs({"proxy": ""})
check("empty proxy counts as unset", ("proxy" in k, c.rest.proxy), (False, None))

print()
print("ALL PASSED" if not fails else f"FAILURES: {fails}")
raise SystemExit(1 if fails else 0)
