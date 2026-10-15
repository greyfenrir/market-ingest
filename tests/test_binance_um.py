import asyncio, sys, time, types
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
try:
    import websockets  # noqa: F401
except ModuleNotFoundError:  # only imported at module level, never used here
    sys.modules["websockets"] = types.ModuleType("websockets")

from ingest.exchanges.binance_um import BinanceUM
from ingest.health import StreamStats

fails = []
def check(name, got, want):
    if got == want: print(f"  PASS  {name}")
    else: print(f"  FAIL  {name}: got {got!r}, want {want!r}"); fails.append(name)


class FakeSink:
    insert_errors = dropped_rows = 0
    def __init__(self): self.rows = []
    def write(self, table, row): self.rows.append((table, row))
    def queued_rows(self): return 0
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

print()
print("ALL PASSED" if not fails else f"FAILURES: {fails}")
raise SystemExit(1 if fails else 0)
