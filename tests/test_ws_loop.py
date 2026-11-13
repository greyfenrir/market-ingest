"""The shared WebSocket loop: reconnect backoff, limit answers to the
handshake, and messages a handler cannot take. The connection, the clock and
the pauses are fakes, so the test runs in a moment."""

import asyncio, sys, time, types
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from websockets.datastructures import Headers
from websockets.exceptions import InvalidStatus
from websockets.http11 import Response

from ingest.exchanges import base
from ingest.exchanges.base import Collector, backoff_delay, handshake_limit

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
    def events(self, kind=None):
        return [r for t, r in self.rows if t == "ops.connection_events"
                and (kind is None or r["event"] == kind)]


class FakeStatsd:
    def __init__(self): self.counts = {}
    def incr(self, name, n=1): self.counts[name] = self.counts.get(name, 0) + n
    def gauge(self, *a, **k): pass


class Clock:
    t = 1000.0
    def monotonic(self): return self.t

clock = Clock()
# The loop's own clock only; asyncio keeps the real one.
base.time = types.SimpleNamespace(monotonic=clock.monotonic, time=time.time,
                                  strftime=time.strftime, gmtime=time.gmtime)
# StreamStats measures silence by the same fake clock.
import ingest.health as health
health.time = types.SimpleNamespace(monotonic=clock.monotonic, time=time.time,
                                    strftime=time.strftime, gmtime=time.gmtime)
base.random = types.SimpleNamespace(random=lambda: 0.5)   # jitter factor 1.0


class Advance:
    """A message slot that only moves the clock on."""
    def __init__(self, s): self.s = s


class FakeWS:
    """Hands out `msgs`, then lives `life` more seconds and drops."""
    def __init__(self, msgs=(), life=0.0):
        self.msgs, self.life = list(msgs), life
    async def recv(self):
        while self.msgs:
            m = self.msgs.pop(0)
            if isinstance(m, Advance):
                clock.t += m.s
                continue
            return m
        clock.t += self.life
        raise ConnectionResetError("dropped by the venue")
    async def send(self, m): pass


class FakeConnect:
    def __init__(self, what): self.what = what
    async def __aenter__(self):
        if isinstance(self.what, Exception):
            raise self.what
        return self.what
    async def __aexit__(self, *a): return False


class C(Collector):
    EXCHANGE = "test"
    def __init__(self, cfg=None):
        self.sink = FakeSink()
        self.sd = FakeStatsd()
        super().__init__(cfg or {}, self.sink, focus=[], sd=self.sd)
        self.repairs = []
    def _on_bad_message(self, stream, payload):
        self.repairs.append(payload)


def run_loop(plan, handle=lambda *a: None, reconnects=None, cfg=None, stream="s", c=None):
    """Run _ws_loop over `plan` (one FakeWS or exception per connect).
    Returns (collector, pauses asked for, connect kwargs)."""
    c = c or C(cfg)
    plan = list(plan)
    pauses, kwargs = [], []
    stop_after = len(plan) if reconnects is None else reconnects

    def connect(url, **kw):
        kwargs.append(kw)
        return FakeConnect(plan.pop(0))

    real_sleep = asyncio.sleep
    async def sleep(s, *a):
        pauses.append(s)
        clock.t += s
        if len(pauses) >= stop_after:
            raise asyncio.CancelledError
        await real_sleep(0)

    async def go():
        base.websockets.connect, real_connect = connect, base.websockets.connect
        asyncio.sleep = sleep
        try:
            await c._ws_loop(stream, "wss://x", handle)
        except asyncio.CancelledError:
            pass
        finally:
            asyncio.sleep = real_sleep
            base.websockets.connect = real_connect
    asyncio.run(go())
    return c, pauses, kwargs


print("== backoff: doubles from 0.5 s with jitter, capped at 60 s")
check("attempt 1, jitter extremes", (backoff_delay(1, 0.0), backoff_delay(1, 0.999999) < 1.5), (0.5, True))
check("doubles", [backoff_delay(a, 0.5) for a in range(1, 7)], [1.0, 2.0, 4.0, 8.0, 16.0, 32.0])
check("cap holds at the top of the jitter", backoff_delay(7, 0.999999), 60.0)
check("cap is reached, not ~48 s", backoff_delay(6, 0.999999) > 47.9 and backoff_delay(7, 0.0) == 32.0, True)
check("huge attempt numbers do not overflow", backoff_delay(10_000, 0.5), 60.0)

print("== a venue that accepts and at once drops every connection")
c, pauses, kwargs = run_loop([FakeWS(life=0) for _ in range(9)])
check("pause grows to the cap", pauses, [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0, 60.0])
check("ping defaults kept", (kwargs[0]["ping_interval"], kwargs[0]["ping_timeout"]), (20.0, 20.0))

print("== connections that drop after a short life still back off")
c, pauses, _ = run_loop([FakeWS(life=30) for _ in range(4)])
check("30 s is not a success", pauses, [1.0, 2.0, 4.0, 8.0])

print("== a connection that lasted starts the backoff over")
c, pauses, _ = run_loop([FakeWS(life=0), FakeWS(life=0), FakeWS(life=0),
                         FakeWS(life=120), FakeWS(life=0)])
check("grows, then back to the first pause", pauses, [1.0, 2.0, 4.0, 1.0, 2.0])
c, pauses, _ = run_loop([FakeWS(life=0), FakeWS(life=0), FakeWS(life=10)],
                        cfg={"reconnect_reset_after_seconds": 5,
                             "ws_ping_interval_seconds": 10, "ws_ping_timeout_seconds": 30})
check("threshold from the config", pauses, [1.0, 2.0, 1.0])
_, _, kwargs = run_loop([FakeWS(life=0)], cfg={"ws_ping_interval_seconds": 10,
                                               "ws_ping_timeout_seconds": 30})
check("ping from the config", (kwargs[0]["ping_interval"], kwargs[0]["ping_timeout"]), (10.0, 30.0))

print("== a handshake refused with a limit status waits it out")
def refused(status, headers=()):
    return InvalidStatus(Response(status, "x", Headers(list(headers))))
check("429 with Retry-After", handshake_limit(refused(429, [("Retry-After", "120")])), (429, 120.0))
check("418 without: the default", handshake_limit(refused(418)), (418, 60.0))
check("403 (Bybit's ban)", handshake_limit(refused(403)), (403, 60.0))
check("a short Retry-After is not taken at its word", handshake_limit(refused(429, [("Retry-After", "1")])), (429, 60.0))
check("junk Retry-After", handshake_limit(refused(429, [("Retry-After", "soon")])), (429, 60.0))
check("other statuses: ordinary backoff", handshake_limit(refused(502)), None)
check("not a handshake error", handshake_limit(TimeoutError("timed out")), None)
c, pauses, _ = run_loop([refused(429, [("Retry-After", "300")]), refused(418), refused(502), FakeWS(life=0)])
check("pauses: Retry-After, 60 s floor, then backoff", pauses, [300.0, 60.0, 4.0, 8.0])
check("each limit recorded", [e["detail"] for e in c.sink.events("rate_limit")],
      ["handshake HTTP 429, next attempt in 300s or later",
       "handshake HTTP 418, next attempt in 60s or later"])

print("== a message the handler cannot take is skipped, the stream stays up")
seen = []
def handle(payload, recv_ms, nbytes, st):
    if "boom" in payload:
        raise KeyError("s")
    seen.append(payload)
msgs = ['{"ok":1}', 'not json', '{"boom":1}', '{"ok":2}', Advance(61),
        '{"boom":2}', '{"ok":3}']
c, pauses, _ = run_loop([FakeWS(msgs, life=120)], handle=handle)
check("good messages around the bad ones handled", seen, [{"ok": 1}, {"ok": 2}, {"ok": 3}])
check("one connection for all of them", len(c.sink.events("open")), 1)
check("every skip counted", c.sd.counts.get("bad_messages.test.s"), 3)
bad = c.sink.events("bad_message")
check("reported at most once a minute", len(bad), 2)
check("first report names the error",
      bad[0]["detail"].startswith("1 skipped since the last report; latest: JSONDecodeError"), True)
check("next report counts what it covers", bad[1]["detail"][:42],
      "2 skipped since the last report; latest: K")
check("the message is quoted", bad[1]["detail"].endswith('message: {"boom":2}'), True)
check("repair hook sees the decoded message, or None", c.repairs, [None, {"boom": 1}, {"boom": 2}])
check("then the ordinary close and reconnect", pauses, [1.0])

print("== a stream whose every message fails is still found stalled")
msgs = []
for _ in range(10):
    msgs += ['{"boom":1}', Advance(0.5)]       # a message every 0.5 s, never 5 s of quiet
c, pauses, _ = run_loop([FakeWS(msgs, life=0)], handle=handle, cfg={"stall_seconds": {"s": 3}})
stall = c.sink.events("stall")
check("stall reported", len(stall), 1)
check("as a stall of usable messages", stall and stall[0]["detail"].startswith("no usable messages"), True)
check("long before the messages ran out", c.sd.counts.get("bad_messages.test.s"), 8)

print("== a failing repair hook does not end the stream either")
class Broken(C):
    def _on_bad_message(self, stream, payload): raise RuntimeError("repair broke")
c = Broken()
c._bad_message("s", "x", None, ValueError("v"))
check("still counted", c.sd.counts.get("bad_messages.test.s"), 1)

print("== only the trade stream's reconnect tags the next trade gap")
c = C()
c.seq.check("BTC", 100)
run_loop([FakeWS(life=0)], stream="mark_price", c=c)
check("another stream's reconnect: an ordinary gap", c.seq.check("BTC", 105), (4, False))
run_loop([FakeWS(life=0)], stream="agg_trade", c=c)
check("the trade stream's own: across a reconnect", c.seq.check("BTC", 110), (4, True))

print("== repairs of one book after bad messages: 1, 2, 4 ... 60 s apart")
c = C()
holds = [(t, c._repair_hold("X", t)) for t in (0.0, 0.5, 1.0, 2.0, 3.0, 4.0, 7.0)]
check("first at once, then waits that double", holds,
      [(0.0, 0.0), (0.5, 0.5), (1.0, 0.0), (2.0, 1.0), (3.0, 0.0), (4.0, 3.0), (7.0, 0.0)])
check("each book on its own", c._repair_hold("Y", 7.5), 0.0)
t = 7.0
for _ in range(10):                  # a book that keeps breaking, repaired as soon as allowed
    t += 0.001
    t += c._repair_hold("X", t)
    c._repair_hold("X", t)
check("never more than 60 s apart", round(c._repair_hold("X", t + 0.001), 3), 59.999)
check("starts over after five quiet minutes",
      (c._repair_hold("X", t + 300), c._repair_hold("X", t + 300.5)), (0.0, 0.5))

print("== bad_messages counters exist from the start")
class D(C):
    WS_STREAMS = ("a", "b")
check("one per WebSocket stream", [n for n in D()._primed_counters() if n.startswith("bad_messages")],
      ["bad_messages.test.a", "bad_messages.test.b"])
check("none for a collector without WebSockets",
      [n for n in C()._primed_counters() if n.startswith("bad_messages")], [])

print()
print("ALL PASSED" if not fails else f"FAILURES: {fails}")
raise SystemExit(1 if fails else 0)
