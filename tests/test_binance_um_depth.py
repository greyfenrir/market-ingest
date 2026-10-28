import asyncio, re, sys, time, types
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
try:
    import websockets  # noqa: F401
except ModuleNotFoundError:  # only imported at module level, never used here
    sys.modules["websockets"] = types.ModuleType("websockets")

from ingest.exchanges.binance_um import BinanceUM
from ingest.exchanges.binance_um_depth import ALIGNING, LIVE, SYNCING, DepthSync

fails = []
def check(name, got, want):
    if got == want: print(f"  PASS  {name}")
    else: print(f"  FAIL  {name}: got {got!r}, want {want!r}"); fails.append(name)


def ev(U, u, pu, b=(), a=(), sym="BTCUSDT", E=1_789_029_900_000):
    return {"e": "depthUpdate", "E": E, "T": E - 1, "s": sym,
            "U": U, "u": u, "pu": pu, "b": [list(x) for x in b], "a": [list(x) for x in a]}

SNAP = {"lastUpdateId": 100,
        "bids": [["99.9", "1"], ["99.6", "1"], ["99.0", "1"]],
        "asks": [["100.1", "1"], ["100.4", "1"], ["101.0", "1"]]}


print("== sequencing: buffer, snapshot, alignment (futures rule U <= L <= u)")
s = DepthSync("BTCUSDT")
check("starts syncing, no snapshot before any event", (s.state, s.needs_snapshot), (SYNCING, False))
s.on_event(ev(95, 98, 94))
check("first buffered event asks for one", s.needs_snapshot, True)
s.on_event(ev(99, 103, 98, b=[("99.9", "2")]))
s.on_event(ev(104, 107, 103, b=[("99.95", "1")]))
check("events buffered while syncing", len(s.buffer), 3)
s.snapshot_pending = True
check("pending snapshot is not requested twice", s.needs_snapshot, False)
check("buffer aligns with snapshot", s.on_snapshot(SNAP), None)
check("live after alignment", s.live, True)
check("book at last buffered id", s.book.last_update_id, 107)
check("event older than snapshot was dropped, spanning one applied",
      (s.book.bids[99.9], s.book.best_bid()), (2.0, 99.95))
check("buffer emptied", s.buffer, [])
check("pu chain continues", s.on_event(ev(108, 110, 107, a=[("100.1", "0")])), None)
check("delta applied live", s.book.best_ask(), 100.4)

print("== sequencing: what forces a resync")
s = DepthSync("BTCUSDT")
s.on_event(ev(105, 110, 104))
reason = s.on_snapshot(SNAP)
check("buffer starting after the snapshot is a gap", reason.startswith("gap after snapshot"), True)
check("gap leaves the book unsynced", (s.state, s.book.synced, s.buffer), (SYNCING, False, []))
check("generation bumped", s.generation, 1)
s.on_event(ev(111, 112, 110))
check("and the next event asks for a new snapshot", s.needs_snapshot, True)

s = DepthSync("BTCUSDT")
s.on_snapshot(SNAP)                                   # nothing buffered yet
check("snapshot without buffer waits to align", (s.state, s.live), (ALIGNING, False))
check("older event ignored while aligning", (s.on_event(ev(90, 99, 89)), s.state), (None, ALIGNING))
check("spanning event goes live", (s.on_event(ev(100, 104, 99)), s.state), (None, LIVE))
check("redelivered event is ignored", (s.on_event(ev(101, 104, 100)), s.state), (None, LIVE))
reason = s.on_event(ev(108, 110, 106))
check("pu mismatch is a sequence break", reason, "sequence break: pu=106, expected 104")
check("break discards the book", (s.state, s.book.synced, s.book.bids), (SYNCING, False, {}))

print("== sequencing: per-second counters")
s = DepthSync("BTCUSDT")
s.on_snapshot(SNAP)
s.on_event(ev(100, 104, 99)); s.on_event(ev(105, 106, 104))
check("counters for the second", s.take_second(),
      {"seq_first": 100, "seq_last": 106, "n_updates": 2, "resynced": False})
check("counters restart", s.take_second(),
      {"seq_first": 0, "seq_last": 0, "n_updates": 0, "resynced": False})
s.reset("test")
check("a reset marks the second", s.take_second()["resynced"], True)
s.reset("test again")                                 # already waiting for a snapshot
check("resetting a book that had none does not", s.take_second()["resynced"], False)

s = DepthSync("BTCUSDT")
s.on_snapshot(SNAP)
check("not live while aligning", s.just_live, False)
s.on_event(ev(100, 104, 99))
check("flags the moment it goes live", s.just_live, True)

print("== refresh: a shadow book is built alongside and swapped in without a gap")
FRESH = {"lastUpdateId": 105, "bids": [["99.95", "1"], ["99.9", "1"], ["98.0", "1"]],
         "asks": [["100.1", "1"], ["100.4", "1"], ["102.0", "1"]]}
s = DepthSync("BTCUSDT", depth_limit=3)               # 3-level snapshots are truncated
check("no refresh before live", s.start_refresh(0.0), False)
s.on_snapshot(SNAP); s.on_event(ev(100, 104, 99))
check("snapshot reach: bids trusted to 99.0", s.book.bid_floor, 99.0)
check("live book starts a refresh", s.start_refresh(5.0), True)
check("only one at a time", s.start_refresh(6.0), False)
sh = s.shadow
check("shadow waits for an event before its snapshot", sh.needs_snapshot, False)
s.on_event(ev(105, 106, 104, b=[("99.95", "1")]))
check("live book keeps applying", (s.live, s.book.best_bid()), (True, 99.95))
check("shadow buffered the same event", (len(sh.buffer), sh.needs_snapshot), (1, True))
check("shadow aligns with its buffer", sh.on_snapshot(FRESH), None)
check("and swaps in on the same update id", s.try_swap(), True)
check("the refreshed book reaches further", (s.shadow, s.book.bid_floor, s.just_refreshed), (None, 98.0, True))
check("live throughout, no reset", (s.live, s.book.last_update_id, s.resyncs), (True, 106, 0))

s = DepthSync("BTCUSDT", depth_limit=3)
s.on_snapshot(SNAP); s.on_event(ev(100, 104, 99))
s.start_refresh(0.0)
s.on_event(ev(105, 106, 104))
s.shadow.on_snapshot({**FRESH, "lastUpdateId": 107})   # newer than anything buffered
check("shadow ahead of the live book waits", (s.try_swap(), s.shadow.state), (False, ALIGNING))
s.on_event(ev(107, 108, 106))
check("next event aligns it and swaps", (s.shadow, s.just_refreshed, s.book.last_update_id), (None, True, 108))

s = DepthSync("BTCUSDT", depth_limit=3)
s.on_snapshot(SNAP); s.on_event(ev(100, 104, 99))
s.start_refresh(0.0); sh = s.shadow
s.on_event(ev(105, 106, 104))
check("a shadow that cannot align starts over", sh.on_snapshot({**FRESH, "lastUpdateId": 100}).startswith("gap"), True)
check("the live book does not notice", (s.live, s.shadow is sh, s.resyncs), (True, True, 0))
s.reset("pu break")
check("a reset drops the shadow", s.shadow, None)

class Tiny(DepthSync):
    MAX_BUFFER = 3
s = Tiny("BTCUSDT")
for i in range(5):
    s.on_event(ev(i * 10, i * 10 + 9, i * 10 - 1))
check("buffer is capped, oldest dropped", [e["U"] for e in s.buffer], [20, 30, 40])


# ------------------------------------------------------------------ connector

class FakeSink:
    insert_errors = dropped_rows = 0
    def __init__(self): self.rows = []
    def write(self, table, row): self.rows.append((table, row))
    def queued_rows(self): return 0
    def of(self, table): return [r for t, r in self.rows if t == table]

class FakeStatsd:
    def incr(self, *a, **k): pass
    def gauge(self, *a, **k): pass

def make(depth=None):
    sink = FakeSink()
    cfg = {"depth": {"enabled": True, "symbols": ["BTCUSDT"], **(depth or {})}}
    return BinanceUM(cfg, sink, focus=["BTCUSDT", "ETHUSDT"], sd=FakeStatsd()), sink

def feed(c, payload):
    c._handle_depth(payload, payload["E"] + 5, 200, c._stat("depth"))

def events(sink):
    return [(r["event"], r["detail"]) for r in sink.of("ops.connection_events")]

SEC = 1_789_029_900

print("== connector: off unless enabled")
check("no books by default", BinanceUM({}, FakeSink(), focus=["BTCUSDT"], sd=FakeStatsd()).depth, {})
c, _ = make({"symbols": "focus"})
check("'focus' takes the focus universe", sorted(c.depth), ["BTCUSDT", "ETHUSDT"])

print("== connector: a row every second, valid or not")
c, sink = make()
c._emit_book_rows(SEC, time.monotonic())
row = sink.of("market.book_agg")[-1]
check("no snapshot yet: state resync", row["state"], "resync")
check("zeros, not missing", (row["best_bid"], row["depth_bid_usd_1"], row["band_valid_bid_0p2"]), (0.0, 0.0, 0))
check("event_time is the second", row["event_time"], "2026-09-10 08:45:00")

sync = c.depth["BTCUSDT"]
sync.on_snapshot({**SNAP, "bids": SNAP["bids"][:2], "asks": SNAP["asks"][:2]})  # shorter than limit: complete
feed(c, ev(100, 104, 99, b=[("99.9", "3")]))
c._emit_book_rows(SEC + 1, time.monotonic())
row = sink.of("market.book_agg")[-1]
check("live book: state ok", row["state"], "ok")
check("figures from the book", (row["best_bid"], row["best_bid_qty"], row["best_ask"]), (99.9, 3.0, 100.1))
check("update ids of the second", (row["seq_first"], row["seq_last"], row["n_updates"]), (100, 104, 1))
c._last_mark["BTCUSDT"] = 200.0
c._emit_book_rows(SEC + 2, time.monotonic())
check("depth priced at mark", sink.of("market.book_agg")[-1]["depth_bid_usd_1"], 4 * 200.0)
c._emit_book_rows(SEC + 3, time.monotonic() + 60)
check("no delta for too long: stale", sink.of("market.book_agg")[-1]["state"], "stale")

print("== connector: resyncs are logged and visible in the rows")
feed(c, ev(110, 112, 109))
check("sequence break logged", events(sink)[-1], ("resync", "BTCUSDT: sequence break: pu=109, expected 104"))
c._emit_book_rows(SEC + 4, time.monotonic())
check("broken book: state resync", sink.of("market.book_agg")[-1]["state"], "resync")
sync.on_snapshot({**SNAP, "lastUpdateId": 200})
feed(c, ev(200, 201, 199))
c._emit_book_rows(SEC + 5, time.monotonic())
check("recovered in a later second: ok", sink.of("market.book_agg")[-1]["state"], "ok")
feed(c, ev(205, 206, 204))                             # break ...
sync.on_snapshot({**SNAP, "lastUpdateId": 300})
feed(c, ev(300, 301, 299))                             # ... and recovery, same second
c._emit_book_rows(SEC + 6, time.monotonic())
check("resynced within the second: degraded", sink.of("market.book_agg")[-1]["state"], "degraded")
c._emit_book_rows(SEC + 7, time.monotonic())
check("next clean second: ok", sink.of("market.book_agg")[-1]["state"], "ok")

feed(c, ev(302, 302, 301, b=[("100.5", "1")]))        # bid above the best ask
c._emit_book_rows(SEC + 8, time.monotonic())
check("crossed book is resynced", events(sink)[-1], ("resync", "BTCUSDT: crossed book"))
check("and not aggregated", sink.of("market.book_agg")[-1]["state"], "resync")

feed(c, ev(5, 6, 4, sym="XRPUSDT"))
check("symbol without a book is ignored", len(c.depth), 1)

print("== connector: reconnect starts every live book over")
c, sink = make()
c._depth_on_open()
check("first connect: nothing to report", events(sink), [])
c.depth["BTCUSDT"].on_snapshot(SNAP); feed(c, ev(100, 104, 99))
c._depth_on_open()
check("live book reset on reconnect", (events(sink)[-1], c.depth["BTCUSDT"].state),
      (("resync", "BTCUSDT: reconnect"), SYNCING))

print("== connector: snapshot fetch obeys 429 and drops stale responses")
class Resp:
    def __init__(self, status, body=None, headers=None, on_json=None):
        self.status, self.body, self.headers, self.on_json = status, body, headers or {}, on_json
    async def json(self):
        if self.on_json: self.on_json()
        return self.body
    async def text(self): return "nope"
    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False
class Rest:
    def __init__(self, *resps): self.resps, self.calls = list(resps), 0
    def get(self, url, params=None):
        self.calls += 1
        return self.resps.pop(0)

def fetch(c, rest):
    async def go():
        real = asyncio.sleep
        asyncio.sleep = lambda *_: real(0)      # skip backoff and Retry-After waits
        try:
            c._rest, c._snap_sem = rest, asyncio.Semaphore(1)
            c._depth_open = True
            await c._fetch_snapshot(c.depth["BTCUSDT"])
        finally:
            asyncio.sleep = real
    asyncio.run(go())

c, sink = make()
c.depth["BTCUSDT"].on_event(ev(99, 103, 98))
fetch(c, Rest(Resp(429, headers={"Retry-After": "7"}), Resp(200, SNAP)))
check("429 logged with its wait", events(sink)[0], ("rate_limit", "BTCUSDT HTTP 429, retry after 7s"))
check("then synced", (events(sink)[-1][0], c.depth["BTCUSDT"].live), ("synced", True))

c, sink = make()
sync = c.depth["BTCUSDT"]
rest = Rest(Resp(200, SNAP, on_json=lambda: sync.reset("reconnect")))
fetch(c, rest)
check("response for a reset book is discarded", (sync.state, sync.book.synced, rest.calls), (SYNCING, False, 1))

c, sink = make()
c.depth["BTCUSDT"].on_event(ev(150, 160, 149))       # buffer starts after the snapshot
fetch(c, Rest(Resp(500), Resp(200, SNAP), Resp(200, {**SNAP, "lastUpdateId": 155})))
check("error, misaligned, then loaded (live on the next event)",
      [e for e, _ in events(sink)], ["error", "resync", "snapshot"])

print("== connector: snapshots wait for an open stream")
def scheduler_pass(c):
    started = []
    async def fake_fetch(sync, owner=None):
        started.append(sync.symbol if owner is None else sync.symbol + "/shadow")
    c._fetch_snapshot = fake_fetch
    async def go():
        n = c._depth_request_snapshots()
        await asyncio.sleep(0)                  # let the started tasks run
        return n
    return asyncio.run(go()), started

c, sink = make()
c.depth["BTCUSDT"].on_event(ev(99, 103, 98))   # something buffered, but no stream
check("closed stream: nothing requested", scheduler_pass(c), (0, []))
c._depth_on_open()
check("open resets the book, so still nothing to align", scheduler_pass(c), (0, []))
c.depth["BTCUSDT"].on_event(ev(99, 103, 98))
check("open and buffering: requested", scheduler_pass(c), (1, ["BTCUSDT"]))
check("pending: not requested twice", scheduler_pass(c), (0, []))
c._depth_on_close()
c.depth["BTCUSDT"].reset("test"); c.depth["BTCUSDT"].on_event(ev(99, 103, 98))
check("closed again: nothing requested", scheduler_pass(c), (0, []))

c, sink = make()
c.depth["BTCUSDT"].on_event(ev(99, 103, 98))
rest = Rest(Resp(200, SNAP))
async def fetch_closed():
    c._rest, c._snap_sem = rest, asyncio.Semaphore(1)
    await c._fetch_snapshot(c.depth["BTCUSDT"])  # _depth_open is False
asyncio.run(fetch_closed())
check("a fetch gives up while the stream is closed", (rest.calls, c.depth["BTCUSDT"].live), (0, False))

print("== connector: synced is logged when the book goes live")
c, sink = make()
c.depth["BTCUSDT"].on_snapshot(SNAP)
check("loading a snapshot is not yet synced", events(sink), [])
feed(c, ev(100, 104, 99))
check("the spanning event is", events(sink), [("synced", "BTCUSDT u=104")])

print("== connector: refresh when the snapshot's reach wears thin")
c, sink = make({"snapshot_limit": 3, "refresh_min_seconds": 60})
sync = c.depth["BTCUSDT"]
c._depth_open = True
sync.on_snapshot(SNAP); feed(c, ev(100, 104, 99))
M0 = 1000.0
c._emit_book_rows(SEC, M0)
check("first aggregate records the reach", sync.cover0, (1.0, 1.0))
# the market lifts: asks up to 100.4 are taken, a bid appears at 100.6
feed(c, ev(105, 105, 104, b=[("100.6", "1")], a=[("100.1", "0"), ("100.4", "0"), ("100.7", "1")]))
c._emit_book_rows(SEC + 1, M0 + 1)
check("ask side under half its reach: refresh starts",
      (events(sink)[-1][0], events(sink)[-1][1].split(":")[0], sync.shadow is not None),
      ("refresh", "BTCUSDT started", True))
check("rows stay ok meanwhile", sink.of("market.book_agg")[-1]["state"], "ok")
feed(c, ev(106, 106, 105))                            # the shadow's first buffered event
check("scheduler fetches for the shadow", scheduler_pass(c), (1, ["BTCUSDT/shadow"]))

c, sink = make({"snapshot_limit": 3, "refresh_min_seconds": 60})
sync = c.depth["BTCUSDT"]
sync.on_snapshot(SNAP); feed(c, ev(100, 104, 99))
c._emit_book_rows(SEC, M0)
feed(c, ev(105, 105, 104, b=[("100.6", "1")], a=[("100.1", "0"), ("100.4", "0"), ("100.7", "1")]))
c._emit_book_rows(SEC + 1, M0 + 1)
feed(c, ev(106, 106, 105))
FRESH2 = {"lastUpdateId": 106, "bids": [["100.6", "1"], ["100.0", "1"], ["99.5", "1"]],
          "asks": [["100.7", "1"], ["101.0", "1"], ["102.5", "1"]]}
async def fetch_shadow():
    real = asyncio.sleep
    asyncio.sleep = lambda *_: real(0)
    try:
        c._rest, c._snap_sem, c._depth_open = Rest(Resp(200, FRESH2)), asyncio.Semaphore(1), True
        await c._fetch_snapshot(sync.shadow, sync)
    finally:
        asyncio.sleep = real
asyncio.run(fetch_shadow())
check("swapped in, logged as refresh done", events(sink)[-1], ("refresh", "BTCUSDT done: u=106"))
check("no resync anywhere", [e for e, _ in events(sink) if e == "resync"], [])
check("reach renewed", (sync.shadow, sync.book.ask_ceil), (None, 102.5))
c._emit_book_rows(SEC + 2, M0 + 2)
row = sink.of("market.book_agg")[-1]
check("next row ok, ask coverage back", (row["state"], round(row["covered_ask_pct"], 2)), ("ok", 1.84))
check("reach measured afresh", sync.cover0 is not None, True)

sync.cover0 = (50.0, 50.0)                            # pretend it is worn again at once
c._emit_book_rows(SEC + 3, M0 + 10)
check("not again within refresh_min_seconds", sync.shadow, None)
sync.cover0 = (50.0, 50.0)
c._emit_book_rows(SEC + 4, M0 + 100)
check("but after it", sync.shadow is not None, True)

print("== ws loop: on_open and on_close bracket every attempt")
import ingest.exchanges.binance_um as bu
class WS:
    async def recv(self): raise RuntimeError("dropped")
class Conn:
    def __init__(self, fail): self.fail = fail
    async def __aenter__(self):
        if self.fail: raise OSError("refused")
        return WS()
    async def __aexit__(self, *a): return False
def one_attempt(fail):
    c, sink = make()
    calls = []
    real_connect, real_sleep = getattr(bu.websockets, "connect", None), asyncio.sleep
    bu.websockets.connect = lambda *a, **k: Conn(fail)
    async def stop(*_): raise asyncio.CancelledError   # end the loop at the backoff
    asyncio.sleep = stop
    async def go():
        try:
            await c._ws_loop("depth", "wss://x", lambda *a: None,
                             on_open=lambda: calls.append("open"),
                             on_close=lambda: calls.append("close"))
        except asyncio.CancelledError:
            pass
    try:
        asyncio.run(go())
    finally:
        bu.websockets.connect, asyncio.sleep = real_connect, real_sleep
    return calls
check("connected then dropped", one_attempt(fail=False), ["open", "close"])
check("never connected: close only", one_attempt(fail=True), ["close"])

print("== schema: row keys match market.book_agg exactly")
# JSONEachRow rejects a whole batch over one unknown key, and the sink retries
# a rejected batch forever, so a drift here would stall the table.
sql = (ROOT / "schema" / "004_book.sql").read_text()
body = sql[sql.index("CREATE TABLE"): sql.index("ENGINE")]
columns = set(re.findall(r"^\s+(\w+)\s+(?:LowCardinality|DateTime|Float|UInt)", body, re.M))
c, sink = make()
c._emit_book_rows(SEC, time.monotonic())                  # zero row
c.depth["BTCUSDT"].on_snapshot(SNAP); feed(c, ev(100, 104, 99))
c._emit_book_rows(SEC + 1, time.monotonic())              # live row
rows = sink.of("market.book_agg")
check("zero row keys == columns", set(rows[0]) ^ columns, set())
check("live row keys == columns", set(rows[1]) ^ columns, set())

print()
print("ALL PASSED" if not fails else f"FAILURES: {fails}")
raise SystemExit(1 if fails else 0)
