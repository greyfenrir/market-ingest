import asyncio, json, logging, re, sys, time
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ingest.exchanges.hyperliquid import Hyperliquid, _why, next_hour_ms
from ingest.rest import HttpError, RateLimited

logging.disable(logging.CRITICAL)

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
    def __init__(self): self.gauges, self.counters = {}, {}
    def incr(self, name, value=1): self.counters[name] = self.counters.get(name, 0) + value
    def gauge(self, name, value): self.gauges[name] = value
    def prime(self, *names): pass

class FakeRest:
    """Answers (or raises) in order; records the bodies."""
    budget = 600
    def __init__(self, *answers): self.answers, self.bodies = list(answers), []
    def used(self): return 0
    async def post(self, path, body, **kw):
        self.bodies.append(body)
        a = self.answers.pop(0)
        if isinstance(a, Exception): raise a
        return a
    async def close(self): pass

def make(cfg=None, focus=("BTC", "ETH", "kPEPE")):
    sink = FakeSink()
    c = Hyperliquid(cfg or {}, sink, focus=list(focus), sd=FakeStatsd())
    return c, sink

def events(sink):
    return [(r["stream"], r["event"], r["detail"]) for r in sink.of("ops.connection_events")]

def columns(sql_file, table):
    sql = (ROOT / "schema" / sql_file).read_text()
    i = sql.index(f"CREATE TABLE IF NOT EXISTS {table}")
    body = sql[i: sql.index("ENGINE", i)]
    return set(re.findall(r"^\s+(\w+)\s+(?:LowCardinality|DateTime|Float|UInt|String)", body, re.M))

# Live answers of 2026-11-04, trimmed to a few coins.
BTC_CTX = {"funding": "-0.0000021449", "openInterest": "40113.83472", "prevDayPx": "77679.0",
           "dayNtlVlm": "5295169840.0874185562", "premium": "-0.0004983871", "oraclePx": "80660.2",
           "markPx": "80617.0", "midPx": "80619.5", "impactPxs": ["80619.0", "80620.0"],
           "dayBaseVlm": "67568.4142"}
ETH_CTX = {"funding": "0.0000125", "openInterest": "1038139.6994000002", "prevDayPx": "2298.7",
           "dayNtlVlm": "1817790637.3455004692", "premium": "-0.0003284477", "oraclePx": "2435.7",
           "markPx": "2434.8", "midPx": "2434.85", "impactPxs": ["2434.8", "2434.9"],
           "dayBaseVlm": "779579.4948000001"}
MATIC_CTX = {"funding": "0.0", "openInterest": "0.0", "prevDayPx": "0.37621", "dayNtlVlm": "0.0",
             "premium": None, "oraclePx": "0.3754", "markPx": "0.37621", "midPx": None,
             "impactPxs": None, "dayBaseVlm": "0.0"}
TSLA_CTX = {"funding": "0.00000625", "openInterest": "101209.722", "prevDayPx": "398.97",
            "dayNtlVlm": "132890758.4852799177", "premium": "0.0002171009", "oraclePx": "389.22",
            "markPx": "389.25", "midPx": "389.305", "impactPxs": ["389.269", "389.34"],
            "dayBaseVlm": "345214.8779999999"}
MAIN = [{"universe": [
            {"szDecimals": 5, "name": "BTC", "maxLeverage": 40, "marginTableId": 56},
            {"szDecimals": 4, "name": "ETH", "maxLeverage": 25, "marginTableId": 55},
            {"szDecimals": 1, "name": "MATIC", "maxLeverage": 20, "marginTableId": 20, "isDelisted": True},
         ], "marginTables": [], "collateralToken": 0},
        [BTC_CTX, ETH_CTX, MATIC_CTX]]
XYZ = [{"universe": [{"szDecimals": 3, "name": "xyz:TSLA", "maxLeverage": 10, "marginTableId": 10,
                      "onlyIsolated": True, "marginMode": "strictIsolated", "growthMode": "enabled"}],
        "marginTables": [], "collateralToken": 0},
       [TSLA_CTX]]
DEAD = [{"universe": [{"szDecimals": 2, "name": "flx:TSLA", "maxLeverage": 10, "isDelisted": True}],
         "marginTables": [], "collateralToken": 360},
        [MATIC_CTX]]
PERP_DEXS = [None, {"name": "xyz", "fullName": "XYZ"}, {"name": "flx", "fullName": "Felix Exchange"}]

RECV = 1_793_815_330_123      # 2026-11-04 18:02:10.123 UTC


print("== main DEX: one mark and one OI row per listed coin")
c, sink = make()
st = c._stat("asset_ctx")
check("listed coins counted", c._write_ctxs("", MAIN, RECV, st), 2)
marks, ois = sink.of("market.mark_price"), sink.of("market.open_interest")
check("delisted coin skipped", [r["symbol"] for r in marks], ["BTC", "ETH"])
btc = marks[0]
check("mark and oracle", (btc["mark_price"], btc["index_price"]), (80617.0, 80660.2))
check("funding is the hourly rate as sent", btc["funding_rate"], -0.0000021449)
check("funding interval 1 h", btc["funding_interval_h"], 1)
check("next funding is the next full hour", btc["next_funding_time"], "2026-11-04 19:00:00.000")
check("event_time is the arrival, no exchange time exists", (btc["event_time"], btc["ingest_time"]),
      ("2026-11-04 18:02:10.123", "2026-11-04 18:02:10.123"))
check("one row a minute: sampled", btc["sampled"], 1)
check("no settle price", btc["settle_price"], 0.0)
check("the other fields kept in extra", json.loads(btc["extra"]),
      {k: BTC_CTX[k] for k in ("prevDayPx", "dayNtlVlm", "premium", "midPx", "impactPxs", "dayBaseVlm")})
check("OI halved to one side, in coins", [(r["symbol"], r["open_interest"]) for r in ois],
      [("BTC", 40113.83472 / 2), ("ETH", 1038139.6994000002 / 2)])
check("raw two-sided OI kept in extra", json.loads(ois[0]["extra"]),
      {"openInterest": "40113.83472", "derived": "openInterest/2"})
check("row keys == market.mark_price columns", set(btc) ^ columns("002_market.sql", "market.mark_price"), set())
check("row keys == market.open_interest columns", set(ois[0]) ^ columns("002_market.sql", "market.open_interest"), set())
check("rows counted per table", (c.sd.counters["rows.hyperliquid.mark_price"],
                                 c.sd.counters["rows.hyperliquid.open_interest"]), (2, 2))
check("stream stats", (st.messages, st.rows), (1, 4))

print("== focus symbols checked once, against the main DEX, case-sensitive")
check("missing and delisted reported", [e for e in events(sink) if e[0] == "config"],
      [("config", "error", "kPEPE does not exist on hyperliquid"),
       ("config", "subscribed", "2/3 focus symbols tradable")])
c2, sink2 = make(focus=("BTC", "MATIC"))
c2._write_ctxs("", MAIN, RECV, c2._stat("asset_ctx"))
c2._write_ctxs("", MAIN, RECV, c2._stat("asset_ctx"))
check("delisted focus coin named; once only", [e for e in events(sink2) if e[0] == "config"],
      [("config", "error", "MATIC status=delisted, expect no data"),
       ("config", "subscribed", "1/2 focus symbols tradable")])
c3, sink3 = make(focus=("kPEPE",))
check("names keep their case", c3.focus, {"kPEPE"})

print("== a broken answer writes nothing")
c, sink = make()
st = c._stat("asset_ctx")
short = [MAIN[0], MAIN[1][:2]]
check("names and contexts of different length: no rows", c._write_ctxs("", short, RECV, st), 0)
check("and an error", events(sink)[-1], ("asset_ctx", "error", "main: 3 names, 2 contexts"))
check("nothing written", sink.of("market.mark_price"), [])
check("an answer of another shape: no rows", c._write_ctxs("xyz", None, RECV, st), 0)
check("and an error", events(sink)[-1][:2], ("asset_ctx", "error"))

print("== HIP-3: names with their DEX, dead DEXes skipped until the next refresh")
c, sink = make()
st = c._stat("hip3")
check("xyz rows", c._write_ctxs("xyz", XYZ, RECV, st), 1)
check("named with the DEX", sink.of("market.mark_price")[0]["symbol"], "xyz:TSLA")
check("deployer's scaled funding as sent", sink.of("market.mark_price")[0]["funding_rate"], 0.00000625)
check("a DEX with every market delisted is dead", (c._write_ctxs("flx", DEAD, RECV, st), c._dead), (0, {"flx"}))
check("HIP-3 rows do not check focus", [e for e in events(sink) if e[0] == "config"], [])

async def hip3_round(c):
    """One pass of _hip3_loop without its sleep."""
    real = asyncio.sleep
    async def stop(*a): raise asyncio.CancelledError
    asyncio.sleep = stop
    try: await c._hip3_loop()
    except asyncio.CancelledError: pass
    finally: asyncio.sleep = real

c, sink = make()
c.rest = FakeRest(PERP_DEXS, XYZ, DEAD)
asyncio.run(hip3_round(c))
check("perpDexs, then every DEX it lists", c.rest.bodies,
      [{"type": "perpDexs"}, {"type": "metaAndAssetCtxs", "dex": "xyz"},
       {"type": "metaAndAssetCtxs", "dex": "flx"}])
check("the list logged", events(sink)[0], ("hip3", "subscribed", "2 DEXes: xyz,flx"))
check("flx dead after one look", c._dead, {"flx"})

c, sink = make({"hip3_dexes": ["xyz"]})
c.rest = FakeRest(XYZ)
asyncio.run(hip3_round(c))
check("a configured list is used as is", c.rest.bodies, [{"type": "metaAndAssetCtxs", "dex": "xyz"}])

c, sink = make({"hip3_dexes": []})
work = c._coroutines()
check("hip3_dexes = [] runs no HIP-3 loop", sorted(work), ["asset_ctx"])
for w in work.values(): w.close()

print("== failures: logged, nothing written, the next poll tries again")
c, sink = make()
c.rest = FakeRest(HttpError(500, "null"))
check("a DEX that is gone: HTTP 500", asyncio.run(c._poll("gone", "hip3")), None)
check("logged", events(sink), [("hip3", "error", "metaAndAssetCtxs gone: HttpError: HTTP 500: null")])
check("a timeout has no text: the type alone", _why(TimeoutError()), "TimeoutError")
check("a 502 page: its first line only",
      _why(HttpError(502, "<html>\r\n<head><title>502 Bad Gateway</title></head>\r\n</html>")),
      "HttpError: HTTP 502: <html>")
c.rest = FakeRest(RateLimited("HTTP 429"))
check("rate limited: None", asyncio.run(c._poll("", "asset_ctx")), None)
check("no second event (the pause logs it)", len(events(sink)), 1)
c.rest = FakeRest(MAIN)
check("main poll asks without dex", (asyncio.run(c._poll("", "asset_ctx")), c.rest.bodies),
      (2, [{"type": "metaAndAssetCtxs"}]))

print("== a failed perpDexs is retried on the next round, not at the next refresh")
async def hip3_rounds(c, n):
    """n passes of _hip3_loop; its sleep ends the last one."""
    real, left = asyncio.sleep, [n]
    async def fake(*a):
        left[0] -= 1
        if left[0] == 0: raise asyncio.CancelledError
    asyncio.sleep = fake
    try: await c._hip3_loop()
    except asyncio.CancelledError: pass
    finally: asyncio.sleep = real

c, sink = make()
c.rest = FakeRest(TimeoutError("read"), PERP_DEXS, XYZ, DEAD, XYZ)
asyncio.run(hip3_rounds(c, 3))
check("asked again on the second round; after a success kept until the refresh", c.rest.bodies,
      [{"type": "perpDexs"},
       {"type": "perpDexs"}, {"type": "metaAndAssetCtxs", "dex": "xyz"},
       {"type": "metaAndAssetCtxs", "dex": "flx"},
       {"type": "metaAndAssetCtxs", "dex": "xyz"}])
check("the failure logged", events(sink)[0], ("hip3", "error", "perpDexs: TimeoutError: read"))

print("== stream_health: a poll is filed under its own minute; HIP-3 has no empty minutes")
def at(t): time.time = lambda: t
real_time = time.time
M = 29_896_922 * 60                      # a whole minute, 2026-11-04 18:02 UTC
try:
    c, sink = make()
    c.rest = FakeRest(MAIN, MAIN, XYZ)
    health = lambda: [(r["stream"], r["minute"], r["messages"]) for r in sink.of("ops.stream_health")]
    at(M + 30); c._roll("asset_ctx"); asyncio.run(c._poll("", "asset_ctx"))
    c._roll("hip3"); asyncio.run(c._poll("xyz", "hip3"))
    at(M + 90); c._roll("asset_ctx"); asyncio.run(c._poll("", "asset_ctx"))
    check("first poll: minute M, one message", health(), [("asset_ctx", "2026-11-04 18:02:00", 1)])
    at(M + 150); c._roll("asset_ctx")
    check("HIP-3 not closed by the main loop", [h[0] for h in health()], ["asset_ctx", "asset_ctx"])
    at(M + 330); c._roll("hip3")
    check("HIP-3 closed on its own round, under the minute of its poll", health()[-1],
          ("hip3", "2026-11-04 18:02:00", 1))
finally:
    time.time = real_time

print("== an unreadable coin is skipped, not the DEX, and nothing raises")
c, sink = make()
st = c._stat("asset_ctx")
broken = [{"universe": [{"name": "BTC"}, {"name": "ETH"}, {"szDecimals": 1}, None, {"name": "SOL"}]},
          [BTC_CTX, None, ETH_CTX, ETH_CTX, dict(ETH_CTX, markPx="n/a")]]
check("only the good coin written", c._write_ctxs("", broken, RECV, st), 1)
check("as a pair", ([r["symbol"] for r in sink.of("market.mark_price")],
                    [r["symbol"] for r in sink.of("market.open_interest")]), (["BTC"], ["BTC"]))
err = events(sink)[-1]
check("one error naming the skipped ones", (err[:2], err[2].startswith("main: 4 unreadable, skipped: ETH (")),
      (("asset_ctx", "error"), True))
check("a nameless coin by position", "#2 (" in err[2] and "#3 (" in err[2] and "SOL (ValueError" in err[2], True)
check("a universe that is not a list", c._write_ctxs("", [{"universe": None}, []], RECV, st), 0)
check("a DEX whose coins are all unreadable is not dead",
      (c._write_ctxs("xyz", [XYZ[0], [None]], RECV, st), c._dead), (0, set()))
c, sink = make()
c.rest = FakeRest(MAIN)
def boom(*a): raise RuntimeError("bug")
c._write_ctxs = boom
check("anything else from an answer: logged, poll returns None", asyncio.run(c._poll("", "asset_ctx")), None)
check("logged", events(sink), [("asset_ctx", "error", "metaAndAssetCtxs main: RuntimeError: bug")])

print("== perpDexs without any DEX: a failure, the list kept, asked again")
for answer in (None, {}, [], [None], {"name": "xyz"}, "xyz"):
    c, sink = make()
    c._dexes = ["xyz"]
    c.rest = FakeRest(answer)
    check(f"perpDexs {answer!r}: failed", asyncio.run(c._refresh_dexes()), False)
    check(f"perpDexs {answer!r}: list kept, error logged", (c._dexes, events(sink)[-1][:2]),
          (["xyz"], ("hip3", "error")))
c, sink = make()
c.rest = FakeRest(None, PERP_DEXS, XYZ, DEAD)
asyncio.run(hip3_rounds(c, 2))
check("perpDexs null at start: the loop survives and asks again", c.rest.bodies[:2],
      [{"type": "perpDexs"}, {"type": "perpDexs"}])

print("== a market without one of its numbers: skipped, never written as 0")
for key, value in (("markPx", None), ("markPx", ""), ("oraclePx", None),
                   ("funding", None), ("openInterest", None)):
    c, sink = make()
    ctx = dict(BTC_CTX, **{key: value})
    n = c._write_ctxs("", [MAIN[0], [ctx, ETH_CTX, MATIC_CTX]], RECV, c._stat("asset_ctx"))
    check(f"{key}={value!r}: BTC skipped, ETH written",
          (n, [r["symbol"] for r in sink.of("market.mark_price")],
           [r["symbol"] for r in sink.of("market.open_interest")]), (1, ["ETH"], ["ETH"]))
    check(f"{key}={value!r}: named in the error", f"BTC (ValueError: no {key})" in events(sink)[-1][2], True)
c, sink = make()
del_ctx = {k: v for k, v in BTC_CTX.items() if k != "markPx"}
c._write_ctxs("", [MAIN[0], [del_ctx, ETH_CTX, MATIC_CTX]], RECV, c._stat("asset_ctx"))
check("a missing key too", [r["symbol"] for r in sink.of("market.mark_price")], ["ETH"])

print("== hip3_dexes: \"all\", a list of names or [], anything else stops the start")
for good in ("all", [], ["xyz", "para"]):
    c, _ = make({"hip3_dexes": good})
    check(f"hip3_dexes={good!r} accepted", c.hip3_dexes, good)
for bad in ("xyz", "none", "", "ALL", ["xyz", 3], [""], {"xyz": 1}, True):
    try: make({"hip3_dexes": bad}); got = None
    except ValueError as e: got = str(e)
    check(f"hip3_dexes={bad!r} refused", got is not None and "hip3_dexes" in got, True)

print("== HIP-3 leaves the last request of each window to the main DEX")
class BudgetRest(FakeRest):
    """Counts weight per window; a sleep of the HIP-3 wait starts a new window."""
    budget = 100
    def __init__(self, *answers):
        super().__init__(*answers); self.spent, self.windows = 0, [[]]
    def used(self): return self.spent
    def window_left(self): return 30.0
    async def post(self, path, body, **kw):
        self.spent += kw["weight"]; self.windows[-1].append(body.get("dex", body["type"]))
        return await super().post(path, body, **kw)

c, sink = make()
five = [None] + [{"name": d} for d in ("a", "b", "c", "d", "e")]
c.rest = BudgetRest(five, XYZ, XYZ, XYZ, XYZ, XYZ)
slept = []
real = asyncio.sleep
async def fake_sleep(d):
    if d > 100: raise asyncio.CancelledError     # the end of the HIP-3 round
    slept.append(round(d, 1)); c.rest.spent = 0; c.rest.windows.append([])
asyncio.sleep = fake_sleep
try: asyncio.run(c._hip3_loop())
except asyncio.CancelledError: pass
finally: asyncio.sleep = real
check("waits for the next window instead of spending the last request", slept, [30.1])
check("four requests, room for the main DEX's fifth; the rest next window", c.rest.windows,
      [["perpDexs", "a", "b", "c"], ["d", "e"]])
check("every window keeps one request free", max(20 * len(w) for w in c.rest.windows) <= 100 - 20, True)

c, sink = make({"hip3_dexes": ["a"]})
c.rest = BudgetRest(XYZ); c.rest.spent = 80
waited = []
async def note(d): waited.append(d); c.rest.spent = 0
asyncio.sleep = note
try: asyncio.run(c._poll("a", "hip3"))
finally: asyncio.sleep = real
check("80 of 100 spent: a HIP-3 poll waits", waited, [30.1])
c, sink = make()
c.rest = BudgetRest(MAIN); c.rest.spent = 80
check("the main DEX takes that last request at once", asyncio.run(c._poll("", "asset_ctx")), 2)
c, sink = make({"hip3_dexes": ["a"]})
c.rest = BudgetRest(XYZ); c.rest.budget = 20
check("a budget of one request: HIP-3 still goes in an empty window", asyncio.run(c._poll("a", "hip3")), 1)

print("== next full hour")
check("inside an hour", next_hour_ms(3_600_000 * 5 + 1), 3_600_000 * 6)
check("exactly on the hour: the next one", next_hour_ms(3_600_000 * 5), 3_600_000 * 6)

print()
if fails:
    print(f"{len(fails)} FAILED"); sys.exit(1)
print("all passed")
