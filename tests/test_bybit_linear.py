import asyncio, json, logging, re, sys, time
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import ingest.exchanges.base as base
from ingest.exchanges.bybit_linear import MAX_TOPICS, BybitLinear, subscribe_messages
from ingest.exchanges.bybit_linear_depth import LIVE, SYNCING, BybitBook
from ingest.rest import RateLimited

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
    def of(self, table): return [r for t, r in self.rows if t == table]

class FakeStatsd:
    def __init__(self): self.gauges, self.counters = {}, {}
    def incr(self, name, value=1): self.counters[name] = self.counters.get(name, 0) + value
    def gauge(self, name, value): self.gauges[name] = value
    def prime(self, *names): pass

def make(cfg=None, focus=("BTCUSDT", "ETHUSDT")):
    sink = FakeSink()
    c = BybitLinear({"depth": {"enabled": True, "symbols": ["BTCUSDT"], "levels": 3}, **(cfg or {})},
                    sink, focus=list(focus), sd=FakeStatsd())
    return c, sink

def events(sink):
    return [(r["stream"], r["event"], r["detail"]) for r in sink.of("ops.connection_events")]

def columns(sql_file, table):
    sql = (ROOT / "schema" / sql_file).read_text()
    i = sql.index(f"CREATE TABLE IF NOT EXISTS {table}")
    body = sql[i: sql.index("ENGINE", i)]
    return set(re.findall(r"^\s+(\w+)\s+(?:LowCardinality|DateTime|Float|UInt|String)", body, re.M))

T0 = 1_793_507_717_000


print("== trades: one row per trade, UUID kept, taker side mapped")
c, sink = make()
st = c._stat("agg_trade")
c._handle_trade({"topic": "publicTrade.BTCUSDT", "type": "snapshot", "ts": T0 + 2, "data": [
    {"T": T0, "s": "BTCUSDT", "S": "Buy", "v": "0.002", "p": "77772.00", "L": "PlusTick",
     "i": "097d7fdc-4e5e-508d-8fd8-712fdb82b94a", "BT": False, "RPI": False, "seq": 836395794566},
    {"T": T0 + 1, "s": "BTCUSDT", "S": "Sell", "v": "0.5", "p": "77771.90", "L": "MinusTick",
     "i": "66183234-c551-5e53-89ca-deee289baee7", "BT": False, "RPI": True, "seq": 836395794567},
]}, T0 + 50, 400, st)
rows = sink.of("market.trades")
check("two trades, two rows", len(rows), 2)
check("identity and order", (rows[0]["trade_id"], rows[0]["agg_id"], rows[0]["first_id"]),
      ("097d7fdc-4e5e-508d-8fd8-712fdb82b94a", 836395794566, 0))
check("taker buy: buyer is not the maker", rows[0]["is_buyer_maker"], 0)
check("taker sell: buyer is the maker", rows[1]["is_buyer_maker"], 1)
check("price and size", (rows[1]["price"], rows[1]["qty"]), (77771.9, 0.5))
check("unmodelled fields kept", json.loads(rows[1]["extra"]), {"L": "MinusTick", "BT": False, "RPI": True})
check("row keys == market.trades columns", set(rows[0]) ^ columns("002_market.sql", "market.trades"), set())
check("counted per stream and exchange",
      (c.sd.counters["rows.bybit_linear.agg_trade"], c.sd.counters["rows_total.bybit_linear"]), (2, 2))

print("== tickers: deltas merged, a timer writes one mark row per second")
c, sink = make()
st = c._stat("mark_price")
def tick(kind, ts, sym="BTCUSDT", **fields):
    c._handle_ticker({"topic": f"tickers.{sym}", "type": kind, "ts": ts,
                      "data": {"symbol": sym, **fields}}, ts + 5, 100, st)
S0 = T0 // 1000
tick("snapshot", T0, markPrice="77771.90", indexPrice="77807.25", fundingRate="0.0000474",
     nextFundingTime="1793520000000", bid1Price="77771.9")
tick("delta", T0 + 100, bid1Price="77772.0")
tick("delta", T0 + 200, markPrice="77772.10")
check("messages alone write no row", len(sink.of("market.mark_price")), 0)
check("mark kept for depth pricing", c._last_mark["BTCUSDT"], 77772.1)
check("pass writes a row for the symbol", c._mark_pass(S0 + 1, T0 + 1050), 1)
tick("delta", T0 + 1100, markPrice="77772.50")
c._mark_pass(S0 + 2, T0 + 2050)
tick("delta", T0 + 2100, bid1Price="77772.1")                # mark unchanged
c._mark_pass(S0 + 3, T0 + 3050)                              # nothing since T0+2100
c._mark_pass(S0 + 4, T0 + 4050)                              # carried forward
rows = sink.of("market.mark_price")
check("event_time is the second that ended", [r["event_time"] for r in rows][:2],
      ["2026-11-01 04:35:18.000", "2026-11-01 04:35:19.000"])
check("one row per second, from the merged state", [r["mark_price"] for r in rows],
      [77772.1, 77772.5, 77772.5, 77772.5])
check("index and funding read from the merged state", (rows[1]["index_price"], rows[1]["funding_rate"]),
      (77807.25, 0.0000474))
check("next funding time", rows[1]["next_funding_time"], "2026-11-01 08:00:00.000")
check("focus rows are not sampled", rows[1]["sampled"], 0)
check("extra empty while the state is fresh", [r["extra"] for r in rows][:3], ["", "", ""])
check("extra holds the last message time once carried", rows[3]["extra"], json.dumps({"ts": T0 + 2100}, separators=(",", ":")))
check("stream rows counted", st.rows, 4)
check("the timer is not a message (stall watch sees only the socket)", st.messages, 5)
check("row keys == market.mark_price columns", set(rows[0]) ^ columns("002_market.sql", "market.mark_price"), set())
check("rows_total counted", c.sd.counters["rows.bybit_linear.mark_price"], 4)
tick("snapshot", T0, sym="BTC-27MAR26", markPrice="80000", fundingRate="", nextFundingTime="")
c._mark_pass(S0 + 1, T0 + 1050)
check("a future's empty funding reads as 0", sink.of("market.mark_price")[-1]["funding_rate"], 0.0)
check("silent past the limit: no row", c._mark_pass(S0 + 3600, T0 + 3_600_050), 0)
c._mark_reset()
check("a closed socket leaves no state to carry", c._mark_pass(S0 + 1, T0 + 1050), 0)
tick("delta", T0 + 5000, markPrice="1")                      # a delta before any snapshot
check("a delta without a snapshot still seeds the state", c._mark_pass(S0 + 6, T0 + 6050), 1)

print("== liquidations: every one, side as the liquidation order's")
c, sink = make()
c._handle_liquidation({"topic": "allLiquidation.ETHUSDT", "type": "snapshot", "ts": T0, "data": [
    {"T": T0 - 10, "s": "ETHUSDT", "S": "Buy", "v": "1.5", "p": "2400.10"},
    {"T": T0 - 5, "s": "ETHUSDT", "S": "Sell", "v": "0.2", "p": "2420.00"},
]}, T0 + 30, 200, c._stat("force_order"))
rows = sink.of("market.liquidations")
check("long liquidated is a SELL, short a BUY", [r["side"] for r in rows], ["SELL", "BUY"])
check("size and bankruptcy price", (rows[0]["orig_qty"], rows[0]["cum_filled_qty"], rows[0]["price"]),
      (1.5, 1.5, 2400.1))
check("Bybit's own side kept verbatim", json.loads(rows[0]["extra"]), {"S": "Buy"})
check("row keys == market.liquidations columns",
      set(rows[0]) ^ columns("002_market.sql", "market.liquidations"), set())

print("== REST tickers: OI for focus, sampled mark for the rest")
c, sink = make()
c._write_tickers({"time": T0, "result": {"list": [
    # a live ticker of 2026-11-02: openInterest is both sides, single is one
    {"symbol": "BTCUSDT", "markPrice": "78040.1", "openInterest": "69080.633",
     "openInterestValue": "5390691307.61", "singleOpenInterest": "34540.317",
     "singleOpenInterestValue": "2695345692.82"},
    {"symbol": "PEPEUSDT", "markPrice": "0.00001", "openInterest": "1e12", "fundingRate": "0.0001"},
]}}, T0 + 40, "focus", c._stat("open_interest"))
check("OI only for focus, one side", [(r["symbol"], r["open_interest"]) for r in sink.of("market.open_interest")],
      [("BTCUSDT", 34540.317)])
check("Bybit's own OI figures kept", json.loads(sink.of("market.open_interest")[0]["extra"]),
      {"openInterest": "69080.633", "openInterestValue": "5390691307.61",
       "singleOpenInterestValue": "2695345692.82"})
marks = sink.of("market.mark_price")
check("non-focus mark, sampled", [(r["symbol"], r["sampled"]) for r in marks], [("PEPEUSDT", 1)])
check("row keys == market.open_interest columns",
      set(sink.of("market.open_interest")[0]) ^ columns("002_market.sql", "market.open_interest"), set())
c, sink = make()
c._write_tickers({"time": T0, "result": {"list": [{"symbol": "PEPEUSDT", "openInterest": "5"}]}},
                 T0, "all", c._stat("open_interest"))
rows = sink.of("market.open_interest")
check("'all' takes OI for every symbol", len(rows), 1)
check("no single-side field: half of both sides, flagged",
      (rows[0]["open_interest"], json.loads(rows[0]["extra"])),
      (2.5, {"openInterest": "5", "derived": "openInterest/2"}))
c, sink = make()
c._write_tickers({"time": T0, "result": {"list": [
    {"symbol": "BTCUSDT", "openInterest": "8", "singleOpenInterest": ""}]}},
    T0, "focus", c._stat("open_interest"))
check("empty single-side field counts as missing",
      sink.of("market.open_interest")[0]["open_interest"], 4.0)

print("== REST errors: retCode is checked, 10006 pauses everything")
class Rest:
    def __init__(self, *answers): self.answers, self.paused = list(answers), []
    async def get(self, path, params=None, **kw): return self.answers.pop(0)
    def pause(self, stream, what, status, headers): self.paused.append(status)
    async def close(self): pass
c, sink = make()
c.rest = Rest({"retCode": 10006, "retMsg": "Too many visits!"}, {"retCode": 10001, "retMsg": "bad"},
              {"retCode": 0, "result": {}})
async def get_once():
    try: return await c._get("/x", {}, stream="open_interest")
    except RateLimited: return "limited"
    except RuntimeError as e: return str(e)
check("10006 is a rate limit", (asyncio.run(get_once()), c.rest.paused), ("limited", [10006]))
check("other codes are errors", asyncio.run(get_once()), "retCode 10001: bad")
check("0 is the answer", asyncio.run(get_once()), {"retCode": 0, "result": {}})

print("== focus validation pages through instruments-info")
c, sink = make(focus=("BTCUSDT", "TONUSDT", "NOPEUSDT"))
c.rest = Rest({"retCode": 0, "result": {"list": [{"symbol": "BTCUSDT", "status": "Trading"}], "nextPageCursor": "p2"}},
              {"retCode": 0, "result": {"list": [{"symbol": "TONUSDT", "status": "Settling"}], "nextPageCursor": ""}})
asyncio.run(c._validate_focus())
ev = [e for e in events(sink) if e[0] == "config"]
check("both pages read, problems named",
      [d for _, _, d in ev], ["NOPEUSDT does not exist on bybit_linear", "TONUSDT status=Settling, expect no data",
                              "1/3 focus symbols tradable"])

print("== subscriptions: chunked, and the 100-topic limit enforced")
msgs = subscribe_messages("agg_trade", [f"publicTrade.S{i}" for i in range(23)])
check("chunks of 10", [len(json.loads(m)["args"]) for m in msgs], [10, 10, 3])
check("req_id names the stream", json.loads(msgs[0])["req_id"], "agg_trade")
try: make(focus=[f"S{i}USDT" for i in range(MAX_TOPICS + 1)]); got = None
except ValueError: got = "refused"
check("more than 100 focus symbols is refused", got, "refused")
c, sink = make()
check("data passes through", c._unwrap({"topic": "x"}), {"topic": "x"})
check("pong is swallowed", c._unwrap({"success": True, "ret_msg": "pong", "op": "ping"}), None)
check("failed subscribe is swallowed", c._unwrap({"success": False, "ret_msg": "bad topic",
                                                   "op": "subscribe", "req_id": "depth"}), None)
check("and reported", events(sink)[-1], ("depth", "error", "subscribe failed: bad topic"))

print("== BybitBook: snapshot on the socket, u + 1 or resync")
SNAP = {"topic": "orderbook.3.BTCUSDT", "type": "snapshot", "ts": T0, "cts": T0 - 2,
        "data": {"s": "BTCUSDT", "u": 100, "seq": 9000,
                 "b": [["99.9", "1"], ["99.6", "1"], ["99.0", "1"]],
                 "a": [["100.1", "1"], ["100.4", "1"], ["101.0", "1"]]}}
def delta(u, b=(), a=(), seq=9001):
    return {"topic": "orderbook.3.BTCUSDT", "type": "delta", "ts": T0, "cts": T0,
            "data": {"s": "BTCUSDT", "u": u, "seq": seq, "b": [list(x) for x in b], "a": [list(x) for x in a]}}
b = BybitBook("BTCUSDT", 3)
check("a delta before any snapshot is ignored", (b.on_message(delta(5)), b.state), (None, SYNCING))
check("snapshot makes it live", (b.on_message(SNAP, 10.0), b.state, b.just_live, b.live_since_mono),
      (None, LIVE, True, 10.0))
check("next u applies", (b.on_message(delta(101, b=[("99.95", "2")])), b.book.best_bid()), (None, 99.95))
check("a gap resets", (b.on_message(delta(103)), b.state), ("sequence break: u=103, expected 102", SYNCING))
check("and flags the second", b.take_second()["resynced"], True)
b.on_message(SNAP, 20.0)
check("a later snapshot is live again", b.state, LIVE)
b.on_message({**SNAP, "data": {**SNAP["data"], "u": 1}}, 30.0)
check("a snapshot with u=1 (Bybit restarted) replaces the book",
      (b.book.last_update_id, b.live_since_mono), (1, 20.0))
check("bounded: the collector does not prune it", BybitBook.bounded, True)

print("== BybitBook: complete out to the N-th level, nothing beyond")
b = BybitBook("BTCUSDT", 3)
b.on_message(SNAP)
b.on_message(delta(101, b=[("99.95", "1")]))               # 4 bids now; Bybit would drop 99.0
b.take_second()
check("full side: trusted to its N-th level", (sorted(b.book.bids), b.book.bid_floor), ([99.6, 99.9, 99.95], 99.6))
check("ask side full too", b.book.ask_ceil, 101.0)
b.on_message(delta(102, a=[("101.0", "0")]))                # Bybit refills from beyond; here it did not
b.take_second()
check("a side short of N is the whole side", b.book.ask_ceil, float("inf"))

print("== connector: depth rows, resync by resubscribing one topic")
c, sink = make()
sync = c.depth["BTCUSDT"]
st = c._stat("depth")
class WS:
    def __init__(self): self.sent = []
    async def send(self, m): self.sent.append(json.loads(m))
ws = WS()
c._ws["depth"] = ws
c._depth_on_open()
c._handle_depth(SNAP, T0 + 5, 300, st)
check("synced on the snapshot", events(sink)[-1], ("depth", "synced", "BTCUSDT u=100"))
c._last_mark["BTCUSDT"] = 100.0
c._emit_book_rows(T0 // 1000, time.monotonic())
row = sink.of("market.book_agg")[-1]
check("ok row", (row["state"], row["best_bid"], row["levels_bid"]), ("ok", 99.9, 3))
check("row keys == market.book_agg columns", set(row) ^ columns("004_book.sql", "market.book_agg"), set())
async def gap():
    c._handle_depth(delta(105), T0 + 6, 100, st)
    await asyncio.sleep(0)
asyncio.run(gap())
check("gap logged as resync", events(sink)[-1][:2], ("depth", "resync"))
check("one topic resubscribed", [(m["op"], m["args"]) for m in ws.sent],
      [("unsubscribe", ["orderbook.3.BTCUSDT"]), ("subscribe", ["orderbook.3.BTCUSDT"])])
async def watch(dt):
    n = c._depth_watch_pass(time.monotonic() + dt)
    await asyncio.sleep(0)
    return n
check("watchdog waits for that attempt", asyncio.run(watch(0)), 0)
check("then tries again", (asyncio.run(watch(60)), len(ws.sent)), (1, 4))
c._depth_on_close()
check("not while disconnected", asyncio.run(watch(120)), 0)

print("== base ws loop: subscribes after connect, pings on schedule")
class PingWS:
    def __init__(self): self.sent, self.n = [], 0
    async def send(self, m): self.sent.append(m)
    async def recv(self):
        self.n += 1
        if self.n > 3: raise RuntimeError("dropped")
        return json.dumps({"topic": "x", "n": self.n})
class Conn:
    def __init__(self, w): self.w = w
    async def __aenter__(self): return self.w
    async def __aexit__(self, *a): return False
c, sink = make()
w = PingWS()
got = []
real_connect, real_sleep = base.websockets.connect, asyncio.sleep
base.websockets.connect = lambda *a, **k: Conn(w)
async def stop(*_): raise asyncio.CancelledError
async def go():
    asyncio.sleep = stop
    try:
        await c._ws_loop("agg_trade", "wss://x", lambda p, *a: got.append(p["n"]),
                         subscribe=["SUB1", "SUB2"], app_ping=(0.0, "PING"))
    except asyncio.CancelledError:
        pass
    finally:
        asyncio.sleep = real_sleep
try:
    asyncio.run(go())
finally:
    base.websockets.connect = real_connect
check("subscriptions first, then a ping before each read", w.sent[:4], ["SUB1", "SUB2", "PING", "PING"])
check("every message handled", got, [1, 2, 3])
check("connection forgotten once closed", c._ws, {})

print()
print("ALL PASSED" if not fails else f"FAILURES: {fails}")
raise SystemExit(1 if fails else 0)
