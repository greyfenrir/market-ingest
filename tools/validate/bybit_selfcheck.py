"""Live self-check of the collector's Bybit book (BybitBook) on orderbook.1000.

Bybit's archive keeps 200 levels, which on BTC and ETH cover less than 0.05 %,
so the majors cannot be checked against it. This checks the code instead:

- connection A maintains a BybitBook per symbol exactly as the collector does
  (snapshot, deltas, trimmed to 1000 levels once a second);
- connection B, once per --every seconds per symbol, subscribes the same topic,
  builds a second BybitBook from Bybit's fresh snapshot and its deltas, and
  unsubscribes again;
- the moment both books stand on the same update id u, every one of the 1000
  levels of each side must be equal, price and quantity.

What it proves: deltas are applied in order, deletions and levels falling out
of the 1000 are handled, and nothing drifts over time. It runs the code in
src/, not the running collector, so after a change to BybitBook it checks the
change. Needs the project's dependencies (websockets).

    python tools/validate/bybit_selfcheck.py --minutes 15 \\
        --symbols BTCUSDT ETHUSDT SOLUSDT XRPUSDT DOGEUSDT HYPEUSDT
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import websockets  # noqa: E402

from ingest.exchanges.bybit_linear_depth import BybitBook  # noqa: E402

WS = "wss://stream.bybit.com/v5/public/linear"
LEVELS = 1000


def top(side: dict, n: int, reverse: bool) -> list[tuple[float, float]]:
    return sorted(side.items(), reverse=reverse)[:n]


def diff(a: BybitBook, b: BybitBook) -> int:
    """How many of the top LEVELS levels differ, both sides together."""
    bad = 0
    for name, rev in (("bids", True), ("asks", False)):
        x = top(getattr(a.book, name), LEVELS, rev)
        y = top(getattr(b.book, name), LEVELS, rev)
        bad += abs(len(x) - len(y)) + sum(1 for p, q in zip(x, y) if p != q)
    return bad


class Check:
    def __init__(self, symbols: list[str], every: float, timeout: float) -> None:
        self.symbols = symbols
        self.every, self.timeout = every, timeout
        self.a = {s: BybitBook(s, LEVELS) for s in symbols}
        self.b: dict[str, BybitBook] = {}          # symbols B is following now
        self.b_since: dict[str, float] = {}
        self.stats = defaultdict(lambda: defaultdict(int))
        self.examples: list[str] = []
        self.ws_b = None

    # -------------------------------------------------------------- books

    def on_a(self, sym: str, msg: dict) -> None:
        why = self.a[sym].on_message(msg, time.monotonic())
        if why:
            self.stats[sym]["A sequence breaks"] += 1
            self.examples.append(f"{sym} A: {why}")
        self.try_compare(sym)

    def on_b(self, sym: str, msg: dict) -> None:
        bk = self.b.get(sym)
        if bk is None:
            return
        why = bk.on_message(msg, time.monotonic())
        if why:
            self.stats[sym]["B sequence breaks"] += 1
        self.try_compare(sym)

    def try_compare(self, sym: str) -> None:
        a, b = self.a[sym], self.b.get(sym)
        if b is None or not (a.live and b.live):
            return
        if a.book.last_update_id != b.book.last_update_id:
            return
        n = diff(a, b)
        st = self.stats[sym]
        st["checks"] += 1
        if n == 0:
            st["equal"] += 1
        else:
            st["different"] += 1
            self.examples.append(f"{sym} u={a.book.last_update_id}: {n} levels differ")
        self.stop_b(sym)

    def stop_b(self, sym: str) -> None:
        self.b.pop(sym, None)
        self.b_since.pop(sym, None)
        if self.ws_b is not None:
            asyncio.ensure_future(self.ws_b.send(json.dumps(
                {"op": "unsubscribe", "args": [f"orderbook.{LEVELS}.{sym}"]})))

    # ---------------------------------------------------------- connections

    async def conn(self, name: str, until: float) -> None:
        async with websockets.connect(WS, ping_interval=20, max_size=1 << 24) as ws:
            if name == "A":
                await ws.send(json.dumps({"op": "subscribe",
                                          "args": [f"orderbook.{LEVELS}.{s}" for s in self.symbols]}))
            else:
                self.ws_b = ws
            last_ping = last_tick = time.monotonic()
            while time.monotonic() < until:
                now = time.monotonic()
                if now - last_ping > 20:
                    await ws.send('{"op":"ping"}')
                    last_ping = now
                if name == "A" and now - last_tick >= 1:     # the collector trims once a second
                    for bk in self.a.values():
                        if bk.live:
                            bk.take_second()
                    last_tick = now
                if name == "B":
                    await self.schedule_b(now)
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=1)
                except asyncio.TimeoutError:
                    continue
                msg = json.loads(raw)
                topic = msg.get("topic", "")
                if not topic.startswith("orderbook."):
                    continue
                sym = topic.rsplit(".", 1)[1]
                (self.on_a if name == "A" else self.on_b)(sym, msg)

    async def schedule_b(self, now: float) -> None:
        for sym, since in list(self.b_since.items()):
            if now - since > self.timeout:
                self.stats[sym]["not aligned in time"] += 1
                self.stop_b(sym)
        if self.b or not all(bk.live for bk in self.a.values()):
            return
        # one symbol at a time, round robin, each every `every` seconds
        due = [s for s in self.symbols if now - self.last_b.get(s, 0) >= self.every]
        if due:
            sym = due[0]
            self.last_b[sym] = now
            self.b[sym] = BybitBook(sym, LEVELS)
            self.b_since[sym] = now
            await self.ws_b.send(json.dumps({"op": "subscribe", "args": [f"orderbook.{LEVELS}.{sym}"]}))

    async def run(self, minutes: float) -> None:
        self.last_b: dict[str, float] = {}
        until = time.monotonic() + minutes * 60
        await asyncio.gather(self.conn("A", until), self.conn("B", until))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--symbols", nargs="+",
                   default=["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "HYPEUSDT"])
    p.add_argument("--minutes", type=float, default=15)
    p.add_argument("--every", type=float, default=60, help="seconds between checks of one symbol")
    p.add_argument("--timeout", type=float, default=15)
    a = p.parse_args()
    c = Check(a.symbols, a.every, a.timeout)
    asyncio.run(c.run(a.minutes))
    print(f"{'symbol':10} {'checks':>6} {'equal':>6} {'different':>9} {'not aligned':>11} "
          f"{'A breaks':>8} {'B breaks':>8}")
    bad = 0
    for s in a.symbols:
        st = c.stats[s]
        bad += st["different"] + st["A sequence breaks"]
        print(f"{s:10} {st['checks']:6} {st['equal']:6} {st['different']:9} "
              f"{st['not aligned in time']:11} {st['A sequence breaks']:8} {st['B sequence breaks']:8}")
    for x in c.examples[:10]:
        print("  ", x)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
