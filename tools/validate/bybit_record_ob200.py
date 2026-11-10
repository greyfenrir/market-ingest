"""Record Bybit's orderbook.200 feed in the archive's own format.

One JSON message a line, as Bybit sends it, into OUT/<day>_<SYM>_ob200.data -
the layout of quote-saver.bycsi.com's daily files. bybit_compare_ob200.py
--file reads it, so the comparison can be tried on minutes of fresh data
instead of waiting for the next day's archive. Needs websockets.

    python tools/validate/bybit_record_ob200.py --minutes 3 --symbols SOLUSDT XRPUSDT --out rec
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path

import websockets

WS = "wss://stream.bybit.com/v5/public/linear"


async def record(symbols: list[str], minutes: float, out: Path) -> dict[str, int]:
    out.mkdir(parents=True, exist_ok=True)
    files, counts = {}, {s: 0 for s in symbols}
    until = time.monotonic() + minutes * 60
    async with websockets.connect(WS, ping_interval=20, max_size=1 << 24) as ws:
        await ws.send(json.dumps({"op": "subscribe", "args": [f"orderbook.200.{s}" for s in symbols]}))
        last_ping = time.monotonic()
        while time.monotonic() < until:
            if time.monotonic() - last_ping > 20:
                await ws.send('{"op":"ping"}')
                last_ping = time.monotonic()
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=1)
            except asyncio.TimeoutError:
                continue
            msg = json.loads(raw)
            if not msg.get("topic", "").startswith("orderbook.200."):
                continue
            sym = msg["topic"].rsplit(".", 1)[1]
            if sym not in files:
                day = time.strftime("%Y-%m-%d", time.gmtime(msg["ts"] / 1000))
                files[sym] = open(out / f"{day}_{sym}_ob200.data", "w")
            files[sym].write(raw if raw.endswith("\n") else raw + "\n")
            counts[sym] += 1
    for f in files.values():
        f.close()
        print(f.name)
    return counts


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--symbols", nargs="+", default=["SOLUSDT", "XRPUSDT"])
    p.add_argument("--minutes", type=float, default=3)
    p.add_argument("--out", type=Path, default=Path("rec"))
    a = p.parse_args()
    print(asyncio.run(record(a.symbols, a.minutes, a.out)))


if __name__ == "__main__":
    main()
