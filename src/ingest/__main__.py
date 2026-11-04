"""Entry point: one process per exchange.

    python -m ingest --config /etc/market-ingest/config.toml --exchange binance_um

One process per exchange, not per stream and not one for everything: a crash in
the Binance collector must not take Bybit down with it, and each gets its own
systemd unit, its own restart policy and its own journal.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import logging
import os
import signal
import sys
from pathlib import Path
try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib

from .chsink import ClickHouseSink
from .metrics import Statsd

# --exchange name -> "module:Class". Each name is also the config section, the
# systemd instance (market-ingest@<name>) and the `exchange` column. Imported
# on demand, so a broken connector cannot stop the others from starting.
EXCHANGES = {
    "binance_um": "ingest.exchanges.binance_um:BinanceUM",
    "bybit_linear": "ingest.exchanges.bybit_linear:BybitLinear",
}


def collector_class(name: str):
    module, cls = EXCHANGES[name].split(":")
    return getattr(importlib.import_module(module), cls)


def exchange_config(cfg: dict, name: str) -> dict:
    """The exchange's own section. Missing is an error, not an empty
    section: an instance enabled for an exchange nobody configured would
    otherwise start with no focus symbols and look healthy."""
    section = cfg.get(name)
    if not isinstance(section, dict):
        raise KeyError(f"no [{name}] section in the config")
    return section


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="ingest")
    p.add_argument("--config", default="/etc/market-ingest/config.toml", type=Path)
    p.add_argument("--exchange", required=True, choices=sorted(EXCHANGES))
    p.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"))
    return p


async def amain(args) -> int:
    cfg = tomllib.loads(args.config.read_text())
    ch = cfg.get("clickhouse", {})
    ing = cfg.get("ingest", {})
    try:
        ex_cfg = exchange_config(cfg, args.exchange)
    except KeyError as exc:
        logging.error("%s", exc.args[0])
        return 2

    logging.info("%s collector, version %s", args.exchange,
                 os.environ.get("COLLECTOR_VERSION") or "unset")

    password = os.environ.get("CH_PASSWORD")
    if not password:
        print("CH_PASSWORD is not set (expected from the systemd EnvironmentFile)",
              file=sys.stderr)
        return 2

    sink = ClickHouseSink(
        ch.get("url", "http://127.0.0.1:8123"),
        ch.get("user", "ingest"),
        password,
        batch_max_rows=ing.get("batch_max_rows", 5000),
        batch_max_seconds=ing.get("batch_max_seconds", 2.0),
        queue_max_rows=ing.get("queue_max_rows", 400_000),
    )
    await sink.start()
    try:
        await sink.ping()
    except Exception as exc:  # noqa: BLE001
        logging.error("ClickHouse is not reachable at %s: %s", ch.get("url"), exc)
        await sink.stop()
        return 3

    m = cfg.get("metrics", {})
    sd = Statsd(
        host=m.get("statsd_host", "127.0.0.1"),
        port=int(m.get("statsd_port", 8125)),
        prefix=m.get("prefix", "market_ingest"),
        enabled=bool(m.get("enabled", True)),
    )
    await sd.start()

    if not ex_cfg.get("enabled", True):
        logging.warning("%s is disabled in config, nothing to do", args.exchange)
        await sd.stop()
        await sink.stop()
        return 0

    collector = collector_class(args.exchange)(
        ex_cfg, sink, focus=ex_cfg.get("focus_symbols", []), sd=sd
    )

    loop = asyncio.get_running_loop()
    stopping = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stopping.set)

    task = asyncio.create_task(collector.run(), name=args.exchange)
    code = await run_until_stopped(task, stopping)
    # Drain the sink so the last couple of seconds of market data are not lost
    # to a routine restart.
    await sink.stop()
    await sd.stop()
    logging.info("inserted=%d dropped=%d insert_errors=%d",
                 sink.inserted_rows, sink.dropped_rows, sink.insert_errors)
    return code


async def run_until_stopped(task: asyncio.Task, stopping: asyncio.Event) -> int:
    """Wait for a stop signal or for the collector to end, whichever is first.

    A collector that ends on its own has failed: its streams are cancelled and
    nothing is being collected, yet the process would stay up and systemd would
    never restart it. Return a non-zero code so that it does.
    """
    stop_wait = asyncio.ensure_future(stopping.wait())
    try:
        await asyncio.wait({task, stop_wait}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        stop_wait.cancel()
    if task.done():
        if task.cancelled():
            logging.error("collector task was cancelled, exiting")
        elif task.exception() is not None:
            logging.error("collector crashed, exiting for a restart",
                          exc_info=task.exception())
        else:
            logging.error("collector returned unexpectedly, exiting for a restart")
        return 1
    logging.info("shutdown requested, draining")
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    return 0


def main() -> int:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    return asyncio.run(amain(args))


if __name__ == "__main__":
    raise SystemExit(main())
