"""Check collector rows against a fresh database and an upgraded database."""

from __future__ import annotations

import argparse
import importlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid


ROOT = Path(__file__).resolve().parents[1]
IMAGE = "clickhouse/clickhouse-server:25.8@sha256:0152dd511befe6a2c2ef53e930726179669b08116da78500b37c51c96ff5ee77"
MARKER = "schema-upgrade-baseline"


def command(args: list[str], *, data: str | None = None, timeout: int = 120) -> str:
    result = subprocess.run(args, input=data, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"Command failed: {' '.join(args)}\n{result.stderr or result.stdout}")
    return result.stdout


def export_ref(repo: Path, ref: str, destination: Path) -> str:
    revision = f"{ref}^{{commit}}"
    sha = command(["git", "-C", str(repo), "rev-parse", "--verify", "--end-of-options",
                   revision]).strip()
    archive = subprocess.check_output(["git", "-C", str(repo), "archive", sha, "schema", "src"])
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        for member in tar:
            path = Path(member.name)
            if path.is_absolute() or ".." in path.parts or not (member.isdir() or member.isfile()):
                raise RuntimeError(f"Unsupported archive entry: {member.name}")
            target = destination / path
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                with tar.extractfile(member) as source:
                    target.write_bytes(source.read())
    return sha


class CaptureSink:
    insert_errors = dropped_rows = 0

    def __init__(self) -> None:
        self.rows: list[dict] = []

    def write(self, table: str, row: dict) -> None:
        if table == "market.book_agg":
            self.rows.append(row)

    def queued_rows(self) -> int:
        return 0


class QuietStats:
    def incr(self, *args, **kwargs) -> None:
        pass

    def gauge(self, *args, **kwargs) -> None:
        pass


def collector_rows(source: Path) -> list[dict]:
    sys.path.insert(0, str(source / "src"))
    registry = importlib.import_module("ingest.__main__").EXCHANGES
    rows: list[dict] = []
    second, mono = 1_704_067_200, time.monotonic()
    for name, entry in sorted(registry.items()):
        if isinstance(entry, str):
            module, cls = entry.split(":")
            cls = getattr(importlib.import_module(module), cls)
        else:
            cls = entry
        sink = CaptureSink()
        collector = cls({"depth": {"enabled": True, "symbols": ["BTCUSDT"]}},
                        sink, focus=["BTCUSDT"], sd=QuietStats())
        collector._emit_book_rows(second, mono)
        bids, asks = [["99.9", "1"], ["99.0", "2"]], [["100.1", "1"], ["101.0", "2"]]
        if name == "binance_um":
            collector.depth["BTCUSDT"].on_snapshot({"lastUpdateId": 10, "bids": bids, "asks": asks})
            payload = {"e": "depthUpdate", "s": "BTCUSDT", "E": second * 1000,
                       "T": second * 1000, "U": 10, "u": 11, "pu": 9, "b": [], "a": []}
        elif name == "bybit_linear":
            payload = {"type": "snapshot", "topic": "orderbook.1000.BTCUSDT",
                       "ts": second * 1000, "cts": second * 1000,
                       "data": {"s": "BTCUSDT", "u": 10, "seq": 10, "b": bids, "a": asks}}
        else:
            raise RuntimeError(f"Add an order-book fixture for {name}")
        collector._handle_depth(payload, second * 1000 + 5, 200, collector._stat("depth"))
        collector._emit_book_rows(second + 1, time.monotonic())
        if len(sink.rows) != 2 or [r["state"] for r in sink.rows] != ["resync", "ok"]:
            raise RuntimeError(f"{name}: expected a resync row and a live row")
        rows.extend(sink.rows)
    if not rows:
        raise RuntimeError("No exchange collectors were tested")
    return rows


def emit_rows(source: Path, version: str) -> list[dict]:
    env = {**os.environ, "COLLECTOR_VERSION": version}
    result = subprocess.run([sys.executable, str(Path(__file__).resolve()), "--emit-rows",
                             "--source-dir", str(source)], text=True, capture_output=True,
                            env=env, timeout=30)
    if result.returncode:
        raise RuntimeError(f"Could not generate collector rows:\n{result.stderr}")
    return json.loads(result.stdout)


class ClickHouse:
    def __init__(self, image: str) -> None:
        self.image = image
        self.name = f"market-ingest-schema-{uuid.uuid4().hex[:12]}"

    def __enter__(self):
        command(["docker", "run", "--detach", "--rm", "--name", self.name,
                 "--network", "none", "--env", "CLICKHOUSE_SKIP_USER_SETUP=1",
                 "--ulimit", "nofile=262144:262144", self.image], timeout=300)
        deadline = time.monotonic() + 60
        try:
            while True:
                try:
                    self.query("SELECT 1")
                    break
                except RuntimeError:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.5)
        except BaseException:
            self.__exit__(*sys.exc_info())
            raise
        return self

    def __exit__(self, kind, value, tb):
        if kind:
            logs = subprocess.run(["docker", "logs", "--tail", "30", self.name],
                                  text=True, capture_output=True, timeout=10)
            print(logs.stdout + logs.stderr, file=sys.stderr)
        subprocess.run(["docker", "rm", "--force", "--volumes", self.name],
                       capture_output=True, timeout=30)

    def query(self, sql: str, data: str | None = None) -> str:
        return command(["docker", "exec", "-i", self.name, "clickhouse-client",
                        "--output_format_json_quote_64bit_integers=0", "--query", sql],
                       data=data, timeout=30)

    def apply(self, source: Path, extra: list[Path] | None = None) -> None:
        files = sorted((source / "schema").glob("*.sql"))
        if not files:
            raise RuntimeError(f"No schema files in {source}")
        for path in [*files, *(extra or [])]:
            print(f"  apply {path.name}", flush=True)
            command(["docker", "exec", "-i", self.name, "clickhouse-client", "--multiquery"],
                    data=path.read_text(), timeout=60)

    def insert(self, rows: list[dict]) -> None:
        description = self.query("DESCRIBE TABLE market.book_agg FORMAT JSONEachRow")
        columns = {json.loads(line)["name"] for line in description.splitlines()}
        missing = set().union(*(row.keys() for row in rows)) - columns
        if missing:
            raise RuntimeError("Collector fields missing from market.book_agg: "
                               f"{', '.join(sorted(missing))}. "
                               "Add an ALTER TABLE migration for the existing table.")
        body = "\n".join(json.dumps(row, separators=(",", ":")) for row in rows) + "\n"
        self.query("INSERT INTO market.book_agg FORMAT JSONEachRow", body)

    def stored_rows(self, keys: list[str]) -> list[dict]:
        projection = ", ".join(f"`{key}`" for key in keys)
        output = self.query(f"SELECT {projection} FROM market.book_agg "
                            "WHERE collector_version = 'schema-upgrade-target' "
                            "ORDER BY exchange, event_time FORMAT JSONEachRow")
        return [json.loads(line) for line in output.splitlines()]

    def count(self, where: str = "") -> int:
        return int(self.query(f"SELECT count() FROM market.book_agg {where}"))

    def baseline_rows(self) -> list[dict]:
        output = self.query("SELECT exchange, symbol, state, best_bid, best_ask, "
                            "toFloat64(depth_bid_usd_1) AS depth_bid_usd_1 "
                            f"FROM market.book_agg WHERE collector_version = '{MARKER}' "
                            "ORDER BY exchange, event_time FORMAT JSONEachRow")
        return [json.loads(line) for line in output.splitlines()]


def check_database(ch: ClickHouse, source: Path, rows: list[dict], extra: list[Path],
                   baseline: Path | None = None, old_rows: list[dict] | None = None) -> None:
    old_rows = old_rows or []
    previous = []
    if baseline is not None:
        ch.apply(baseline)
        ch.insert(old_rows)
        previous = ch.baseline_rows()
    ch.apply(source, extra)
    ch.apply(source, extra)
    if baseline is not None and ch.baseline_rows() != previous:
        raise RuntimeError("The schema upgrade changed or removed existing book rows")
    ch.insert(rows)
    if ch.count() != len(old_rows) + len(rows):
        raise RuntimeError("Collector rows did not survive the database round trip")
    expected = sorted(rows, key=lambda row: (row["exchange"], row["event_time"]))
    if ch.stored_rows(sorted(rows[0])) != expected:
        raise RuntimeError("Stored values differ from the emitted collector rows")
    for exchange in {row["exchange"] for row in rows}:
        for state in ("resync", "ok"):
            count = ch.count(f"WHERE collector_version = 'schema-upgrade-target' "
                             f"AND exchange = '{exchange}' AND state = '{state}'")
            if count != 1:
                raise RuntimeError(f"Expected one {exchange}/{state} row, found {count}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=ROOT,
                        help="Repository checkout to test (default: this checkout)")
    parser.add_argument("--base-ref", default="origin/main",
                        help="Git revision containing the schema to upgrade from")
    parser.add_argument("--target-ref", help="Test a Git revision instead of the working tree")
    parser.add_argument("--extra-sql", type=Path, action="append", default=[],
                        help="Apply a proposed migration after schema/*.sql (repeatable)")
    parser.add_argument("--image", default=IMAGE, help="ClickHouse server image")
    parser.add_argument("--emit-rows", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    source = args.source_dir.resolve()
    if args.emit_rows:
        print(json.dumps(collector_rows(source)))
        return 0
    extra = [path.resolve(strict=True) for path in args.extra_sql]
    with tempfile.TemporaryDirectory(prefix="market-ingest-upgrade-") as temp:
        root = Path(temp)
        baseline = root / "baseline"
        sha = export_ref(source, args.base_ref, baseline)
        print(f"Baseline: {sha}", flush=True)
        if args.target_ref:
            sha = export_ref(source, args.target_ref, root / "target")
            source = root / "target"
            print(f"Target: {sha}", flush=True)
        else:
            print(f"Target working tree: {source}", flush=True)
        old_rows = emit_rows(baseline, MARKER)
        rows = emit_rows(source, "schema-upgrade-target")
        print(f"Collector rows: {len(rows)} ({', '.join(sorted({r['exchange'] for r in rows}))})",
              flush=True)
        with ClickHouse(args.image) as ch:
            print("== Fresh database", flush=True)
            check_database(ch, source, rows, extra)
            print("PASS: fresh schema accepts resync and live collector rows", flush=True)
            ch.query("DROP DATABASE market")
            ch.query("DROP DATABASE ops")
            print("== Upgrade existing database", flush=True)
            check_database(ch, source, rows, extra, baseline, old_rows)
            print("PASS: upgraded schema preserves old rows and accepts new collector rows",
                  flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, subprocess.SubprocessError, OSError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        raise SystemExit(1)
