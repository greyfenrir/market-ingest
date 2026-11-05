"""Check collector rows against a fresh database and an upgraded database."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
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
TARGET = "schema-upgrade-target"
DEPTH_COLUMNS = [f"depth_{side}_usd_{band}"
                 for side in ("bid", "ask") for band in ("0p2", "0p5", "1", "2")]


def accumulated(column: str) -> str:
    return column.replace("_usd_", "_usd_acc_")


def command(args: list[str], *, data: str | None = None, timeout: int = 120) -> str:
    result = subprocess.run(args, input=data, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"Command failed: {' '.join(args)}\n{result.stderr or result.stdout}")
    return result.stdout


def export_ref(repo: Path, ref: str, destination: Path) -> str:
    revision = f"{ref}^{{commit}}"
    sha = command(["git", "-C", str(repo), "rev-parse", "--verify", "--end-of-options",
                   revision]).strip()
    archive = subprocess.check_output(["git", "-C", str(repo), "archive", sha, "schema", "src", "deploy"])
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
        collector = cls({"depth": {"enabled": True, "symbols": ["BTCUSDT"], "levels": 5}},
                        sink, focus=["BTCUSDT"], sd=QuietStats())
        collector._emit_book_rows(second, mono)
        bids, asks = [["99.9", "1.25"]], [["100.1", "1.75"]]
        far_bids = [["99.85", "2.125"], ["99.6", "3.375"],
                    ["99.0", "4.625"], ["98.5", "5.875"]]
        far_asks = [["100.15", "2.375"], ["100.4", "3.625"],
                    ["101.0", "4.875"], ["101.5", "6.125"]]
        collector._last_mark["BTCUSDT"] = 100.25
        sync = collector.depth["BTCUSDT"]
        if name == "binance_um":
            sync.depth_limit = 1
            sync.on_snapshot({"lastUpdateId": 10, "bids": bids, "asks": asks})
            payload = {"e": "depthUpdate", "s": "BTCUSDT", "E": second * 1000,
                       "T": second * 1000, "U": 10, "u": 11, "pu": 9,
                       "b": far_bids, "a": far_asks}
        elif name == "bybit_linear":
            payload = {"type": "snapshot", "topic": "orderbook.5.BTCUSDT",
                       "ts": second * 1000, "cts": second * 1000,
                       "data": {"s": "BTCUSDT", "u": 10, "seq": 10,
                                "b": bids + far_bids, "a": asks + far_asks}}
        else:
            raise RuntimeError(f"Add an order-book fixture for {name}")
        collector._handle_depth(payload, second * 1000 + 5, 200, collector._stat("depth"))
        sync.live_since_mono = time.monotonic() - 7
        collector._emit_book_rows(second + 1, time.monotonic())
        if len(sink.rows) != 2 or [r["state"] for r in sink.rows] != ["resync", "ok"]:
            raise RuntimeError(f"{name}: expected a resync row and a live row")
        if "book_age_s" in sink.rows[-1] and sink.rows[-1]["book_age_s"] != 7:
            raise RuntimeError(f"{name}: expected a nonzero book age of seven seconds")
        if name == "binance_um" and accumulated(DEPTH_COLUMNS[0]) in sink.rows[-1]:
            for column in DEPTH_COLUMNS:
                strict, acc = sink.rows[-1][column], sink.rows[-1][accumulated(column)]
                if not (0 < strict < acc and acc != round(acc)):
                    raise RuntimeError(f"Fixture must distinguish strict, accumulated and rounded {column}")
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

    def schema(self) -> dict[str, dict]:
        description = self.query("DESCRIBE TABLE market.book_agg FORMAT JSONEachRow")
        fields = ("type", "default_type", "default_expression", "codec_expression", "ttl_expression")
        return {row["name"]: {key: row[key] for key in fields}
                for row in map(json.loads, description.splitlines())}

    def stored_rows(self, keys: list[str], version: str) -> list[dict]:
        projection = ", ".join(f"`{key}`" for key in keys)
        output = self.query(f"SELECT {projection} FROM market.book_agg "
                            f"WHERE collector_version = '{version}' "
                            "ORDER BY exchange, event_time FORMAT JSONEachRow")
        return [json.loads(line) for line in output.splitlines()]

    def count(self, where: str = "") -> int:
        return int(self.query(f"SELECT count() FROM market.book_agg {where}"))


def check_rows(ch: ClickHouse, rows: list[dict], version: str) -> None:
    keys = sorted(rows[0])
    expected = sorted(rows, key=lambda row: (row["exchange"], row["event_time"]))
    stored = ch.stored_rows(keys, version)
    if len(stored) != len(expected):
        raise RuntimeError(f"{version}: expected {len(expected)} rows, found {len(stored)}")
    for want, got in zip(expected, stored):
        for key in keys:
            if got[key] != want[key]:
                raise RuntimeError(f"{version}/{want['exchange']}/{want['event_time']}: "
                                   f"{key}: expected {want[key]!r}, found {got[key]!r}")


def later_rows(rows: list[dict], version: str) -> list[dict]:
    shifted = []
    for row in rows:
        at = datetime.fromisoformat(row["event_time"]) + timedelta(seconds=10)
        shifted.append({**row, "event_time": at.strftime("%Y-%m-%d %H:%M:%S"),
                        "collector_version": version})
    return shifted


def historical_rows(rows: list[dict], schema: dict[str, dict]) -> list[dict]:
    expected = []
    for row in rows:
        migrated = {}
        for key, definition in schema.items():
            source = key
            if (key in DEPTH_COLUMNS and accumulated(key) in row
                    and accumulated(key) not in schema):
                source = accumulated(key)
            if source in row:
                value = row[source]
            elif key == "book_age_s":
                value = 0
            else:
                raise RuntimeError(f"Define the historical value for new column {key}")
            if key in DEPTH_COLUMNS and definition["type"] == "UInt64":
                value = round(value)
            migrated[key] = value
        expected.append(migrated)
    return expected


def migration(ch: ClickHouse, script: Path | None, phase: str) -> None:
    if script is not None:
        print(f"  migration {script.name} --phase {phase}", flush=True)
        command([sys.executable, str(script), "--phase", phase, "--client", "docker", "exec",
                 "-i", ch.name, "clickhouse-client"], timeout=180)


def prepare(ch: ClickHouse, source: Path, extra: list[Path], script: Path | None) -> None:
    ch.apply(source, extra)
    migration(ch, script, "prepare")


def check_database(ch: ClickHouse, source: Path, rows: list[dict], extra: list[Path],
                   script: Path | None, baseline: Path | None = None,
                   old_rows: list[dict] | None = None,
                   fresh_schema: dict[str, dict] | None = None) -> dict[str, dict]:
    old_rows = old_rows or []
    legacy_rows = []
    if baseline is not None:
        ch.apply(baseline)
        ch.insert(old_rows)
        check_rows(ch, old_rows, MARKER)
        legacy_rows = later_rows(old_rows, "schema-upgrade-late-old-writer")
    else:
        ch.apply(source, extra)
        fresh_schema = ch.schema()
    prepare(ch, source, extra, script)
    prepare(ch, source, extra, script)
    if legacy_rows:
        ch.insert(legacy_rows)
        check_rows(ch, legacy_rows, "schema-upgrade-late-old-writer")
        print("PASS: old collector can still write after preparation", flush=True)
    ch.insert(rows)
    check_rows(ch, rows, TARGET)
    migration(ch, script, "finish")
    check_rows(ch, rows, TARGET)
    if old_rows:
        check_rows(ch, historical_rows(old_rows, fresh_schema), MARKER)
        check_rows(ch, historical_rows(legacy_rows, fresh_schema), "schema-upgrade-late-old-writer")
    if ch.schema() != fresh_schema:
        expected, actual = fresh_schema, ch.schema()
        missing, obsolete = expected.keys() - actual.keys(), actual.keys() - expected.keys()
        changed = {k: {"expected": expected[k], "actual": actual[k]}
                   for k in expected.keys() & actual.keys() if expected[k] != actual[k]}
        raise RuntimeError(f"Fresh and upgraded schemas differ: missing={sorted(missing)}, "
                           f"obsolete={sorted(obsolete)}, changed={changed}")
    after = later_rows(rows, TARGET)
    ch.insert(after)
    prepare(ch, source, extra, script)
    migration(ch, script, "finish")
    check_rows(ch, rows + after, TARGET)
    if old_rows:
        check_rows(ch, historical_rows(old_rows, fresh_schema), MARKER)
        check_rows(ch, historical_rows(legacy_rows, fresh_schema), "schema-upgrade-late-old-writer")
    if ch.schema() != fresh_schema:
        raise RuntimeError("Repeating the migration changed the schema")
    if ch.count() != len(old_rows) + len(legacy_rows) + len(rows) + len(after):
        raise RuntimeError("Migration changed the row count")
    return fresh_schema


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=ROOT,
                        help="Repository checkout to test (default: this checkout)")
    parser.add_argument("--base-ref", default="origin/main",
                        help="Git revision containing the schema to upgrade from")
    parser.add_argument("--target-ref", help="Test a Git revision instead of the working tree")
    parser.add_argument("--extra-sql", type=Path, action="append", default=[],
                        help="Apply a proposed migration after schema/*.sql (repeatable)")
    parser.add_argument("--migration-script", type=Path,
                        help="Override deploy/migrations/book_depth.py for a migration experiment")
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
        script = source / "deploy" / "migrations" / "book_depth.py"
        if args.migration_script:
            script = args.migration_script.resolve(strict=True)
        elif not script.is_file():
            script = None
        old_rows = emit_rows(baseline, MARKER)
        rows = emit_rows(source, TARGET)
        print(f"Collector rows: {len(rows)} ({', '.join(sorted({r['exchange'] for r in rows}))})",
              flush=True)
        with ClickHouse(args.image) as ch:
            print("== Fresh database", flush=True)
            fresh_schema = check_database(ch, source, rows, extra, script)
            print("PASS: fresh schema accepts resync and live collector rows", flush=True)
            ch.query("DROP DATABASE market")
            ch.query("DROP DATABASE ops")
            print("== Upgrade existing database", flush=True)
            check_database(ch, source, rows, extra, script, baseline, old_rows, fresh_schema)
            print("PASS: upgraded history and schema match the target; repeated deployment preserves writes",
                  flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, subprocess.SubprocessError, OSError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        raise SystemExit(1)
