"""Migrate order-book history before and after updating the collectors."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys


BANDS = ("0p2", "0p5", "1", "2")
DEPTH = [f"depth_{side}_usd_{band}" for side in ("bid", "ask") for band in BANDS]
ACC = [name.replace("_usd_", "_usd_acc_") for name in DEPTH]
FLAGS = [f"band_valid_{side}_{band}" for side in ("bid", "ask") for band in BANDS]


def query(client: list[str], sql: str) -> str:
    result = subprocess.run([*client, "--query", sql], text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(result.stderr or result.stdout)
    return result.stdout


def prepare(client: list[str]) -> None:
    query(client, "ALTER TABLE market.book_agg ADD COLUMN IF NOT EXISTS "
                  "book_age_s UInt32 CODEC(Delta(4), ZSTD(3)) AFTER n_updates")


def finish(client: list[str]) -> None:
    description = query(client, "DESCRIBE TABLE market.book_agg FORMAT JSONEachRow")
    columns = {row["name"] for row in map(json.loads, description.splitlines())}
    present = columns & set(ACC)
    if present and present != set(ACC):
        raise RuntimeError(f"Incomplete legacy depth columns: {sorted(present)}")
    if present:
        # New writers omit acc columns; their nonzero primary values must survive.
        assignments = ", ".join(f"{depth} = round(if({acc} != 0, {acc}, {depth}))"
                                for depth, acc in zip(DEPTH, ACC))
        changed = " OR ".join(f"{acc} != 0" for acc in ACC)
        query(client, f"ALTER TABLE market.book_agg UPDATE {assignments} "
                      f"WHERE {changed} SETTINGS mutations_sync = 2")
    changes = [f"MODIFY COLUMN {name} UInt64 CODEC(T64, ZSTD(3))" for name in DEPTH]
    # Wait for conversion before removing the source columns.
    query(client, "ALTER TABLE market.book_agg " + ", ".join(changes)
                  + " SETTINGS mutations_sync = 2, alter_sync = 2")
    query(client, "ALTER TABLE market.book_agg "
                  + ", ".join(f"DROP COLUMN IF EXISTS {name}" for name in ACC + FLAGS))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("prepare", "finish"), required=True)
    parser.add_argument("--client", nargs=argparse.REMAINDER,
                        help="clickhouse-client command, including connection options")
    args = parser.parse_args()
    client = args.client or ["clickhouse-client"]
    if args.phase == "prepare":
        prepare(client)
    else:
        finish(client)
    print(f"PASS: book depth migration {args.phase}")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        raise SystemExit(1)
