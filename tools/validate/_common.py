"""Shared helpers for the order-book checks: ClickHouse over HTTP, downloads,
and summary statistics. Standard library only, so the archive comparisons run
anywhere Python does.

ClickHouse is reached one of two ways:
- HTTP: --ch or CH_URL, e.g. http://172.16.0.106:8123/?user=ro. A password
  goes in CH_PASSWORD (sent as a header, never in the URL or on the command
  line); CH_USER overrides the user;
- a command: --ch-cmd or CH_CMD, e.g. "sh ~/bin/prod-sql" for
  production, which is read through a forced-command ssh key and has no HTTP
  port open. The query is passed as the last argument, on one line, and the
  command must print the result as ClickHouse prints it.
"""

from __future__ import annotations

import calendar
import csv
import hashlib
import io
import os
import re
import shlex
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

BANDS = (0.2, 0.5, 1.0, 2.0)          # what book_agg stores
BAND_COL = {0.2: "0p2", 0.5: "0p5", 1.0: "1", 2.0: "2"}
AGE_BUCKETS = ((0, 1800, "<30m"), (1800, 3600, "30-60m"), (3600, 7200, "1-2h"),
               (7200, 10**9, ">2h"))


CMD = "cmd:"


def add_ch_args(p) -> None:
    p.add_argument("--ch", help="ClickHouse HTTP URL (default: $CH_URL)")
    p.add_argument("--ch-cmd", help="command that runs one query instead of HTTP (default: $CH_CMD)")


def ch_url(arg: str | None, cmd: str | None = None) -> str:
    """The target ch_query uses: an HTTP URL, or CMD + a command line."""
    cmd = cmd or (None if arg else os.environ.get("CH_CMD"))
    if cmd:
        return CMD + cmd
    url = arg or os.environ.get("CH_URL")
    if not url:
        sys.exit("ClickHouse: pass --ch URL or --ch-cmd COMMAND (or set CH_URL / CH_CMD)")
    return url


def ch_query(url: str, sql: str) -> list[dict]:
    """Run one SELECT, return rows as dicts of strings."""
    sql = sql + " FORMAT TabSeparatedWithNames"
    if url.startswith(CMD):
        one_line = re.sub(r"\s+", " ", sql).strip()
        # No shell runs the command, so expand a leading ~ ourselves.
        argv = [os.path.expanduser(a) for a in shlex.split(url[len(CMD):])]
        r = subprocess.run(argv + [one_line],
                           capture_output=True, text=True, timeout=600)
        if r.returncode:
            sys.exit(f"query command failed ({r.returncode}): {r.stderr.strip()[:500]}")
        text = r.stdout
    else:
        req = urllib.request.Request(url, data=sql.encode(), method="POST")
        if os.environ.get("CH_USER"):
            req.add_header("X-ClickHouse-User", os.environ["CH_USER"])
        if os.environ.get("CH_PASSWORD"):
            req.add_header("X-ClickHouse-Key", os.environ["CH_PASSWORD"])
        try:
            with urllib.request.urlopen(req, timeout=300) as r:
                text = r.read().decode()
        except urllib.error.HTTPError as e:
            sys.exit(f"ClickHouse HTTP {e.code}: {e.read().decode(errors='replace')[:800]}")
    rows = list(csv.reader(io.StringIO(text), delimiter="\t"))
    if not rows:
        return []
    head = rows[0]
    return [dict(zip(head, r)) for r in rows[1:]]


def download(url: str, dest: Path, sha256_url: str | None = None) -> Path:
    """Fetch url into dest (kept as a cache: an existing file is reused).
    With sha256_url (Binance's .CHECKSUM), the file is verified."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not dest.exists():
        tmp = dest.with_suffix(dest.suffix + ".part")
        print(f"downloading {url}", file=sys.stderr)
        with urllib.request.urlopen(url, timeout=600) as r, open(tmp, "wb") as f:
            while chunk := r.read(1 << 20):
                f.write(chunk)
        tmp.replace(dest)
    if sha256_url:
        with urllib.request.urlopen(sha256_url, timeout=60) as r:
            want = r.read().decode().split()[0].lower()
        got = hashlib.sha256(dest.read_bytes()).hexdigest()
        if got != want:
            dest.unlink()
            sys.exit(f"{dest.name}: sha256 {got} != {want}, file removed")
    return dest


def summary(xs: list[float]) -> dict:
    """Median, p5/p95 and the share within 1 % and 5 % of 1.0."""
    if not xs:
        return {"n": 0}
    s = sorted(xs)
    q = lambda p: s[min(len(s) - 1, int(p * len(s)))]  # noqa: E731
    return {"n": len(s), "median": statistics.median(s), "p5": q(0.05), "p95": q(0.95),
            "in1": sum(abs(x - 1) <= 0.01 for x in s) / len(s),
            "in5": sum(abs(x - 1) <= 0.05 for x in s) / len(s)}


def fmt(st: dict) -> str:
    if not st.get("n"):
        return f"{'-':>7} {'':>13} {'':>5} {'':>5} n=0"
    return (f"{st['median']:7.3f} {st['p5']:6.3f}-{st['p95']:<6.3f} "
            f"{st['in1'] * 100:4.0f}% {st['in5'] * 100:4.0f}% n={st['n']}")


HEADER = f"{'median':>7} {'p5-p95':>13} {'<=1%':>5} {'<=5%':>5}"


def age_bucket(age: int) -> str:
    for lo, hi, name in AGE_BUCKETS:
        if lo <= age < hi:
            return name
    return AGE_BUCKETS[-1][2]


def add_window_args(p) -> None:
    p.add_argument("--date", nargs="+", help="UTC day(s), YYYY-MM-DD")
    p.add_argument("--hours", type=float, help="the last N hours, up to the last full hour")


def windows(a) -> list[tuple[str, int, int]]:
    """(label, start, end) in unix seconds, end exclusive: one per --date, or
    the last --hours (default 24) ending at the last full hour."""
    if a.date:
        out = []
        for d in a.date:
            t0 = calendar.timegm(time.strptime(d, "%Y-%m-%d"))
            out.append((d, t0, t0 + 86400))
        return out
    t1 = int(time.time()) // 3600 * 3600
    t0 = t1 - int((a.hours or 24) * 3600)
    return [(f"last {(t1 - t0) / 3600:g} h to {time.strftime('%Y-%m-%d %H:%M', time.gmtime(t1))} UTC",
             t0, t1)]


def ts(t: int) -> str:
    """unix seconds as a ClickHouse UTC literal"""
    return f"toDateTime({int(t)}, 'UTC')"
