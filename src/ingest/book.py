"""Local order book and its once-a-second aggregate, independent of any venue.

The venue layer owns sequencing (snapshot alignment, update-id checks,
resyncs); this module only holds levels and answers "what does the book look
like right now". Keeping it free of network code is what makes the arithmetic
testable offline and reusable for every venue that ships snapshot + deltas.

THE TRUSTED RANGE IS THE POINT OF THIS FILE.

A REST snapshot is truncated: Binance's 1000 levels reach only about +-0.16 %
on BTC. Deltas then report every change on the whole book, so levels beyond
the snapshot do appear - but only those that changed after we subscribed. A
large resting order nobody touched stays invisible forever, and it is exactly
the kind of order a depth factor exists to see. So the book remembers how far
the snapshot reached on each side:

    bids >= bid_floor  and  asks <= ask_ceil   are complete;
    anything further out                        is a partial sample.

Every depth figure is reported twice - strictly inside the trusted range, and
over everything accumulated - and a band is valid only when the trusted range
covers it. Whether the accumulated figure is good enough to use is a question
for the data (compare against Binance's bookDepth archive), not for this code.
"""

from __future__ import annotations

import math
from typing import Iterable

# Bands around mid, in percent. 0.2 is there to line up with Binance's
# bookDepth archive, which has no 0.5.
BANDS_PCT = (0.2, 0.5, 1.0, 2.0)

Levels = Iterable[tuple[str | float, str | float]]


def band_label(pct: float) -> str:
    """0.2 -> '0p2', 1.0 -> '1': a column-name-safe spelling of a band."""
    return f"{pct:g}".replace(".", "p")


class LocalBook:
    def __init__(self) -> None:
        # Keyed by float, not by the exchange's string: "100.10" and "100.1"
        # are the same level, and float parsing of a decimal literal is exact
        # enough to be a stable key. Strings would split one level into two.
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.last_update_id = 0
        self.synced = False
        # No snapshot yet: nothing is trusted.
        self.bid_floor = math.inf
        self.ask_ceil = -math.inf

    # ------------------------------------------------------------- updates

    def load_snapshot(self, bids: Levels, asks: Levels, last_update_id: int,
                      *, depth_limit: int | None = None) -> None:
        """Replace the book with a REST snapshot.

        depth_limit is the `limit` the snapshot was requested with. A side
        that came back shorter than that is the whole side, so it is trusted
        all the way out; without the limit we cannot tell and assume the
        snapshot was cut off."""
        self.bids = {}
        self.asks = {}
        bids, asks = list(bids), list(asks)
        _apply_side(self.bids, bids)
        _apply_side(self.asks, asks)

        complete_bids = depth_limit is not None and len(bids) < depth_limit
        complete_asks = depth_limit is not None and len(asks) < depth_limit
        self.bid_floor = 0.0 if complete_bids else min(self.bids, default=math.inf)
        self.ask_ceil = math.inf if complete_asks else max(self.asks, default=-math.inf)

        self.last_update_id = last_update_id
        self.synced = True

    def apply(self, bids: Levels, asks: Levels, update_id: int) -> None:
        """Apply one delta. Quantities are absolute; zero removes the level.
        Ordering and continuity are the caller's job."""
        _apply_side(self.bids, bids)
        _apply_side(self.asks, asks)
        self.last_update_id = update_id

    def invalidate(self) -> None:
        """Forget everything. Called on a sequence break, so that a stale
        book can never be aggregated by mistake while the resync is pending."""
        self.__init__()

    def prune(self, max_distance_pct: float) -> None:
        """Drop levels further than max_distance_pct from mid.

        Deltas can add levels arbitrarily far out, and nothing ever removes a
        level that stops being updated, so without this the book only grows.
        The trusted range shrinks with it: what was dropped is no longer
        known, even on a side that used to be complete."""
        mid = self.mid()
        if mid is None:
            return
        lo = mid * (1 - max_distance_pct / 100)
        hi = mid * (1 + max_distance_pct / 100)
        self.bids = {p: q for p, q in self.bids.items() if p >= lo}
        self.asks = {p: q for p, q in self.asks.items() if p <= hi}
        self.bid_floor = max(self.bid_floor, lo)
        self.ask_ceil = min(self.ask_ceil, hi)

    # -------------------------------------------------------------- queries

    def best_bid(self) -> float | None:
        return max(self.bids, default=None)

    def best_ask(self) -> float | None:
        return min(self.asks, default=None)

    def mid(self) -> float | None:
        bb, ba = self.best_bid(), self.best_ask()
        if bb is None or ba is None:
            return None
        return (bb + ba) / 2

    def crossed(self) -> bool:
        """Best bid at or above best ask. A real book cannot do that, so it
        always means the local copy went wrong and must be resynced."""
        bb, ba = self.best_bid(), self.best_ask()
        return bb is not None and ba is not None and bb >= ba

    def aggregate(self, bands: Iterable[float] = BANDS_PCT, *,
                  mark: float | None = None, contract_size: float = 1.0) -> dict | None:
        """One row of figures for this instant, or None when the book cannot
        produce honest ones (not synced, a side empty, or crossed).

        Notional is qty x contract_size x mark: depth is compared across
        venues in USD at the mark price. mid stands in when no mark is known.
        """
        if not self.synced or not self.bids or not self.asks or self.crossed():
            return None

        bids = sorted(self.bids.items(), reverse=True)   # best first
        asks = sorted(self.asks.items())
        bb, bb_qty = bids[0]
        ba, ba_qty = asks[0]
        mid = (bb + ba) / 2
        ref = (mark if mark and mark > 0 else mid) * contract_size

        bands = sorted(bands)
        bounds = [mid * p / 100 for p in bands]
        bid_trusted = mid - self.bid_floor
        ask_trusted = self.ask_ceil - mid
        bid_strict, bid_acc = _depth(((mid - p, q) for p, q in bids), bounds, bid_trusted)
        ask_strict, ask_acc = _depth(((p - mid, q) for p, q in asks), bounds, ask_trusted)

        row = {
            "best_bid": bb,
            "best_ask": ba,
            "best_bid_qty": bb_qty,
            "best_ask_qty": ba_qty,
            "mid": mid,
            "spread_bps": (ba - bb) / mid * 1e4,
            "covered_bid_pct": _pct(bid_trusted, mid),
            "covered_ask_pct": _pct(ask_trusted, mid),
            "levels_bid": len(bids),
            "levels_ask": len(asks),
        }
        for i, p in enumerate(bands):
            b = band_label(p)
            row[f"depth_bid_usd_{b}"] = bid_strict[i] * ref
            row[f"depth_ask_usd_{b}"] = ask_strict[i] * ref
            row[f"depth_bid_usd_acc_{b}"] = bid_acc[i] * ref
            row[f"depth_ask_usd_acc_{b}"] = ask_acc[i] * ref
            # Compared on distances rather than on the rounded percentages, so
            # a band edge that the trusted range exactly reaches counts.
            row[f"band_valid_bid_{b}"] = int(bid_trusted >= bounds[i])
            row[f"band_valid_ask_{b}"] = int(ask_trusted >= bounds[i])
        return row


def empty_aggregate(bands: Iterable[float] = BANDS_PCT) -> dict:
    """The same keys as LocalBook.aggregate, all zero and every band invalid.
    Written while a book is resyncing, so a second without a valid book is a
    row that says so rather than a hole that says nothing."""
    row: dict = {k: 0.0 for k in ("best_bid", "best_ask", "best_bid_qty", "best_ask_qty",
                                  "mid", "spread_bps", "covered_bid_pct", "covered_ask_pct")}
    row["levels_bid"] = row["levels_ask"] = 0
    for p in sorted(bands):
        b = band_label(p)
        for side in ("bid", "ask"):
            row[f"depth_{side}_usd_{b}"] = 0.0
            row[f"depth_{side}_usd_acc_{b}"] = 0.0
            row[f"band_valid_{side}_{b}"] = 0
    return row


def _apply_side(side: dict[float, float], levels: Levels) -> None:
    for price, qty in levels:
        p, q = float(price), float(qty)
        if q == 0:
            side.pop(p, None)
        else:
            side[p] = q


def _depth(levels, bounds: list[float], trusted: float) -> tuple[list[float], list[float]]:
    """Cumulative qty within each distance bound, in one pass from the best
    level outward. Returns (strict, accumulated): strict counts only levels
    inside the trusted distance."""
    n = len(bounds)
    strict, acc = [0.0] * n, [0.0] * n
    s = a = 0.0
    i = 0
    for dist, qty in levels:
        while i < n and dist > bounds[i]:
            strict[i], acc[i] = s, a
            i += 1
        if i == n:
            break
        a += qty
        if dist <= trusted:
            s += qty
    while i < n:
        strict[i], acc[i] = s, a
        i += 1
    return strict, acc


def _pct(distance: float, mid: float) -> float:
    """Trusted distance as percent of mid, clamped to [0, 100]: an infinite
    reach (a complete side) reads as 100, a floor above mid as 0."""
    return min(100.0, max(0.0, distance / mid * 100))
