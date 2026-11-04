-- Order-book aggregates: one row per second per (exchange, symbol).
--
-- Raw depth deltas are far too heavy to keep, so the collector holds a local
-- book and writes what it looks like at the end of every second. A row is
-- written even when the book is not usable (state = resync, zeros), so a
-- missing second is evidence of a collector that was not running, never of a
-- quiet market.
--
-- DEPTH AND COVERAGE.
-- A REST snapshot is truncated - 1000 levels reach about +-0.16 % on Binance
-- BTC - and deltas reveal the levels beyond it as they change. depth_* counts
-- every level the book holds; covered_*_pct says how far from mid the book is
-- known level by level (a band of width b is fully known when covered >= b).
-- Checked against Binance's bookDepth archive on 2026-10-30: depth_* matched
-- it at every band (median ratio 1.000), BTC and ETH 1-2 % included, where
-- coverage is a fraction of the band. A book needs time to learn the far
-- levels; book_age_s lets the first minutes after a (re)start be cut out.
--
-- Notional is qty x contract size x mark price, in whole USD.
-- Column names must match ingest.book.LocalBook.aggregate exactly: JSONEachRow
-- rejects a whole batch over one unknown key (tests/test_binance_um_depth.py
-- checks the two against each other).

CREATE TABLE IF NOT EXISTS market.book_agg
(
    exchange          LowCardinality(String),
    symbol            LowCardinality(String),
    -- the second at whose end the book was read, local clock (chrony)
    event_time        DateTime('UTC')      CODEC(Delta(4), ZSTD(3)),
    ingest_time       DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(3)),
    -- exchange transaction time of the last delta applied; the gap to
    -- event_time is how far behind the book was
    last_update_time  DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(3)),

    best_bid          Float64 CODEC(Gorilla, ZSTD(3)),
    best_ask          Float64 CODEC(Gorilla, ZSTD(3)),
    best_bid_qty      Float64 CODEC(Gorilla, ZSTD(3)),
    best_ask_qty      Float64 CODEC(Gorilla, ZSTD(3)),
    mid               Float64 CODEC(Gorilla, ZSTD(3)),
    spread_bps        Float64 CODEC(Gorilla, ZSTD(3)),

    -- cumulative notional within the band, best level outward
    depth_bid_usd_0p2 UInt64 CODEC(T64, ZSTD(3)),
    depth_bid_usd_0p5 UInt64 CODEC(T64, ZSTD(3)),
    depth_bid_usd_1   UInt64 CODEC(T64, ZSTD(3)),
    depth_bid_usd_2   UInt64 CODEC(T64, ZSTD(3)),
    depth_ask_usd_0p2 UInt64 CODEC(T64, ZSTD(3)),
    depth_ask_usd_0p5 UInt64 CODEC(T64, ZSTD(3)),
    depth_ask_usd_1   UInt64 CODEC(T64, ZSTD(3)),
    depth_ask_usd_2   UInt64 CODEC(T64, ZSTD(3)),

    -- distance from mid, in %, inside which the book is complete
    covered_bid_pct   Float64 CODEC(Gorilla, ZSTD(3)),
    covered_ask_pct   Float64 CODEC(Gorilla, ZSTD(3)),

    levels_bid        UInt32 CODEC(ZSTD(3)),
    levels_ask        UInt32 CODEC(ZSTD(3)),
    -- update ids applied during this second; n_updates = 0 on a live book
    -- means nothing changed, not that nothing arrived (see state)
    seq_first         UInt64 CODEC(Delta(8), ZSTD(3)),
    seq_last          UInt64 CODEC(Delta(8), ZSTD(3)),
    n_updates         UInt32 CODEC(ZSTD(3)),
    -- seconds since the book went live from a snapshot (0 when not live);
    -- a refresh does not reset it
    book_age_s        UInt32 CODEC(Delta(4), ZSTD(3)),
    -- ok | degraded (resynced this second, or price left the snapshot's range)
    -- | stale (no update for stale_seconds) | resync (no usable book)
    state             LowCardinality(String),
    collector_version LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (exchange, symbol, event_time, ingest_time);
