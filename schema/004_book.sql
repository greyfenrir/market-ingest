-- Order-book aggregates: one row per second per (exchange, symbol).
--
-- Raw depth deltas are far too heavy to keep, so the collector holds a local
-- book and writes what it looks like at the end of every second. A row is
-- written even when the book is not usable (state = resync, zeros, every band
-- invalid), so a missing second is evidence of a collector that was not
-- running, never of a quiet market.
--
-- TRUSTED VS ACCUMULATED DEPTH.
-- A REST snapshot is truncated - 1000 levels reach about +-0.16 % on Binance
-- BTC - and deltas only reveal the levels that change after subscription. Each
-- band is therefore reported twice:
--   depth_*_usd_<band>      only levels inside the range the snapshot covered;
--   depth_*_usd_acc_<band>  everything the book has accumulated.
-- band_valid_* is 1 only when the snapshot's range covers the whole band.
-- Whether the accumulated figure is usable is to be settled against Binance's
-- bookDepth archive, not assumed.
--
-- Notional is qty x contract size x mark price, in USD.
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

    depth_bid_usd_0p2 Float64 CODEC(Gorilla, ZSTD(3)),
    depth_bid_usd_0p5 Float64 CODEC(Gorilla, ZSTD(3)),
    depth_bid_usd_1   Float64 CODEC(Gorilla, ZSTD(3)),
    depth_bid_usd_2   Float64 CODEC(Gorilla, ZSTD(3)),
    depth_ask_usd_0p2 Float64 CODEC(Gorilla, ZSTD(3)),
    depth_ask_usd_0p5 Float64 CODEC(Gorilla, ZSTD(3)),
    depth_ask_usd_1   Float64 CODEC(Gorilla, ZSTD(3)),
    depth_ask_usd_2   Float64 CODEC(Gorilla, ZSTD(3)),

    depth_bid_usd_acc_0p2 Float64 CODEC(Gorilla, ZSTD(3)),
    depth_bid_usd_acc_0p5 Float64 CODEC(Gorilla, ZSTD(3)),
    depth_bid_usd_acc_1   Float64 CODEC(Gorilla, ZSTD(3)),
    depth_bid_usd_acc_2   Float64 CODEC(Gorilla, ZSTD(3)),
    depth_ask_usd_acc_0p2 Float64 CODEC(Gorilla, ZSTD(3)),
    depth_ask_usd_acc_0p5 Float64 CODEC(Gorilla, ZSTD(3)),
    depth_ask_usd_acc_1   Float64 CODEC(Gorilla, ZSTD(3)),
    depth_ask_usd_acc_2   Float64 CODEC(Gorilla, ZSTD(3)),

    -- how far from mid the snapshot's range reaches, percent (100 = whole side)
    covered_bid_pct   Float64 CODEC(Gorilla, ZSTD(3)),
    covered_ask_pct   Float64 CODEC(Gorilla, ZSTD(3)),
    band_valid_bid_0p2 UInt8 CODEC(ZSTD(1)),
    band_valid_bid_0p5 UInt8 CODEC(ZSTD(1)),
    band_valid_bid_1   UInt8 CODEC(ZSTD(1)),
    band_valid_bid_2   UInt8 CODEC(ZSTD(1)),
    band_valid_ask_0p2 UInt8 CODEC(ZSTD(1)),
    band_valid_ask_0p5 UInt8 CODEC(ZSTD(1)),
    band_valid_ask_1   UInt8 CODEC(ZSTD(1)),
    band_valid_ask_2   UInt8 CODEC(ZSTD(1)),

    levels_bid        UInt32 CODEC(ZSTD(3)),
    levels_ask        UInt32 CODEC(ZSTD(3)),
    -- update ids applied during this second; n_updates = 0 on a live book
    -- means nothing changed, not that nothing arrived (see state)
    seq_first         UInt64 CODEC(Delta(8), ZSTD(3)),
    seq_last          UInt64 CODEC(Delta(8), ZSTD(3)),
    n_updates         UInt32 CODEC(ZSTD(3)),
    -- ok | degraded (resynced this second, or price left the snapshot's range)
    -- | stale (no update for stale_seconds) | resync (no usable book)
    state             LowCardinality(String),
    collector_version LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (exchange, symbol, event_time, ingest_time);
