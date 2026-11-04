-- market.trades gains trade_id for venues whose trade ids are not numbers
-- (Bybit: a UUID). Additive and idempotent: existing rows read it as '', and
-- 002_market.sql already has it for tables created from scratch.
ALTER TABLE market.trades ADD COLUMN IF NOT EXISTS trade_id String CODEC(ZSTD(3)) AFTER agg_id;
