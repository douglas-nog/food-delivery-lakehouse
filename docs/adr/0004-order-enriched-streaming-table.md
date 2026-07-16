# 4. Build order_enriched from the append-only change feed

Date: 2026-07

## Status

Accepted

## Context

The serving layer needs a denormalized, current-state view of each order joined
with customer and restaurant context, to be synced to Lakebase for low-latency
lookups. The natural first implementation read the silver `orders` table as a
stream.

That failed: the silver tables are produced by `AUTO CDC`, which applies MERGEs,
so they are **not append-only**. A streaming source in Structured Streaming
requires an append-only source; the first status update to an order broke the
stream with a non-append error. The failure only surfaced once realistic traffic
(status transitions) was flowing — a static dataset had hidden it.

## Decision

Build `order_enriched` as a **streaming table fed by the append-only change feed**
(the validated orders view) via `AUTO CDC`, rather than by streaming the
MERGE-updated silver table. Model it as SCD Type 1 (current state per order).

## Consequences

- The serving table is compatible with standard Change Data Feed, which enables
  an incremental `TRIGGERED` sync to Lakebase without depending on a preview
  feature.
- It holds exactly one current row per order, joined with customer and restaurant
  context, so the application performs no joins at request time.
- The lineage flows from the change feed, not the materialized silver table, which
  is the correct source for a streaming consumer.

## Alternatives considered

- **Stream the silver `orders` table directly.** The initial approach; invalid
  because that table is not append-only.
- **Make it a materialized view.** MV sources require the Auto-CDF private preview
  for incremental sync; a streaming table with standard CDF avoids that
  dependency.
- **`skipChangeCommits` on the stream.** Would silently ignore updates — wrong for
  a view whose entire purpose is to reflect current order state.
