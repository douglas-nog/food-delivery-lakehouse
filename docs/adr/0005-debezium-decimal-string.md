# 5. Handle Debezium decimals as strings, cast to DECIMAL

Date: 2026-07

## Status

Accepted

## Context

By default, Debezium serializes PostgreSQL `NUMERIC` columns as base64-encoded
binary (Kafka Connect `Decimal` logical type). Parsed naively against a decimal
schema, these values arrived as `null` in the lakehouse, silently dropping the
monetary amounts and causing records to fall out of the pipeline entirely.

Two properties matter for monetary values: they must not be lost in
serialization, and they must preserve exact precision (floating point is unsafe
for money — accumulated rounding error diverges over aggregation).

## Decision

Configure the Debezium connector with `decimal.handling.mode = string`. `NUMERIC`
columns then arrive as exact decimal strings (for example, `"135.00"`), declared
as `StringType` at ingestion and cast to `DECIMAL(10,2)` in silver.

## Consequences

- Monetary precision is exact end to end; floating point is never used for money.
- The values are human-readable in the raw bronze events, which aids debugging.
- Casting is explicit and centralized in silver, keeping the contract clear:
  strings on the wire, decimals in the model.

## Alternatives considered

- **`decimal.handling.mode = double`.** Simpler, but converts exact decimals to
  floating point at ingestion, losing precision guarantees for money — rejected
  on correctness grounds.
- **Decoding the default base64 in-pipeline.** Possible but fragile and obscure,
  reconstructing a decimal from bytes when the connector can emit the correct
  representation directly.
