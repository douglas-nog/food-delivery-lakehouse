# 3. Silver modeling: SCD per entity and a quarantine for invalid data

Date: 2026-07

## Status

Accepted

## Context

Silver reconstructs current (and historical) state from the Debezium change
stream using `AUTO CDC`. Two modeling questions arise: how much history to keep
per entity, and what to do with records that violate business rules.

Different entities have different history needs. Customer contact details are
corrections — the latest value is what matters. Restaurants, products, orders,
and order items have meaningful lifecycles (price changes, status transitions)
where history has analytical value.

Invalid records (for example, an order with a negative total) will occur; the
question is whether to drop them silently or preserve them for observability.

## Decision

Model customers as **SCD Type 1** (overwrite) and restaurants, products, orders,
and order items as **SCD Type 2** (full history). Enforce data quality with
expectations, and route invalid orders to a dedicated **quarantine** table
instead of dropping them.

## Consequences

- Analytical queries can reconstruct the state of an order or a price at any point
  in time, while customer dimensions stay compact.
- Bad data is observable and auditable: the quarantine table makes rejects
  countable and inspectable rather than invisible.
- SCD Type 2 tables grow with every change and require filtering to the current
  version (`__END_AT IS NULL`) for point-in-time reads.

## Alternatives considered

- **SCD Type 2 everywhere.** Uniform but wasteful for entities where history has
  no analytical value (customer contact fields).
- **Dropping invalid rows.** Simpler, but silent data loss is the opposite of
  observable — it hides quality problems instead of surfacing them.
