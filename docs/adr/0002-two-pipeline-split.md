# 2. Separate ingestion and transformation pipelines

Date: 2026-07

## Status

Accepted

## Context

The lakehouse follows a medallion architecture (bronze, silver, gold). Databricks
recommends separating raw ingestion from downstream transformation so the two can
evolve, be scheduled, and fail independently. Ingestion is tied to the external
source (Kafka availability, connector state); transformation is tied to business
logic (SCD, expectations, aggregates).

## Decision

Implement two Lakeflow Declarative Pipelines: a **bronze** pipeline that ingests
the Kafka topics into raw streaming tables, and a **transform** pipeline that
builds silver and gold.

## Consequences

- Ingestion failures (a broker being unreachable) do not invalidate or re-run the
  transformation logic, and vice versa.
- The bronze pipeline stays minimal and stable; business-rule churn is contained
  in the transform pipeline.
- A Lakeflow Job orchestrates the two in order (bronze → transform → sync), which
  is where their dependency is expressed explicitly.
- Two pipelines mean two sets of compute and two definitions to maintain — an
  acceptable cost for the isolation gained.

## Alternatives considered

- **A single pipeline covering bronze through gold.** Simpler to define and
  orchestrate, but couples ingestion and business logic into one failure domain
  and one release unit, against the recommended pattern.
