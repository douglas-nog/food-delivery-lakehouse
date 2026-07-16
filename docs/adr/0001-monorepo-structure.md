# 1. Monorepo with top-level separation by concern

Date: 2026-07

## Status

Accepted

## Context

The project spans three distinct concerns: cloud infrastructure (Azure resources
via Terraform), the CDC source system (PostgreSQL schema, Debezium, Kafka, load
generator), and the lakehouse itself (Databricks pipelines, jobs, packaging).
These evolve at different rates and use different toolchains, but they describe
one coherent end-to-end system and are developed by the same person.

## Decision

Keep everything in a single repository, separated at the top level into `infra/`,
`source-system/`, and `lakehouse/`, with shared documentation under `docs/`.

## Consequences

- A single clone and a single history give a complete, reproducible picture of
  the platform end to end; a reviewer can follow the data from Postgres to
  Lakebase without jumping between repositories.
- Cross-cutting changes (for example, adding a table that touches the schema, the
  Debezium include list, and the bronze layer) land in one atomic commit.
- The repository mixes toolchains (Terraform, Docker, Python, Databricks
  bundles), so tooling config is scoped per directory rather than global.

## Alternatives considered

- **One repository per concern (polyrepo).** Better isolation and independent
  versioning, but heavier coordination for cross-cutting changes and a fragmented
  narrative — poor fit for a single-owner portfolio system meant to be read as a
  whole.
