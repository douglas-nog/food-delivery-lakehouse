# 6. Reverse ETL to Lakebase; synced table created via CLI

Date: 2026-07

## Status

Accepted

## Context

The platform closes an OLTP → OLAP → OLTP loop: after modeling data analytically,
it serves current order state back to an operational store for low-latency
application access. Lakebase (serverless Postgres integrated with Databricks) is
the serving target.

Two sub-decisions: how to declare the Lakebase project, and how to create the
synced table that mirrors the gold serving table into Postgres. At the time of
implementation, a declarative bundle/Terraform resource for Autoscaling synced
tables was not yet available, while the Lakebase project itself is declarable.

## Decision

Declare the **Lakebase project in the bundle** (`postgres_projects`), and create
the **synced table via the Databricks CLI**, driven by a parameterized script
(`create_synced_table.sh <env>`) in incremental `TRIGGERED` mode. The Provisioned
tier is avoided in favor of Autoscaling.

## Consequences

- The serving loop is complete: applications query current order state directly
  from Postgres, with no analytical joins at request time.
- Most of the serving infrastructure is declarative; the one imperative step (the
  synced table) is scripted, parameterized per environment, and documented, so the
  IaC gap is contained and reproducible.
- When a declarative resource for Autoscaling synced tables becomes available, the
  script can be replaced by a bundle resource with no change to the surrounding
  design.
- The generated sync pipeline id is not known until creation, so it is injected at
  deploy time rather than committed (see the CI/CD decision).

## Alternatives considered

- **Provisioned Lakebase tier.** Being deprecated; Autoscaling is the forward
  path.
- **Waiting for full declarative support.** Would block the serving loop
  indefinitely; scripting the gap keeps the project moving while staying explicit
  about it.
