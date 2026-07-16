# Project Roadmap — Food Delivery Lakehouse (CDC)

Event-driven lakehouse: PostgreSQL → Debezium → Kafka → Databricks, through a
medallion architecture with Lakeflow Declarative Pipelines, closing the loop
back to an operational store (Lakebase) via reverse ETL.

Status: ✅ done · 🔶 partial · ⬜ pending

## v1 — Source system + CDC infrastructure

| Item | Status |
|---|---|
| Terraform: resource group | ✅ |
| Terraform: PostgreSQL Flexible Server (logical replication) | ✅ |
| Postgres CDC params (wal_level, slots, senders, workers) | ✅ |
| Domain schema: 5 tables + REPLICA IDENTITY FULL | ✅ |
| CDC replication user + publication | ✅ |
| Terraform: VM (Kafka + Debezium host) | ✅ |
| Dedicated SSH key for VM | ✅ |
| Docker Compose: Kafka + Kafka Connect + Debezium | ✅ |
| Validate CDC flowing (Postgres → Kafka topics) | ✅ |
| Automated setup (apply.sh + setup.sh) | ✅ |
| Load generator (realistic OLTP traffic + anomalies) | ✅ |

## v2 — Databricks ingestion (bronze)

| Item | Status |
|---|---|
| Databricks reads Kafka via Structured Streaming | ✅ |
| Bronze: 5 raw CDC streaming tables (factory loop) | ✅ |
| Lakeflow Declarative Pipeline (bronze) | ✅ |

## v3 — Transform (silver + gold)

| Item | Status |
|---|---|
| Silver: AUTO CDC (SCD Type 1 customers, Type 2 others) | ✅ |
| Silver: data quality expectations (drop + warn) | ✅ |
| Silver: orders quarantine pattern | ✅ |
| Debezium decimal handling (string mode) + exact DECIMAL cast | ✅ |
| shared package as wheel (schemas + pure domain logic) | ✅ |
| Gold: 3 materialized views with joins | ✅ |
| Gold: order_enriched streaming table (denormalized serving view) | ✅ |
| Liquid clustering on gold tables | ✅ |
| Broadcast hints on dimension joins | ✅ |
| Change Data Feed enabled on gold (for reverse ETL) | ✅ |

## v4 — Operational serving (reverse ETL → Lakebase)

| Item | Status |
|---|---|
| Lakebase project (serverless Postgres, declared in bundle) | ✅ |
| Synced table: gold → Lakebase (TRIGGERED, CDF-driven, via CLI) | ✅ |
| Validate low-latency serving from Lakebase (psql) | ✅ |
| End-to-end validation with zero manual intervention | ✅ |

## v5 — Platform hardening (DABs + CI/CD)

| Item | Status |
|---|---|
| Databricks Asset Bundle (bronze + transform + job + Lakebase) | ✅ |
| Bundle: variable injection (kafka_bootstrap, synced_table_pipeline_id) | ✅ |
| Bundle: presets (tags, per-target name_prefix, root_path) | ✅ |
| Lakeflow Job: bronze → transform → Lakebase sync | ✅ |
| Job schedule (hourly, active in prod only) | ✅ |
| Unit tests (pytest) + lint (ruff) | ✅ |
| Git Flow (feature → develop → homolog → main) | ✅ |
| CI: lint + tests + bundle validate on PRs | ✅ |
| CD: deploy per branch, workflow_dispatch, prod approval gate | ✅ |
| Service principal deploy identity (OAuth M2M) | ✅ |
| Multi-target (dev / stg / prod, catalog + Lakebase per env) | ✅ |

## v6 — Documentation & polish

| Item | Status |
|---|---|
| README with architecture + design decisions | ✅ |
| ADRs (monorepo, 2-pipeline split, SCD-per-entity, reverse ETL, IaC gap) | ✅ |
| Remove tfstate from version control (tech debt) | ✅ |
| Workspace cleanup: keep only bundle-deployed assets | ✅ |

## Concepts demonstrated (certification-aligned)

CDC · CDF · Lakeflow Declarative Pipelines (LDP) · Spark Declarative Pipelines
(SDP) · AUTO CDC / SCD Type 1 & 2 · expectations · quarantine · liquid
clustering · streaming tables · materialized views · stream-static join ·
reverse ETL · Lakebase (LTAP) · Lakeflow Jobs orchestration · DABs ·
multi-environment CI/CD · service principal deploys · unit testing.

## Architecture

```
Postgres OLTP → Debezium → Kafka → [bronze → silver → gold] → Lakebase → apps
                (CDC)                 Lakehouse (LDP)          (reverse ETL)

Orchestration: Lakeflow Job (bronze → transform → sync), all declared in DABs.
Delivery: GitHub Actions CI/CD across dev / stg / prod, deployed by a service principal.
```

## Design decisions

- Monorepo with top-level separation (infra / source-system / lakehouse).
- Ephemeral cloud infra via Terraform (apply → test → destroy).
- Two LDP pipelines: bronze (ingestion) + transform (silver+gold), per Databricks best practice.
- shared package as a wheel: schemas + pure domain logic, unit-tested and imported deterministically.
- Debezium decimal.handling.mode=string: NUMERIC arrives as exact decimal strings, cast to
  DECIMAL in silver — floating point is never used for money.
- Public IP + PLAINTEXT Kafka (trial pragmatism; production would use private networking + SASL/TLS).
- CDF on gold enables reverse ETL to Lakebase (operational serving, closing OLTP→OLAP→OLTP).
- order_enriched is a streaming table fed by the append-only change feed via AUTO CDC: the silver
  tables it derives from receive MERGEs (not append-only), so reading them as a stream would fail;
  the change feed keeps it CDF-compatible for incremental Lakebase sync.
- Lakebase project declared in the bundle; synced table created via CLI (DAB/Terraform
  resources for Autoscaling synced tables are not yet available).
- Triggered pipeline mode (not continuous): enables task chaining in the Job and controls cost.
- One catalog + one Lakebase project per environment; shared Kafka source across environments
  (isolated by per-table checkpoints).
- Config injected at deploy time (Terraform outputs locally, GitHub environment variables in CI/CD),
  never committed; deploys run as a service principal, not a personal identity.
- Per-target name_prefix + root_path: unique resource names and isolated state across the three
  environments in a single workspace.