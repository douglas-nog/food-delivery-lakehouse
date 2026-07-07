# Project Roadmap — Food Delivery Lakehouse (CDC)

Event-driven lakehouse: PostgreSQL → Debezium → Kafka → Databricks, through a
medallion architecture with Lakeflow Declarative Pipelines.

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
| Load generator (simulated OLTP writes) | ⬜ |
| Validate CDC flowing (Postgres → Kafka topics) | ✅ |
| Automate VM provisioning via cloud-init (tech debt) | ✅ |

## v2 — Databricks ingestion (bronze)

| Item | Status |
|---|---|
| Databricks reads Kafka via Structured Streaming | ✅ |
| Bronze: raw CDC change events (streaming tables) | ✅ |
| Lakeflow Declarative Pipeline scaffold | ✅ |

## v3 — Transform (silver + gold)

| Item | Status |
|---|---|
| Silver: APPLY CHANGES (SCD Type 1/2) | ⬜ |
| Silver: data quality expectations | ⬜ |
| Silver: joins across entities | ⬜ |
| Gold: business aggregates (materialized views) | ⬜ |
| Liquid clustering on target tables | ⬜ |
| Change Data Feed between layers | ⬜ |

## v4 — Platform hardening

| Item | Status |
|---|---|
| Databricks Asset Bundle (dev/stg/prod) | ⬜ |
| CI/CD: GitHub Actions + Git Flow | ⬜ |
| Unit tests gating merges | ⬜ |
| ADRs (monorepo, CDC choice, namespace) | ⬜ |
| README with architecture + diagrams | ⬜ |

## Concepts to demonstrate (certification-aligned)

CDC · CDF · Lakeflow Declarative Pipelines (LDP) · Spark Declarative Pipelines
(SDP) · APPLY CHANGES / SCD · expectations · liquid clustering · streaming
tables · materialized views.

## Design decisions

- Monorepo with top-level separation (infra / source-system / lakehouse).
- Ephemeral cloud infra via Terraform (apply → test → destroy).
- Public IP + authenticated Kafka (trial pragmatism; production would use private networking).