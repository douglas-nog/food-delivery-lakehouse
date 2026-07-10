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
| Load generator (simulated OLTP writes) | ⬜ |
| Automate Kafka via cloud-init (tech debt) | ⬜ |

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
| shared schemas packaged as wheel (food_delivery_shared) | ✅ |
| Gold: 3 materialized views with joins | ✅ |
| Liquid clustering on gold MVs | ✅ |
| Broadcast hints on dimension joins | ✅ |
| Change Data Feed enabled on gold (for reverse ETL) | ✅ |

## v4 — Operational serving (reverse ETL → Lakebase)

| Item | Status |
|---|---|
| Lakebase project (serverless Postgres) | ✅ |
| Synced table: gold → Lakebase (reverse ETL, CDF-driven) | ✅ |
| Validate low-latency serving from Lakebase | 🔶 |

## v5 — Platform hardening (DABs + CI/CD)

| Item | Status |
|---|---|
| Databricks Asset Bundle (bronze + transform) | 🔶 |
| Bundle: variable injection (kafka.bootstrap, no secrets) | ✅ |
| Git Flow (feature → develop → homolog → main) | 🔶 |
| Lakeflow Job coordinating bronze + transform | ⬜ |
| CI/CD: GitHub Actions | ⬜ |
| Multi-target (dev / homolog / prod) | ⬜ |

## v6 — Documentation & polish

| Item | Status |
|---|---|
| ADRs (monorepo, 2-pipeline split, SCD-per-entity, reverse ETL) | ⬜ |
| README with architecture + diagrams | ⬜ |
| Workspace cleanup: keep only bundle-deployed assets | ⬜ |
