# Architecture Decision Records

Records of the significant architectural decisions in this project, in
[MADR](https://adr.github.io/madr/) format. Each captures the context, the
decision, its consequences, and the alternatives that were weighed.

| # | Decision |
|---|---|
| [0001](0001-monorepo-structure.md) | Monorepo with top-level separation by concern |
| [0002](0002-two-pipeline-split.md) | Separate ingestion and transformation pipelines |
| [0003](0003-silver-modeling.md) | Silver modeling: SCD per entity and a quarantine for invalid data |
| [0004](0004-order-enriched-streaming-table.md) | Build order_enriched from the append-only change feed |
| [0005](0005-debezium-decimal-string.md) | Handle Debezium decimals as strings, cast to DECIMAL |
| [0006](0006-reverse-etl-lakebase.md) | Reverse ETL to Lakebase; synced table created via CLI |
| [0007](0007-multi-environment-cicd.md) | Multi-environment CI/CD with a service principal |
