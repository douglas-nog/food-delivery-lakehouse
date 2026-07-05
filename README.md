# food-delivery-lakehouse

Event-driven lakehouse on Databricks with real CDC from PostgreSQL via Debezium.
A food-delivery domain streams change events (Postgres → Debezium → Kafka →
Databricks) through a medallion architecture, exercising CDC, CDF, Lakeflow
Declarative Pipelines, SCD, and liquid clustering.

Work in progress. Infrastructure is provisioned via Terraform (ephemeral).