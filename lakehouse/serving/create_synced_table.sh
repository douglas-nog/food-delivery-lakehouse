#!/usr/bin/env bash
set -euo pipefail

# Reverse ETL: sync the gold streaming table `order_enriched` into Lakebase.
#
# order_enriched is a STREAMING TABLE (not a materialized view), so standard
# CDF drives an incremental TRIGGERED sync without the Auto-CDF private preview
# that MV sources require.
#
# NOTE: Lakebase Autoscaling synced tables are not yet supported as a DAB
# resource (postgres_synced_tables) nor by the Terraform provider. Until that
# support lands, the synced table is created via the Databricks CLI. The
# Lakebase project itself IS declared in the bundle (postgres_projects).
# See docs/adr/ for the rationale.

PROFILE="${DATABRICKS_PROFILE:-adb-7405613325482314}"
PROJECT="food-delivery-serving"

databricks postgres create-synced-table \
  food_delivery.gold.order_enriched_synced \
  --profile "${PROFILE}" \
  --json '{
    "spec": {
      "source_table_full_name": "food_delivery.gold.order_enriched",
      "branch": "projects/'"${PROJECT}"'/branches/production",
      "primary_key_columns": ["order_id"],
      "scheduling_policy": "TRIGGERED",
      "postgres_database": "databricks_postgres",
      "create_database_objects_if_missing": true,
      "new_pipeline_spec": {
        "storage_catalog": "food_delivery",
        "storage_schema": "gold"
      }
    }
  }'