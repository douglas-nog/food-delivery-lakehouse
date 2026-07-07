#!/usr/bin/env bash
set -euo pipefail

# --- Load secrets ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/.env"

# --- Read Terraform outputs ---
cd "${SCRIPT_DIR}/../infra"
PG_FQDN=$(terraform output -raw postgres_fqdn)
VM_IP=$(terraform output -raw vm_public_ip)
cd "${SCRIPT_DIR}"

SSH_KEY="${HOME}/.ssh/food-delivery-vm"
PG_CONN="host=${PG_FQDN} port=5432 dbname=delivery user=${PG_ADMIN_USER} sslmode=require"
PG_CONN_ADMIN="host=${PG_FQDN} port=5432 dbname=postgres user=${PG_ADMIN_USER} sslmode=require"

echo ">> Creating database if not exists..."
PGPASSWORD="${PG_ADMIN_PASSWORD}" psql "${PG_CONN_ADMIN}" \
  -tc "SELECT 1 FROM pg_database WHERE datname='delivery'" | grep -q 1 || \
PGPASSWORD="${PG_ADMIN_PASSWORD}" psql "${PG_CONN_ADMIN}" \
  -c "CREATE DATABASE delivery"

echo ">> Applying schema..."
PGPASSWORD="${PG_ADMIN_PASSWORD}" psql "${PG_CONN}" -f schema.sql

echo ">> Setting up CDC user and publication..."
PGPASSWORD="${PG_ADMIN_PASSWORD}" psql "${PG_CONN}" \
  -v cdc_password="${CDC_USER_PASSWORD}" -f cdc-setup.sql

echo ">> Copying docker-compose to VM..."
scp -i "${SSH_KEY}" -o StrictHostKeyChecking=accept-new \
  docker-compose.yml azureuser@"${VM_IP}":~/

echo ">> Starting Kafka + Connect on VM..."
ssh -i "${SSH_KEY}" azureuser@"${VM_IP}" \
  "VM_PUBLIC_IP=${VM_IP} docker compose up -d"

echo ">> Waiting for Kafka Connect REST API..."
until ssh -i "${SSH_KEY}" azureuser@"${VM_IP}" \
  "curl -sf http://localhost:8083/connectors >/dev/null 2>&1"; do
  echo "   Connect not ready, waiting 5s..."
  sleep 5
done

echo ">> Generating connector config on VM..."
ssh -i "${SSH_KEY}" azureuser@"${VM_IP}" "cat > register-postgres.json" << JSON
{
  "name": "delivery-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "${PG_FQDN}",
    "database.port": "5432",
    "database.user": "cdc_user",
    "database.password": "${CDC_USER_PASSWORD}",
    "database.dbname": "delivery",
    "database.sslmode": "require",
    "topic.prefix": "delivery",
    "plugin.name": "pgoutput",
    "publication.name": "delivery_pub",
    "slot.name": "delivery_slot",
    "table.include.list": "public.customers,public.restaurants,public.products,public.orders,public.order_items"
  }
}
JSON

echo ">> Registering Debezium connector (with retry)..."
for i in {1..10}; do
  HTTP_CODE=$(ssh -i "${SSH_KEY}" azureuser@"${VM_IP}" \
    "curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8083/connectors \
     -H 'Content-Type: application/json' -d @register-postgres.json")
  if [ "${HTTP_CODE}" = "201" ] || [ "${HTTP_CODE}" = "409" ]; then
    echo "   Connector registered (HTTP ${HTTP_CODE})"
    break
  fi
  echo "   Attempt ${i}: HTTP ${HTTP_CODE}, retrying in 10s..."
  sleep 10
  if [ "${i}" = "10" ]; then
    echo "   ERROR: connector registration failed after 10 attempts"
    exit 1
  fi
done

echo ">> Verifying connector status..."
ssh -i "${SSH_KEY}" azureuser@"${VM_IP}" \
  "curl -s http://localhost:8083/connectors/delivery-connector/status"
echo ""

echo ">> Setup complete."