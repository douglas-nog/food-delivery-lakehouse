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
  "export VM_PUBLIC_IP=${VM_IP} && docker compose up -d"

echo ">> Waiting for Kafka Connect to be ready..."
until ssh -i "${SSH_KEY}" azureuser@"${VM_IP}" \
  "curl -sf http://localhost:8083/ >/dev/null 2>&1"; do
  sleep 5
done

echo ">> Registering Debezium connector..."
ssh -i "${SSH_KEY}" azureuser@"${VM_IP}" bash -s << EOF
cat > register-postgres.json << 'JSON'
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
curl -s -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @register-postgres.json
echo ""
EOF

echo ">> Setup complete."