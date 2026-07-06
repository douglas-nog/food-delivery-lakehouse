-- CDC setup: replication user and publication for Debezium.
-- The cdc_user password is passed at runtime via psql variable (:cdc_password),
-- never hardcoded here.

-- Replication user (idempotent)
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'cdc_user') THEN
    CREATE ROLE cdc_user WITH LOGIN PASSWORD :'cdc_password';
  END IF;
END
$$;

GRANT azure_pg_admin TO cdc_user;
ALTER ROLE cdc_user WITH REPLICATION;
GRANT CONNECT ON DATABASE delivery TO cdc_user;
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;

-- Publication (idempotent)
DROP PUBLICATION IF EXISTS delivery_pub;
CREATE PUBLICATION delivery_pub FOR TABLE customers, restaurants, products, orders, order_items;