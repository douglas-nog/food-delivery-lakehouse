"""
Load generator for the food delivery source system.

Simulates a real OLTP workload against Postgres so the CDC pipeline has a
continuous, realistic change stream to process:

  - seeds reference data (customers, restaurants, products) once, idempotently
  - creates a full order every INTERVAL_SECONDS (order + order_items, priced
    from the chosen restaurant's own menu)
  - advances the status of open orders (created -> confirmed -> preparing ->
    delivered / cancelled), producing UPDATE events that exercise SCD Type 2
  - occasionally mutates dimensions (product price, restaurant availability) so
    the SCD Type 2 history on those entities is non-trivial
  - injects a configurable share of anomalies (ANOMALY_RATE, default 5%) to
    exercise the silver-layer expectations and the orders quarantine

The domain rules (order total, anomaly injection, status lifecycle, price
adjustment, deactivation guard) live in food_delivery_shared.load_logic as pure
functions, so they are unit-tested without a database. This module orchestrates
those rules around the actual SQL.

Anomalies are limited to what the schema actually allows: total_amount has no
CHECK constraint (so a negative total is possible, mimicking a pricing bug) and
status is a free VARCHAR (so an unknown status is possible, mimicking a value
leaking in from an upstream integration).
"""

import logging
import os
import random
import signal
import sys
import time
from decimal import Decimal

import psycopg2
from psycopg2.extras import execute_values

from food_delivery_shared import load_logic as logic

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("load-generator")

PG_HOST = os.environ["PG_HOST"]
PG_DB = os.environ.get("PG_DB", "delivery")
PG_USER = os.environ["PG_USER"]
PG_PASSWORD = os.environ["PG_PASSWORD"]

INTERVAL_SECONDS = float(os.environ.get("INTERVAL_SECONDS", "10"))
ANOMALY_RATE = float(os.environ.get("ANOMALY_RATE", "0.05"))

# --- Seed data (realistic Brazilian delivery domain) -------------------------

CUSTOMERS = [
    ("Ana Silva", "ana.silva@example.com", "44991234501", "Campo Mourao"),
    ("Bruno Costa", "bruno.costa@example.com", "44991234502", "Maringa"),
    ("Carla Souza", "carla.souza@example.com", "44991234503", "Curitiba"),
    ("Diego Almeida", "diego.almeida@example.com", "44991234504", "Londrina"),
    ("Elisa Rocha", "elisa.rocha@example.com", "44991234505", "Campo Mourao"),
    ("Felipe Lima", "felipe.lima@example.com", "44991234506", "Maringa"),
    ("Gabriela Nunes", "gabriela.nunes@example.com", "44991234507", "Curitiba"),
    ("Henrique Dias", "henrique.dias@example.com", "44991234508", "Cascavel"),
    ("Isabela Martins", "isabela.martins@example.com", "44991234509", "Londrina"),
    ("Joao Pereira", "joao.pereira@example.com", "44991234510", "Campo Mourao"),
    ("Karina Freitas", "karina.freitas@example.com", "44991234511", "Maringa"),
    ("Lucas Barbosa", "lucas.barbosa@example.com", "44991234512", "Curitiba"),
]

# restaurant -> (category, city, menu[(product, price)])
RESTAURANTS = [
    (
        "Cantina Bella", "Italiana", "Campo Mourao",
        [("Lasanha Bolonhesa", "48.90"), ("Spaghetti Carbonara", "42.50"),
         ("Nhoque ao Sugo", "39.90"), ("Tiramisu", "22.00")],
    ),
    (
        "Sushi Kaze", "Japonesa", "Maringa",
        [("Combinado 20 pecas", "89.90"), ("Temaki Salmao", "32.00"),
         ("Hot Roll", "28.50"), ("Yakisoba", "45.00")],
    ),
    (
        "Burger House", "Hamburgueria", "Curitiba",
        [("Cheeseburger Duplo", "36.90"), ("Bacon Supreme", "41.50"),
         ("Batata Rustica", "18.00"), ("Milkshake Chocolate", "21.90")],
    ),
    (
        "Cantinho Mineiro", "Brasileira", "Londrina",
        [("Feijoada Completa", "52.00"), ("Frango com Quiabo", "44.90"),
         ("Tutu de Feijao", "38.00"), ("Pudim de Leite", "16.50")],
    ),
    (
        "Pizza Nostra", "Pizzaria", "Campo Mourao",
        [("Pizza Margherita", "54.90"), ("Pizza Calabresa", "49.90"),
         ("Pizza Portuguesa", "56.00"), ("Refrigerante 2L", "12.00")],
    ),
    (
        "Green Bowl", "Saudavel", "Cascavel",
        [("Bowl de Quinoa", "34.90"), ("Salada Caesar", "29.90"),
         ("Wrap Vegano", "31.50"), ("Suco Detox", "14.90")],
    ),
]


def connect():
    return psycopg2.connect(
        host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
        sslmode="require", connect_timeout=10,
    )


def seed(conn):
    """Populate reference data once. Safe to run repeatedly."""
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM customers")
        if cur.fetchone()[0] > 0:
            log.info("seed skipped (reference data already present)")
            return

        log.info("seeding reference data")
        execute_values(
            cur,
            "INSERT INTO customers (full_name, email, phone, city) VALUES %s",
            CUSTOMERS,
        )

        for name, category, city, menu in RESTAURANTS:
            cur.execute(
                "INSERT INTO restaurants (name, category, city) VALUES (%s, %s, %s) "
                "RETURNING restaurant_id",
                (name, category, city),
            )
            restaurant_id = cur.fetchone()[0]
            execute_values(
                cur,
                "INSERT INTO products (restaurant_id, name, price) VALUES %s",
                [(restaurant_id, p, Decimal(price)) for p, price in menu],
            )

        conn.commit()
        log.info(
            "seeded %d customers, %d restaurants",
            len(CUSTOMERS), len(RESTAURANTS),
        )


def create_order(conn):
    """Create one order with its items, priced from the restaurant's menu."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
        row = cur.fetchone()
        if row is None:
            log.warning("no customers available; skipping order")
            return
        customer_id = row[0]

        cur.execute(
            "SELECT restaurant_id FROM restaurants WHERE is_active "
            "ORDER BY random() LIMIT 1"
        )
        row = cur.fetchone()
        if row is None:
            log.warning("no active restaurants; skipping order")
            return
        restaurant_id = row[0]

        # Only products from THIS restaurant, so the order is internally coherent.
        cur.execute(
            "SELECT product_id, price FROM products "
            "WHERE restaurant_id = %s AND is_available "
            "ORDER BY random() LIMIT %s",
            (restaurant_id, random.randint(1, 3)),
        )
        picked = cur.fetchall()
        if not picked:
            log.warning("restaurant %s has no available products",
                        restaurant_id)
            return

        items = [(pid, random.randint(1, 3), price) for pid, price in picked]

        # Domain rules (pure, unit-tested in load_logic).
        total = logic.order_total(items)
        status = "created"
        anomaly = logic.choose_anomaly(random.random(), ANOMALY_RATE)
        unknown = random.choice(logic.UNKNOWN_STATUSES)
        total, status = logic.apply_anomaly(total, status, anomaly, unknown)

        cur.execute(
            "INSERT INTO orders (customer_id, restaurant_id, status, total_amount) "
            "VALUES (%s, %s, %s, %s) RETURNING order_id",
            (customer_id, restaurant_id, status, total),
        )
        order_id = cur.fetchone()[0]

        execute_values(
            cur,
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price) "
            "VALUES %s",
            [(order_id, pid, qty, price) for pid, qty, price in items],
        )
        conn.commit()

        log.info(
            "order %s created (restaurant=%s items=%d total=%s%s)",
            order_id, restaurant_id, len(items), total,
            f" ANOMALY={anomaly}" if anomaly else "",
        )


def advance_statuses(conn, batch=3):
    """Move open orders along the lifecycle, producing UPDATE change events."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT order_id, status FROM orders "
            "WHERE status IN ('created', 'confirmed', 'preparing') "
            "ORDER BY random() LIMIT %s",
            (batch,),
        )
        for order_id, status in cur.fetchall():
            # A small share of orders is cancelled instead of progressing.
            new_status = logic.next_status(
                status, cancel=random.random() < 0.08)
            cur.execute(
                "UPDATE orders SET status = %s, updated_at = now() "
                "WHERE order_id = %s",
                (new_status, order_id),
            )
            log.info("order %s: %s -> %s", order_id, status, new_status)
        conn.commit()


def mutate_dimensions(conn):
    """Occasionally change dimension data, so SCD Type 2 history is meaningful."""
    with conn.cursor() as cur:
        if random.random() < 0.5:
            # Price adjustment (+/- up to 10%).
            cur.execute(
                "SELECT product_id, price FROM products ORDER BY random() LIMIT 1"
            )
            row = cur.fetchone()
            if row is None:
                log.warning("no products available; skipping dimension change")
                return
            product_id, price = row

            new_price = logic.adjusted_price(
                price, round(random.uniform(0.90, 1.10), 2)
            )
            cur.execute(
                "UPDATE products SET price = %s, updated_at = now() "
                "WHERE product_id = %s",
                (new_price, product_id),
            )
            log.info("product %s: price %s -> %s",
                     product_id, price, new_price)
        else:
            # Toggle a restaurant's availability, but never leave the platform
            # with fewer than MIN_ACTIVE_RESTAURANTS open, which would starve
            # order creation.
            cur.execute("SELECT count(*) FROM restaurants WHERE is_active")
            active = cur.fetchone()[0]

            cur.execute(
                "SELECT restaurant_id, is_active FROM restaurants "
                "ORDER BY random() LIMIT 1"
            )
            row = cur.fetchone()
            if row is None:
                log.warning(
                    "no restaurants available; skipping dimension change")
                return
            restaurant_id, is_active = row

            if not logic.can_deactivate(is_active, active, logic.MIN_ACTIVE_RESTAURANTS):
                log.info(
                    "skipping deactivation (only %d active restaurants)", active)
            else:
                cur.execute(
                    "UPDATE restaurants SET is_active = %s, updated_at = now() "
                    "WHERE restaurant_id = %s",
                    (not is_active, restaurant_id),
                )
                log.info(
                    "restaurant %s: is_active %s -> %s",
                    restaurant_id, is_active, not is_active,
                )
        conn.commit()


class Shutdown:
    stop = False

    def __init__(self):
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, *_):
        log.info("shutdown requested")
        self.stop = True


def main():
    shutdown = Shutdown()
    conn = connect()
    log.info(
        "connected to %s/%s (interval=%ss anomaly_rate=%.0f%%)",
        PG_HOST, PG_DB, INTERVAL_SECONDS, ANOMALY_RATE * 100,
    )

    seed(conn)

    tick = 0
    while not shutdown.stop:
        try:
            create_order(conn)

            # Statuses advance on every other tick; dimensions change rarely.
            if tick % 2 == 0:
                advance_statuses(conn)
            if tick % 12 == 0 and tick > 0:
                mutate_dimensions(conn)

            tick += 1
        except psycopg2.Error as exc:
            log.error("database error: %s", exc)
            conn.rollback()
            time.sleep(5)
            if conn.closed:
                conn = connect()
                log.info("reconnected")

        time.sleep(INTERVAL_SECONDS)

    conn.close()
    log.info("stopped")


if __name__ == "__main__":
    main()
