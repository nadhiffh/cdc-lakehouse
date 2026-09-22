"""Load the Olist CSVs into Postgres as the OLTP starting state.

Deliberately loads the *initial* state only:

  * order_status is reset to its pre-transition value, and the lifecycle
    timestamps after purchase are nulled out. replay.py then walks them
    forward, so Debezium captures real UPDATEs.
  * customer rows carry the address from each person's earliest order.
    Later orders with a different address become UPDATEs, which is what
    produces genuine SCD Type 2 history.

DuckDB does the CSV parsing and reshaping, then hands Postgres a binary COPY.
It is faster than row-by-row inserts and keeps the type coercion in one place.
"""

from __future__ import annotations

import argparse
import io
import sys

import duckdb
import psycopg

from pipeline import config


def _csv(name: str) -> str:
    path = config.RAW_DIR / f"{name}.csv"
    if not path.exists():
        sys.exit(f"missing {path}. run `make data` first.")
    return f"read_csv_auto('{path}', header=true)"


def build_frames(con: duckdb.DuckDBPyConnection) -> None:
    """Reshape the CSVs into the OLTP table layout."""
    con.execute(f"CREATE VIEW raw_orders    AS SELECT * FROM {_csv('olist_orders_dataset')}")
    con.execute(f"CREATE VIEW raw_customers AS SELECT * FROM {_csv('olist_customers_dataset')}")
    con.execute(f"CREATE VIEW raw_items     AS SELECT * FROM {_csv('olist_order_items_dataset')}")
    con.execute(f"CREATE VIEW raw_products  AS SELECT * FROM {_csv('olist_products_dataset')}")
    con.execute(f"CREATE VIEW raw_sellers   AS SELECT * FROM {_csv('olist_sellers_dataset')}")
    con.execute(f"CREATE VIEW raw_xlate     AS SELECT * FROM {_csv('product_category_name_translation')}")

    # One row per real person, holding their earliest known address.
    # ROW_NUMBER over purchase time picks the first; the 250 people whose zip
    # later changes become UPDATE events during replay.
    con.execute(
        """
        CREATE TABLE t_customer AS
        WITH ranked AS (
            SELECT c.customer_unique_id,
                   c.customer_zip_code_prefix AS zip_code_prefix,
                   c.customer_city  AS city,
                   c.customer_state AS state,
                   ROW_NUMBER() OVER (
                       PARTITION BY c.customer_unique_id
                       ORDER BY o.order_purchase_timestamp, o.order_id
                   ) AS rn
            FROM raw_customers c
            JOIN raw_orders o USING (customer_id)
        )
        SELECT customer_unique_id, zip_code_prefix, city, state
        FROM ranked WHERE rn = 1
        """
    )

    con.execute(
        """
        CREATE TABLE t_seller AS
        SELECT seller_id,
               seller_zip_code_prefix AS zip_code_prefix,
               seller_city  AS city,
               seller_state AS state
        FROM raw_sellers
        """
    )

    # 610 products have no category; left join keeps them with NULL rather
    # than dropping them.
    con.execute(
        """
        CREATE TABLE t_product AS
        SELECT p.product_id,
               p.product_category_name AS category_name,
               x.product_category_name_english AS category_name_en,
               CAST(p.product_weight_g AS INTEGER) AS weight_g,
               CAST(p.product_length_cm AS INTEGER) AS length_cm,
               CAST(p.product_height_cm AS INTEGER) AS height_cm,
               CAST(p.product_width_cm  AS INTEGER) AS width_cm
        FROM raw_products p
        LEFT JOIN raw_xlate x
               ON x.product_category_name = p.product_category_name
        """
    )

    # Orders rewound to their state at purchase time. 'created' is the initial
    # status for every order regardless of where it ended up.
    con.execute(
        """
        CREATE TABLE t_order AS
        SELECT o.order_id,
               o.customer_id,
               c.customer_unique_id,
               'created' AS order_status,
               o.order_purchase_timestamp      AS purchased_at,
               CAST(NULL AS TIMESTAMP)         AS approved_at,
               CAST(NULL AS TIMESTAMP)         AS delivered_carrier_at,
               CAST(NULL AS TIMESTAMP)         AS delivered_customer_at,
               o.order_estimated_delivery_date AS estimated_delivery_at
        FROM raw_orders o
        JOIN raw_customers c USING (customer_id)
        """
    )

    con.execute(
        """
        CREATE TABLE t_order_item AS
        SELECT order_id,
               CAST(order_item_id AS SMALLINT) AS order_item_id,
               product_id, seller_id,
               shipping_limit_date AS shipping_limit_at,
               CAST(price AS DECIMAL(10,2))         AS price,
               CAST(freight_value AS DECIMAL(10,2)) AS freight_value
        FROM raw_items
        """
    )


# Load order last among its dependencies: it FKs to customer.
LOAD_ORDER = [
    ("customer", "t_customer",
     ["customer_unique_id", "zip_code_prefix", "city", "state"]),
    ("seller", "t_seller",
     ["seller_id", "zip_code_prefix", "city", "state"]),
    ("product", "t_product",
     ["product_id", "category_name", "category_name_en",
      "weight_g", "length_cm", "height_cm", "width_cm"]),
    ("order", "t_order",
     ["order_id", "customer_id", "customer_unique_id", "order_status",
      "purchased_at", "approved_at", "delivered_carrier_at",
      "delivered_customer_at", "estimated_delivery_at"]),
    ("order_item", "t_order_item",
     ["order_id", "order_item_id", "product_id", "seller_id",
      "shipping_limit_at", "price", "freight_value"]),
]


def copy_into_postgres(duck: duckdb.DuckDBPyConnection, pg: psycopg.Connection) -> None:
    for table, source, cols in LOAD_ORDER:
        buf = io.StringIO()
        rows = duck.execute(f"SELECT {', '.join(cols)} FROM {source}").fetchall()
        for row in rows:
            buf.write(
                "\t".join(r"\N" if v is None else str(v) for v in row) + "\n"
            )
        buf.seek(0)
        collist = ", ".join(f'"{c}"' for c in cols)
        with pg.cursor() as cur:
            with cur.copy(
                f'COPY commerce."{table}" ({collist}) FROM STDIN'
            ) as copy:
                copy.write(buf.read())
        print(f"  {table:12s} {len(rows):>7,} rows")


def main() -> int:
    ap = argparse.ArgumentParser(description="seed Postgres with initial OLTP state")
    ap.add_argument("--truncate", action="store_true",
                    help="clear existing rows first (safe to rerun)")
    args = ap.parse_args()

    duck = duckdb.connect()
    build_frames(duck)

    with psycopg.connect(config.POSTGRES_DSN) as pg:
        if args.truncate:
            with pg.cursor() as cur:
                cur.execute(
                    'TRUNCATE commerce.order_item, commerce."order", '
                    "commerce.customer, commerce.seller, commerce.product CASCADE"
                )
        print("seeding Postgres:")
        copy_into_postgres(duck, pg)
        pg.commit()

        with pg.cursor() as cur:
            cur.execute(
                """
                SELECT (SELECT count(*) FROM commerce.customer),
                       (SELECT count(*) FROM commerce."order"),
                       (SELECT count(*) FROM commerce.order_item),
                       (SELECT count(*) FROM commerce."order" WHERE order_status <> 'created')
                """
            )
            cust, orders, items, not_created = cur.fetchone()

    print(f"\nseeded: {cust:,} customers, {orders:,} orders, {items:,} items")
    if not_created:
        print(f"WARNING: {not_created} orders are not in 'created' state", file=sys.stderr)
        return 1
    print("all orders start at 'created'; run `make replay` to stream transitions")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
