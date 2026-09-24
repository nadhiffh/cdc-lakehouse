"""Assert the marts hold the row counts the source implies.

Independent of dbt on purpose. Every dbt test is a query against the warehouse,
so a warehouse that built the right *shape* out of the wrong *volume* can pass
them all: uniqueness holds trivially on an empty table, and referential tests
pass when both sides are empty. This pins the absolute numbers.
"""

from __future__ import annotations

import sys

import duckdb

from pipeline import config

# Verified against the source CSVs during profiling.
EXPECTED = {
    "landing.cdc_events":                    639_764,
    "main_marts.dim_customer":                96_355,
    "main_marts.dim_order_status_history":   394_713,
    "main_marts.dim_seller":                   3_095,
    "main_marts.dim_product":                 32_951,
    "main_marts.fct_orders":                  99_441,
    "main_marts.fct_order_items":            112_650,
}

# Business facts that must survive the whole pipeline unchanged.
INVARIANTS = [
    ("gross revenue to the cent",
     "select round(sum(price), 2) from main_marts.fct_order_items",
     13_591_643.70),
    ("orders with no line items",
     "select count_if(is_missing_items) from main_marts.fct_orders",
     775),
    ("customers carrying real SCD2 history",
     "select count(distinct customer_unique_id) from main_marts.dim_customer "
     "where has_history",
     252),
    ("fact rows resolving to a historical address version",
     "select count(*) from main_marts.fct_order_items f "
     "join main_marts.dim_customer c using (customer_key) where not c.is_current",
     317),
    ("customer versions with exactly one current row",
     "select count(*) from (select customer_unique_id from main_marts.dim_customer "
     "group by 1 having count_if(is_current) <> 1)",
     0),
    ("SCD2 windows running backwards",
     "select count(*) from main_marts.dim_order_status_history "
     "where valid_to < valid_from",
     0),
]


def main() -> int:
    con = duckdb.connect(str(config.WAREHOUSE), read_only=True)
    failures = []

    print("row counts:")
    for relation, expected in EXPECTED.items():
        actual = con.execute(f"select count(*) from {relation}").fetchone()[0]
        ok = actual == expected
        if not ok:
            failures.append(f"{relation}: {actual:,} rows, expected {expected:,}")
        print(f"  {relation:38s} {actual:>9,} {'ok' if ok else 'MISMATCH'}")

    print("\ninvariants:")
    for label, sql, expected in INVARIANTS:
        actual = con.execute(sql).fetchone()[0]
        actual = float(actual) if isinstance(expected, float) else actual
        ok = actual == expected
        if not ok:
            failures.append(f"{label}: got {actual:,}, expected {expected:,}")
        print(f"  {label:48s} {actual:>14,} {'ok' if ok else 'MISMATCH'}")

    con.close()

    if failures:
        print("\nFAILED:", file=sys.stderr)
        for f in failures:
            print(f"  {f}", file=sys.stderr)
        return 1

    print("\nall mart counts and invariants hold")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
