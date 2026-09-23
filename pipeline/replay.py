"""Replay the order lifecycle and customer address changes as real UPDATEs.

Every UPDATE here is backed by a timestamp that exists in the source data, so
the resulting Kafka stream is a faithful change history rather than synthetic
churn.

Profiling drove three decisions:

  * 1,359 orders record `shipped` before `approved` (worst case 171 days
    backwards). Replaying strictly by timestamp would emit transitions out of
    order and produce negative-width SCD2 windows downstream. Events are
    therefore ordered by (order_id, lifecycle position) and the wall-clock
    timestamp is carried as data, with the inversion flagged rather than
    silently "corrected".
  * 1,296 orders have approved_at exactly equal to purchased_at. A dimension
    keyed on timestamp alone would collapse these; SCD2 downstream breaks ties
    on lifecycle position.
  * 6 canceled orders still have a delivery timestamp. Cancellation is applied
    as the final transition so the terminal status matches the source.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict

import duckdb
import psycopg

from pipeline import config


def load_transitions(limit_orders: int | None) -> list[tuple]:
    """Read the target end-state from the CSV and derive the event list."""
    con = duckdb.connect()
    orders_csv = config.RAW_DIR / "olist_orders_dataset.csv"
    con.execute(
        f"CREATE VIEW o AS SELECT * FROM read_csv_auto('{orders_csv}', header=true)"
    )

    sample = ""
    if limit_orders:
        # Deterministic subset: hash the id so the sample is stable across runs
        # and independent of file order.
        sample = f"WHERE hash(order_id) % 100 < {max(1, min(100, limit_orders))}"

    rows = con.execute(
        f"""
        SELECT order_id,
               order_status,
               order_approved_at,
               order_delivered_carrier_date,
               order_delivered_customer_date
        FROM o
        {sample}
        ORDER BY order_purchase_timestamp, order_id
        """
    ).fetchall()
    return rows


def build_events(rows: list[tuple]) -> list[dict]:
    """Expand each order into its ordered sequence of state transitions."""
    events: list[dict] = []
    stats = defaultdict(int)

    for order_id, final_status, approved, shipped, delivered in rows:
        seq = 0
        prev_ts = None

        for ts, status in (
            (approved, "approved"),
            (shipped, "shipped"),
            (delivered, "delivered"),
        ):
            if ts is None:
                continue
            seq += 1
            if prev_ts is not None and ts < prev_ts:
                stats["out_of_order"] += 1
            prev_ts = ts
            events.append(
                {
                    "order_id": order_id,
                    "seq": seq,
                    "status": status,
                    "ts": ts,
                }
            )
            stats[f"to_{status}"] += 1

        # The status implied by the timestamps is not always the status the
        # source actually records. 314 invoiced and 301 processing orders carry
        # an approved_at, and 8 delivered orders have no delivery timestamp at
        # all. Emitting a final transition whenever the derived status differs
        # keeps the replayed end state reconciling exactly with the source.
        derived = events[-1]["status"] if seq else "created"
        if final_status != derived:
            seq += 1
            events.append(
                {
                    "order_id": order_id,
                    "seq": seq,
                    "status": final_status,
                    "ts": None,  # no backing timestamp exists for this hop
                }
            )
            stats[f"to_{final_status}"] += 1
            stats["status_not_implied_by_timestamps"] += 1

    return events, stats


def address_changes(limit_orders: int | None) -> list[tuple]:
    """Customers whose address differs on a later order than their first.

    These are the events that give dim_customer real SCD Type 2 history:
    250 zip changes and 122 city changes among 2,997 repeat customers.
    """
    con = duckdb.connect()
    con.execute(
        f"CREATE VIEW c AS SELECT * FROM read_csv_auto('{config.RAW_DIR / 'olist_customers_dataset.csv'}', header=true)"
    )
    con.execute(
        f"CREATE VIEW o AS SELECT * FROM read_csv_auto('{config.RAW_DIR / 'olist_orders_dataset.csv'}', header=true)"
    )

    sample = ""
    if limit_orders:
        sample = f"AND hash(o.order_id) % 100 < {max(1, min(100, limit_orders))}"

    return con.execute(
        f"""
        WITH hist AS (
            SELECT c.customer_unique_id,
                   c.customer_zip_code_prefix AS zip,
                   c.customer_city  AS city,
                   c.customer_state AS state,
                   o.order_purchase_timestamp AS ts,
                   ROW_NUMBER() OVER (
                       PARTITION BY c.customer_unique_id
                       ORDER BY o.order_purchase_timestamp, o.order_id
                   ) AS rn
            FROM c JOIN o USING (customer_id)
            WHERE 1 = 1 {sample}
        ),
        changed AS (
            SELECT h.*,
                   LAG(zip)   OVER w AS prev_zip,
                   LAG(city)  OVER w AS prev_city,
                   LAG(state) OVER w AS prev_state
            FROM hist h
            WINDOW w AS (PARTITION BY customer_unique_id ORDER BY rn)
        )
        SELECT customer_unique_id, zip, city, state, ts
        FROM changed
        WHERE prev_zip IS NOT NULL
          AND (zip <> prev_zip OR city <> prev_city OR state <> prev_state)
        ORDER BY ts
        """
    ).fetchall()


def expected_status_counts(limit_orders: int | None) -> dict[str, int]:
    """Terminal status counts straight from the source, for reconciliation."""
    con = duckdb.connect()
    con.execute(
        f"CREATE VIEW o AS SELECT * FROM read_csv_auto('{config.RAW_DIR / 'olist_orders_dataset.csv'}', header=true)"
    )
    where = ""
    if limit_orders:
        where = f"WHERE hash(order_id) % 100 < {max(1, min(100, limit_orders))}"
    return dict(
        con.execute(
            f"SELECT order_status, count(*) FROM o {where} GROUP BY 1"
        ).fetchall()
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="replay CDC changes into Postgres")
    ap.add_argument("--sample-pct", type=int, default=None,
                    help="replay only N%% of orders (deterministic, for smoke tests)")
    ap.add_argument("--batch", type=int, default=5000,
                    help="rows per transaction")
    args = ap.parse_args()

    rows = load_transitions(args.sample_pct)
    events, stats = build_events(rows)
    addrs = address_changes(args.sample_pct)

    print(f"replaying {len(events):,} order transitions "
          f"and {len(addrs):,} address changes")
    if stats.get("out_of_order"):
        print(f"  note: {stats['out_of_order']:,} transitions carry a timestamp "
              f"earlier than the one before them (source defect, flagged not fixed)")

    with psycopg.connect(config.POSTGRES_DSN) as pg:
        with pg.cursor() as cur:
            # Order transitions. Each UPDATE sets the status and the timestamp
            # column the transition corresponds to.
            col_for = {
                "approved": "approved_at",
                "shipped": "delivered_carrier_at",
                "delivered": "delivered_customer_at",
            }
            done = 0
            for i in range(0, len(events), args.batch):
                chunk = events[i:i + args.batch]
                for ev in chunk:
                    col = col_for.get(ev["status"])
                    # updated_at carries the business instant of the transition,
                    # never now(). Debezium's own ts_ms records when the
                    # connector saw the event, and this replay compresses two
                    # years of history into under a minute of wall clock, so
                    # wall-clock stamps would collapse every SCD2 window to
                    # zero width downstream.
                    if col and ev["ts"] is not None:
                        cur.execute(
                            f'UPDATE commerce."order" '
                            f"SET order_status = %s, {col} = %s, updated_at = %s "
                            f"WHERE order_id = %s",
                            (ev["status"], ev["ts"], ev["ts"], ev["order_id"]),
                        )
                    else:
                        # No backing timestamp for this hop (invoiced,
                        # processing, or a terminal cancellation). Keep the
                        # previous business time rather than jumping to wall
                        # clock; ordering still comes from the LSN.
                        cur.execute(
                            'UPDATE commerce."order" '
                            "SET order_status = %s, updated_at = GREATEST("
                            "  updated_at, COALESCE(delivered_customer_at,"
                            "  delivered_carrier_at, approved_at, purchased_at)) "
                            "WHERE order_id = %s",
                            (ev["status"], ev["order_id"]),
                        )
                pg.commit()
                done += len(chunk)
                print(f"  transitions {done:,}/{len(events):,}", end="\r", flush=True)
            print()

            # ts is the purchase instant of the order that first carried the new
            # address, so updated_at is the business time the customer moved.
            for uid, zip_code, city, state, ts in addrs:
                cur.execute(
                    "UPDATE commerce.customer "
                    "SET zip_code_prefix = %s, city = %s, state = %s, updated_at = %s "
                    "WHERE customer_unique_id = %s",
                    (zip_code, city, state, ts, uid),
                )
            pg.commit()
            print(f"  address changes {len(addrs):,} applied")

        with pg.cursor() as cur:
            cur.execute(
                'SELECT order_status, count(*) FROM commerce."order" '
                "GROUP BY 1 ORDER BY 2 DESC"
            )
            actual = dict(cur.fetchall())

    # The replayed end state must match the source exactly. Without this check
    # the first version of this script silently lost all 314 invoiced and 301
    # processing orders, because it derived the terminal status from timestamps
    # and the source does not always agree with them.
    expected = expected_status_counts(args.sample_pct)
    print("\nfinal status distribution (replayed vs source):")
    drift = 0
    for status in sorted(set(expected) | set(actual)):
        exp, act = expected.get(status, 0), actual.get(status, 0)
        mark = "" if exp == act else f"  <-- drift {act - exp:+,}"
        drift += abs(act - exp)
        print(f"  {status:12s} {act:>7,} vs {exp:>7,}{mark}")

    if drift:
        print(f"\nFAILED: replayed state differs from source in {drift:,} orders",
              file=sys.stderr)
        return 1
    print("\nreplayed state reconciles exactly with the source")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
