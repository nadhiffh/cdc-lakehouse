"""Block until Debezium has caught up.

Needed because `make rebuild` chains steps that are asynchronous at the edges:
registering the connector starts a snapshot in the background, and the replay's
UPDATEs reach Kafka some time after Postgres commits them. Without a barrier the
next step races ahead and reconciles against partial state, which looks like a
data bug and is not one.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time

import psycopg

from pipeline import config

# Expected snapshot sizes, which equal the live row counts in Postgres.
KAFKA_CONTAINER = "cdc-kafka"


def topic_totals() -> dict[str, int]:
    spec = ",".join(
        f"{config.TOPIC_PREFIX}.{config.SOURCE_SCHEMA}.{t}:0" for t in config.CDC_TABLES
    )
    proc = subprocess.run(
        ["docker", "exec", KAFKA_CONTAINER, "/opt/kafka/bin/kafka-get-offsets.sh",
         "--bootstrap-server", "localhost:9092", "--topic-partitions", spec],
        capture_output=True, text=True,
    )
    out: dict[str, int] = {}
    for line in proc.stdout.splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3:
            out[parts[0].split(".")[-1]] = int(parts[2])
    return out


def postgres_expected() -> dict[str, int]:
    """Rows plus updates: the number of events each table should have produced."""
    with psycopg.connect(config.POSTGRES_DSN) as pg, pg.cursor() as cur:
        expected = {}
        for table in config.CDC_TABLES:
            cur.execute(f'SELECT count(*) FROM commerce."{table}"')
            rows = cur.fetchone()[0]
            cur.execute(
                "SELECT COALESCE(n_tup_upd, 0) FROM pg_stat_user_tables "
                "WHERE schemaname = 'commerce' AND relname = %s",
                (table,),
            )
            row = cur.fetchone()
            expected[table] = rows + (row[0] if row else 0)
        return expected


def replication_lag() -> int | None:
    with psycopg.connect(config.POSTGRES_DSN) as pg, pg.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), "
            "confirmed_flush_lsn), 0)::bigint FROM pg_replication_slots "
            "WHERE slot_name = 'dbz_commerce_slot'"
        )
        row = cur.fetchone()
        return row[0] if row else None


def main() -> int:
    ap = argparse.ArgumentParser(description="wait for Debezium to catch up")
    ap.add_argument("--for", dest="phase", choices=["snapshot", "stream"],
                    required=True)
    ap.add_argument("--timeout", type=int, default=900)
    args = ap.parse_args()

    deadline = time.time() + args.timeout
    label = "snapshot" if args.phase == "snapshot" else "change stream"
    print(f"waiting for {label} to complete (timeout {args.timeout}s)")

    while time.time() < deadline:
        expected = postgres_expected()
        actual = topic_totals()
        missing = {
            t: expected[t] - actual.get(t, 0)
            for t in config.CDC_TABLES
            if actual.get(t, 0) < expected[t]
        }

        if not missing:
            lag = replication_lag()
            print(f"  {label} complete: "
                  + ", ".join(f"{t}={actual[t]:,}" for t in config.CDC_TABLES))
            if lag is not None:
                print(f"  replication slot lag {lag:,} bytes")
            return 0

        short = ", ".join(f"{t} short {n:,}" for t, n in list(missing.items())[:3])
        print(f"  {short}", end="\r", flush=True)
        time.sleep(5)

    print(f"\nFAILED: {label} did not complete within {args.timeout}s",
          file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
