"""Return the whole stack to a genuinely clean state.

Truncating Postgres is not enough. TRUNCATE is a DDL-level operation that
writes no per-row deletes to the WAL, so Debezium never sees the rows leave.
Reseeding then emits a second full generation of INSERTs and the Kafka topics
silently accumulate duplicates: three seed runs left 288,288 events on a topic
that should hold 96,096.

A real reset therefore has to drop the connector, the replication slot and the
topics as well, so the next snapshot starts from nothing.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

import psycopg

from pipeline import config

CONNECTOR = "commerce-connector"
SLOT = "dbz_commerce_slot"


def _connect_api(path: str, method: str = "GET") -> tuple[int, str]:
    req = urllib.request.Request(f"{config.CONNECT_URL}{path}", method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()
    except urllib.error.URLError as exc:
        return 0, str(exc)


def drop_connector() -> None:
    status, _ = _connect_api(f"/connectors/{CONNECTOR}", method="DELETE")
    if status in (204, 404):
        print(f"  connector {CONNECTOR}: {'deleted' if status == 204 else 'absent'}")
    elif status == 0:
        print("  Kafka Connect unreachable, skipping connector delete")
    else:
        print(f"  connector delete returned HTTP {status}")
    # Releasing the replication slot is asynchronous.
    time.sleep(5)


def drop_topics() -> None:
    topics = [f"{config.TOPIC_PREFIX}.{config.SOURCE_SCHEMA}.{t}" for t in config.CDC_TABLES]
    # Connect's internal topics keep connector offsets. Leaving them behind
    # makes a recreated connector resume mid-stream instead of re-snapshotting.
    topics += ["_connect_configs", "_connect_offsets", "_connect_status"]
    for topic in topics:
        proc = subprocess.run(
            ["docker", "exec", "cdc-kafka", "/opt/kafka/bin/kafka-topics.sh",
             "--bootstrap-server", "localhost:9092", "--delete",
             "--if-exists", "--topic", topic],
            capture_output=True, text=True,
        )
        state = "deleted" if proc.returncode == 0 else "skipped"
        print(f"  topic {topic}: {state}")


def drop_slot() -> None:
    with psycopg.connect(config.POSTGRES_DSN, autocommit=True) as pg:
        with pg.cursor() as cur:
            cur.execute(
                "SELECT active_pid FROM pg_replication_slots WHERE slot_name = %s",
                (SLOT,),
            )
            row = cur.fetchone()
            if not row:
                print(f"  slot {SLOT}: absent")
                return
            if row[0]:
                cur.execute("SELECT pg_terminate_backend(%s)", (row[0],))
                time.sleep(2)
            cur.execute("SELECT pg_drop_replication_slot(%s)", (SLOT,))
            print(f"  slot {SLOT}: dropped")


def truncate_tables() -> None:
    with psycopg.connect(config.POSTGRES_DSN) as pg:
        with pg.cursor() as cur:
            cur.execute(
                'TRUNCATE commerce.order_item, commerce."order", '
                "commerce.customer, commerce.seller, commerce.product CASCADE"
            )
            # Reset the stats counters too, so n_tup_ins is a reliable signal
            # that the next run inserted exactly one generation.
            cur.execute("SELECT pg_stat_reset()")
        pg.commit()
    print("  postgres tables: truncated")


def restart_connect() -> None:
    """Restart Kafka Connect after its internal topics are dropped.

    Connect caches the config topic in memory and keeps writing to the deleted
    one, so registering a connector fails with "Error writing connector
    configuration to Kafka" until the worker restarts and recreates them.
    """
    subprocess.run(["docker", "compose", "restart", "connect"],
                   capture_output=True, text=True, cwd=str(config.PROJECT_ROOT))
    for _ in range(36):
        status, body = _connect_api("/connectors")
        if status == 200 and body.strip() == "[]":
            print("  kafka connect: restarted, no connectors")
            return
        time.sleep(5)
    print("  kafka connect: restart did not settle in time", file=sys.stderr)


def main() -> int:
    ap = argparse.ArgumentParser(description="reset Postgres, Kafka and the CDC slot")
    ap.add_argument("--keep-data", action="store_true",
                    help="reset streaming state only, leave table rows in place")
    args = ap.parse_args()

    print("resetting CDC stack:")
    drop_connector()
    drop_slot()
    drop_topics()
    restart_connect()
    if not args.keep_data:
        truncate_tables()
    print("\nclean. `make seed` then `make register` starts a fresh snapshot.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
