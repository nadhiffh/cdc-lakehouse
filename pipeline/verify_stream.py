"""Assert the Kafka stream matches Postgres exactly.

Exists because of a bug this project actually hit: TRUNCATE writes no per-row
deletes to the WAL, so reseeding without dropping the topics left three full
generations of events stacked on each topic (288,288 customer events where
96,096 were expected) while Postgres itself looked perfectly correct.

Run after `seed` and after `replay`.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import Counter

import psycopg

from pipeline import config

KAFKA_CONTAINER = "cdc-kafka"


def topic_offsets() -> dict[str, int]:
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


def postgres_counts() -> dict[str, int]:
    with psycopg.connect(config.POSTGRES_DSN) as pg, pg.cursor() as cur:
        counts = {}
        for table in config.CDC_TABLES:
            cur.execute(f'SELECT count(*) FROM commerce."{table}"')
            counts[table] = cur.fetchone()[0]
        return counts


def postgres_dml() -> dict[str, tuple[int, int, int]]:
    """Insert/update/delete counters, to prove event volume adds up."""
    with psycopg.connect(config.POSTGRES_DSN) as pg, pg.cursor() as cur:
        cur.execute(
            "SELECT relname, n_tup_ins, n_tup_upd, n_tup_del "
            "FROM pg_stat_user_tables WHERE schemaname = 'commerce'"
        )
        return {r[0]: (r[1], r[2], r[3]) for r in cur.fetchall()}


def read_ops(table: str, cap: int) -> Counter:
    topic = f"{config.TOPIC_PREFIX}.{config.SOURCE_SCHEMA}.{table}"
    proc = subprocess.run(
        ["docker", "exec", KAFKA_CONTAINER, "/opt/kafka/bin/kafka-console-consumer.sh",
         "--bootstrap-server", "localhost:9092", "--topic", topic,
         "--from-beginning", "--max-messages", str(cap), "--timeout-ms", "120000"],
        capture_output=True, text=True,
    )
    ops: Counter = Counter()
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            ops[json.loads(line).get("op")] += 1
        except json.JSONDecodeError:
            continue
    return ops


def main() -> int:
    ap = argparse.ArgumentParser(description="verify Kafka stream matches Postgres")
    ap.add_argument("--deep", action="store_true",
                    help="also decode every event and reconcile op types")
    args = ap.parse_args()

    offsets = topic_offsets()
    rows = postgres_counts()
    dml = postgres_dml()

    print(f"{'table':14s} {'pg rows':>10s} {'kafka':>10s} {'ins':>9s} {'upd':>9s} {'expected':>10s}")
    failures = []
    for table in config.CDC_TABLES:
        pg_rows = rows.get(table, 0)
        kafka = offsets.get(table, 0)
        ins, upd, dele = dml.get(table, (0, 0, 0))
        # Every insert and every update becomes exactly one event. Snapshot
        # reads replace the inserts they correspond to, so the expected total
        # is one row per live row plus one event per update.
        expected = pg_rows + upd
        ok = kafka == expected
        if not ok:
            failures.append(
                f"{table}: kafka has {kafka:,}, expected {expected:,} "
                f"({pg_rows:,} rows + {upd:,} updates)"
            )
        print(f"{table:14s} {pg_rows:>10,} {kafka:>10,} {ins:>9,} {upd:>9,} "
              f"{expected:>10,} {'' if ok else '  <-- MISMATCH'}")

    if args.deep:
        print("\ndecoding events per topic:")
        for table in config.CDC_TABLES:
            cap = offsets.get(table, 0)
            if not cap:
                continue
            ops = read_ops(table, cap)
            total = sum(ops.values())
            detail = " ".join(f"{k}={v:,}" for k, v in sorted(ops.items(), key=lambda x: str(x[0])))
            print(f"  {table:14s} {total:>9,}  {detail}")
            if total != cap:
                failures.append(f"{table}: decoded {total:,} events but offset is {cap:,}")

    if failures:
        print("\nFAILED:", file=sys.stderr)
        for f in failures:
            print(f"  {f}", file=sys.stderr)
        print("\nIf counts are a multiple of the expected value, the topics hold\n"
              "stale generations from an earlier seed. Run `make reset`.",
              file=sys.stderr)
        return 1

    print("\nstream matches Postgres exactly")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
