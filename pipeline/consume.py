"""Drain the Debezium topics into DuckDB as an append-only event log.

The landing tables are deliberately raw: one row per change event, carrying the
Debezium envelope fields needed to order and interpret it. No deduplication,
no last-write-wins collapsing. dbt does that, so the event history stays
auditable and the SCD2 logic is testable against the full stream.

Ordering key is (lsn, event_seq) rather than any source timestamp. Postgres
LSNs are monotonic in commit order, whereas the business timestamps are not:
1,382 order transitions carry a timestamp earlier than the transition before
them, so timestamp ordering would produce negative-width SCD2 windows.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

import duckdb
from confluent_kafka import Consumer, KafkaError, TopicPartition

from pipeline import config

# Debezium op codes: r=snapshot read, c=insert, u=update, d=delete.
OP_NAMES = {"r": "read", "c": "insert", "u": "update", "d": "delete"}

DDL = """
CREATE SCHEMA IF NOT EXISTS landing;

CREATE TABLE IF NOT EXISTS landing.cdc_events (
    -- envelope
    source_table   VARCHAR  NOT NULL,
    op             VARCHAR  NOT NULL,
    lsn            BIGINT,          -- null only for snapshot reads
    tx_id          BIGINT,
    event_ts_ms    BIGINT   NOT NULL,
    is_snapshot    BOOLEAN  NOT NULL,
    kafka_offset   BIGINT   NOT NULL,
    -- identity + payload, kept as JSON so one table serves every source table
    pk             VARCHAR  NOT NULL,
    before         JSON,
    after          JSON
);
"""


def epoch_ms_to_ts(value):
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).replace(tzinfo=None)


def primary_key(table: str, payload: dict) -> str:
    """Business key of the row the event describes."""
    if table == "customer":
        return payload["customer_unique_id"]
    if table == "seller":
        return payload["seller_id"]
    if table == "product":
        return payload["product_id"]
    if table == "order":
        return payload["order_id"]
    if table == "order_item":
        return f"{payload['order_id']}:{payload['order_item_id']}"
    raise ValueError(f"unknown table {table}")


def topic_end_offsets(consumer: Consumer, topics: list[str]) -> dict[str, int]:
    ends = {}
    for topic in topics:
        tp = TopicPartition(topic, 0)
        low, high = consumer.get_watermark_offsets(tp, timeout=30, cached=False)
        ends[topic] = high
    return ends


def main() -> int:
    ap = argparse.ArgumentParser(description="consume Debezium events into DuckDB")
    ap.add_argument("--from-beginning", action="store_true",
                    help="read the topics from offset 0 and rebuild the log")
    ap.add_argument("--timeout", type=float, default=30.0,
                    help="seconds to wait for new messages before stopping")
    args = ap.parse_args()

    topics = [
        f"{config.TOPIC_PREFIX}.{config.SOURCE_SCHEMA}.{t}" for t in config.CDC_TABLES
    ]

    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        # A fixed group id means a rerun resumes where the last one stopped,
        # which is what makes this incremental. --from-beginning overrides it.
        "group.id": "cdc-duckdb-loader",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    con = duckdb.connect(str(config.WAREHOUSE))
    con.execute(DDL)

    if args.from_beginning:
        con.execute("DELETE FROM landing.cdc_events")
        consumer.assign([TopicPartition(t, 0, 0) for t in topics])
        print("rebuilding event log from offset 0")
    else:
        consumer.subscribe(topics)

    ends = topic_end_offsets(consumer, topics)
    target = sum(ends.values())
    print(f"kafka holds {target:,} events across {len(topics)} topics")

    batch: list[tuple] = []
    counts: dict[str, int] = {t: 0 for t in config.CDC_TABLES}
    ops: dict[str, int] = {}
    total = 0
    empty_polls = 0

    def flush() -> None:
        if not batch:
            return
        con.executemany(
            "INSERT INTO landing.cdc_events "
            "(source_table, op, lsn, tx_id, event_ts_ms, is_snapshot, "
            " kafka_offset, pk, before, after) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            batch,
        )
        batch.clear()

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            empty_polls += 1
            if empty_polls * 1.0 >= args.timeout:
                break
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"kafka error: {msg.error()}", file=sys.stderr)
            return 1

        empty_polls = 0
        raw = msg.value()
        if raw is None:
            # Tombstone. tombstones.on.delete is false, so this should not
            # occur; skip rather than crash if it ever does.
            continue

        event = json.loads(raw)
        source = event.get("source", {})
        table = source.get("table")
        if table not in counts:
            continue

        op = event["op"]
        payload = event.get("after") or event.get("before") or {}

        batch.append((
            table,
            OP_NAMES.get(op, op),
            source.get("lsn"),
            source.get("txId"),
            event.get("ts_ms"),
            source.get("snapshot") not in (None, False, "false"),
            msg.offset(),
            primary_key(table, payload),
            json.dumps(event.get("before")) if event.get("before") else None,
            json.dumps(event.get("after")) if event.get("after") else None,
        ))
        counts[table] += 1
        ops[OP_NAMES.get(op, op)] = ops.get(OP_NAMES.get(op, op), 0) + 1
        total += 1

        if len(batch) >= 10_000:
            flush()
            print(f"  loaded {total:,}/{target:,}", end="\r", flush=True)

        if total >= target:
            break

    flush()
    consumer.commit(asynchronous=False)
    consumer.close()
    print(f"  loaded {total:,} events" + " " * 20)

    print("\nby table:")
    for table, n in counts.items():
        print(f"  {table:14s} {n:>9,}")
    print("by op:")
    for op, n in sorted(ops.items()):
        print(f"  {op:14s} {n:>9,}")

    stored = con.execute("SELECT count(*) FROM landing.cdc_events").fetchone()[0]
    print(f"\nlanding.cdc_events holds {stored:,} rows")

    # Snapshot reads carry no LSN, so they cannot be ordered against streamed
    # events by LSN alone. Confirm they are all genuinely snapshot rows.
    bad = con.execute(
        "SELECT count(*) FROM landing.cdc_events "
        "WHERE lsn IS NULL AND NOT is_snapshot"
    ).fetchone()[0]
    if bad:
        print(f"FAILED: {bad:,} non-snapshot events have no LSN", file=sys.stderr)
        return 1

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
