"""Drain the Debezium topics into DuckDB as an append-only event log.

The landing tables are deliberately raw: one row per change event, carrying the
Debezium envelope fields needed to order and interpret it. No deduplication,
no last-write-wins collapsing. dbt does that, so the event history stays
auditable and the SCD2 logic is testable against the full stream.

Every row carries both clocks, because downstream needs them for different jobs:

  * (lsn, kafka_offset) orders events. Postgres LSNs are monotonic in commit
    order, verified to never run backwards or collide across 295,531 streamed
    events, so this is the only sound total order available.
  * event_ts_ms is when the connector observed the event. Useful for lineage and
    CDC latency, useless as an SCD2 window bound: this replay compresses two
    years of history into under a minute of wall clock.

The business instant of each change travels in the payload itself, as the source
table's updated_at. That is what bounds the SCD2 windows in the marts.
"""

from __future__ import annotations

import argparse
import json
import sys

import duckdb
import pyarrow as pa
from confluent_kafka import Consumer, KafkaError, TopicPartition

from pipeline import config

# Column order for the Arrow batch handed to DuckDB. Must match INSERT_COLUMNS.
ARROW_SCHEMA = pa.schema([
    ("source_table", pa.string()),
    ("op", pa.string()),
    ("lsn", pa.int64()),
    ("tx_id", pa.int64()),
    ("event_ts_ms", pa.int64()),
    ("is_snapshot", pa.bool_()),
    ("kafka_offset", pa.int64()),
    ("pk", pa.string()),
    ("before", pa.string()),
    ("after", pa.string()),
])

INSERT_COLUMNS = ", ".join(f.name for f in ARROW_SCHEMA)

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
    ap.add_argument("--batch-size", type=int, default=50_000,
                    help="events per columnar insert")
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
        # Defaults fetch conservatively, which shows up as latency per poll when
        # draining a backlog of 639,764 events rather than tailing a live stream.
        "fetch.min.bytes": 1_048_576,
        "fetch.wait.max.ms": 100,
        "queued.max.messages.kbytes": 262_144,
    })

    con = duckdb.connect(str(config.WAREHOUSE))
    con.execute(DDL)

    existing = con.execute("SELECT count(*) FROM landing.cdc_events").fetchone()[0]

    if args.from_beginning:
        con.execute("DELETE FROM landing.cdc_events")
        consumer.assign([TopicPartition(t, 0, 0) for t in topics])
        print("rebuilding event log from offset 0")
    else:
        consumer.subscribe(topics)

    ends = topic_end_offsets(consumer, topics)
    target = sum(ends.values())
    print(f"kafka holds {target:,} events across {len(topics)} topics")

    # Guard against loading a second generation on top of the first.
    #
    # `make reset` recreates the Kafka topics, which resets their offsets to
    # zero. A resuming consumer then reloads everything, and because the landing
    # table is append-only the log silently doubles while Postgres and the marts
    # still look correct. That is exactly what happened: 1,279,530 rows where
    # 639,764 were expected.
    #
    # The check is the only one that actually matters: does the log already hold
    # at least as many events as Kafka currently has? If so there is nothing to
    # add, and appending anything would be duplication.
    if not args.from_beginning and existing >= target > 0:
        print(f"nothing to do: the log holds {existing:,} events and Kafka has "
              f"{target:,}.")
        print("Use --from-beginning to rebuild from scratch, or `make reset` to "
              "start over.")
        consumer.close()
        con.close()
        return 0

    batch: list[tuple] = []
    counts: dict[str, int] = {t: 0 for t in config.CDC_TABLES}
    ops: dict[str, int] = {}
    total = 0
    empty_polls = 0

    def flush() -> None:
        """Insert the batch columnar, via Arrow.

        executemany was the first implementation and cost about 430 events per
        second regardless of batch size: DuckDB's Python API binds and appends
        row by row. On a 2-core CI runner that turned 639,764 events into 25
        minutes and blew the job's time budget. Handing DuckDB one Arrow table
        per batch moves the work into its native columnar append path.
        """
        if not batch:
            return
        # zip(*batch) transposes rows into columns without copying the values.
        columns = list(zip(*batch))
        arrow_batch = pa.table(
            {field.name: pa.array(col, type=field.type)
             for field, col in zip(ARROW_SCHEMA, columns)},
            schema=ARROW_SCHEMA,
        )
        # Registered as a view so the INSERT reads straight from Arrow memory.
        con.register("arrow_batch", arrow_batch)
        con.execute(
            f"INSERT INTO landing.cdc_events ({INSERT_COLUMNS}) "
            f"SELECT {INSERT_COLUMNS} FROM arrow_batch"
        )
        con.unregister("arrow_batch")
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

        if len(batch) >= args.batch_size:
            flush()
            print(f"  loaded {total:,}/{target:,}", end="\r", flush=True)

        if total >= target:
            break

    flush()
    # Committing with nothing consumed raises _NO_OFFSET rather than being a
    # no-op, which turned an already-up-to-date rerun into a crash.
    if total:
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
