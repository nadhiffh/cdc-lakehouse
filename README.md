# CDC Lakehouse

Change data capture pipeline over a Brazilian e-commerce OLTP database.
Debezium streams row-level changes out of Postgres into Kafka; a consumer lands
them in DuckDB as an append-only event log; dbt builds SCD Type 2 dimensions on
top.

Verified end to end on a clean rebuild: 639,764 change events, 99,441 orders,
112,650 line items, 12 models and 88 data tests passing.

![ci](https://github.com/nadhiffh/cdc-lakehouse/actions/workflows/ci.yml/badge.svg)

## Architecture

```
Olist CSV export
      |  seed.py  (initial OLTP state: every order at 'created')
      v
[Postgres 17]  wal_level=logical, REPLICA IDENTITY FULL
      |        replay.py applies 295,272 status transitions
      |        and 259 customer address changes
      v
[Debezium 3.6] logical decoding via pgoutput, publication dbz_commerce
      |
      v
[Kafka 4.3]    5 topics, cleanup.policy=delete, retention=-1
      |
      v  consume.py
[DuckDB]  landing.cdc_events            append-only, 639,764 rows
      |
      +--> stg_customer_events ---> dim_customer                (SCD Type 2)
      +--> stg_order_events ------> dim_order_status_history    (SCD Type 2)
      |          |
      |          +--> int_orders_current --> fct_orders
      +--> stg_order_items -----------------> fct_order_items
      +--> stg_products ------------------->  dim_product       (SCD Type 1)
      +--> stg_sellers -------------------->  dim_seller        (SCD Type 1)
```

Everything runs locally under Docker Compose. No cloud account needed.

## Why these choices

**Replayed history, not invented churn.** A CDC project needs a mutation
stream, and there is no public "real CDC feed" to download. Rather than
randomly mutating rows, this replays change events that the source actually
records: each order carries four lifecycle timestamps, so 295,272 status
transitions are reconstructed from data instead of fabricated. The 259 customer
address changes come from repeat buyers whose address differs between orders.

**REPLICA IDENTITY FULL.** Postgres defaults to writing only the primary key of
an updated row into the WAL, which leaves Debezium's `before` block empty. With
the full pre-image, an address correction is distinguishable from a genuine
relocation. It costs WAL volume; correctness of history is worth more here.

**`cleanup.policy=delete`, not `compact`.** Log compaction keeps only the newest
value per key. On a topic whose entire purpose is recording intermediate order
states, compaction would silently destroy the history SCD2 exists to capture.
Many Debezium examples default to compact; it is wrong for this use case.

**`decimal.handling.mode=string`.** The default `double` introduces float drift
on money. Prices arrive as strings and are cast to `DECIMAL(10,2)`, so
`sum(price)` reconciles to the cent: 13,591,643.70 across 112,650 items.

**Append-only landing table.** `landing.cdc_events` is never deduplicated, so
SCD2 logic is rebuildable and testable against the full stream. Collapsing to
latest-per-key at load time would make the history unauditable.

**Columnar inserts via Arrow.** The consumer first used `executemany`, which
cost a flat ~430 events/sec because DuckDB's Python API binds and appends row by
row. That is tolerable locally and fatal in CI: 639,764 events took 25 minutes on
a 2-core runner and consumed the entire job budget. Batching into an Arrow table
and inserting from a registered view uses DuckDB's native columnar append path,
which brought the same load to 11 seconds locally.

**SCD Type 2 only where history exists.** `dim_customer` and
`dim_order_status_history` carry real history. `dim_seller` and `dim_product`
are Type 1 because profiling found zero attribute drift across all 3,095
sellers. Applying Type 2 there would produce dimensions where every key has
exactly one version, implying tracked history that does not exist.

## Data quality findings

Profiled from the real source before any schema was written.

| Finding | Volume | Consequence |
|---|---|---|
| `customer_id` is a per-order alias, `customer_unique_id` is the person | 99,441 vs 96,096 | `dim_customer` keys on the person, or it could never hold history |
| Repeat customers who change address | 250 zip, 122 city, of 2,997 repeat buyers | The only genuine SCD2 driver in the dataset |
| Orders recording `shipped` before `approved` | 1,359, worst case 171 days backwards | Business timestamps cannot bound SCD2 windows unaided |
| Orders `approved` at the exact purchase instant | 1,296 | Zero-width windows are legitimate, not a bug |
| Orders with no line items | 775 | An inner join to items silently drops them |
| Products with no category | 610 of 32,951 | Surfaced as `unknown`, not dropped |
| Sellers with attribute drift | 0 of 3,095 | SCD2 on sellers would be decoration |
| Status not implied by its timestamps | 314 invoiced, 301 processing | Terminal status must be applied explicitly |

Two findings that shaped the model:

- Referential integrity is clean: no orphans across orders, customers, items,
  products or sellers, so the Postgres FKs are enforceable.
- `(order_id, order_item_id)` is a valid primary key on all 112,650 rows.

## The timestamp problem

Worth its own section, because it is the central modelling decision.

SCD2 needs a monotonic clock to bound validity windows. This pipeline has three
candidate timestamps and none is usable alone:

| Candidate | Monotonic | Business-meaningful |
|---|---|---|
| Debezium `ts_ms` | yes | no — when the connector observed the event |
| Source lifecycle timestamps | no — 1,359 inversions | yes |
| Postgres LSN | yes | no — a byte position in the WAL |

The first attempt used `ts_ms` and the positive-width test failed on 189,737 of
394,713 order versions. The replay pushes two years of history through in about
48 seconds of wall clock, so consecutive versions landed on the same
millisecond. `ts_ms` orders events correctly but says nothing about when
anything happened.

The fix was upstream, not in SQL: `updated_at` on every captured table now
carries the business instant of the change, set explicitly by the seed and the
replay. That is standard practice for an OLTP schema whose history needs to be
reconstructable downstream.

Ordering still uses `(lsn, kafka_offset)`, verified to never run backwards and
never collide across 295,531 streamed events. `valid_from` is the business
instant clamped non-decreasing with a running max, so an inverted source
timestamp is pulled forward rather than dropped, and `is_timestamp_clamped`
records that it happened — 1,443 rows. Zero-length states are kept and flagged
as `is_instantaneous` (4,605 rows) rather than padded, because an order really
can be approved and shipped at the same recorded instant.

## Tests

88 data tests run as part of `dbt build`, so a failure halts the graph before bad
data reaches the marts. Beyond uniqueness, not-null, accepted-values and
referential checks, these target CDC and SCD2 failure modes specifically:

- `assert_scd2_has_exactly_one_current_version` — the defining SCD2 invariant.
  Zero current rows means history closed without a successor; more than one
  means any join on `is_current` fans out and double-counts revenue. Neither
  shows up in a uniqueness test.
- `assert_scd2_windows_have_no_gaps_or_overlaps` — each version's `valid_to`
  must equal the next `valid_from`. A gap is a moment where the entity has no
  row and an as-of join returns nothing; an overlap returns two rows.
- `assert_scd2_join_does_not_fan_out` — the as-of join must resolve each order
  to exactly one customer version, guarding the half-open range predicate.
  Closed bounds on both sides would duplicate every line item.
- `assert_scd2_version_counts_match_event_log` — version counts must be
  derivable from the events, so the model cannot drop a change or invent a
  version on a no-op update.
- `assert_no_duplicate_cdc_generations` — more than one snapshot `read` per key
  means the log spans multiple snapshots and every count is inflated. See below.
- `assert_deleted_rows_do_not_reappear` — a key whose newest event is a delete
  must be absent from every current-state model. Handles resurrection too, since
  a key can be inserted, deleted, then inserted again.
- `assert_fct_orders_reconciles_to_cdc` — fact row count must equal the distinct
  orders in the event log, catching a join fanning out or a filter dropping rows.
- `assert_order_items_reconcile_between_facts` — guards the 775 order with no
  items, which must be absent from the item fact without changing item counts
  on the order fact.

Two checks sit outside dbt deliberately, because every dbt test is a query
against the warehouse and so cannot see problems upstream of it:

- `pipeline/verify_stream.py` asserts Kafka offsets equal Postgres rows plus
  updates, per table. It catches drift between the source and the stream.
- `pipeline/assert_marts.py` pins absolute row counts and business invariants
  (gross revenue to the cent, 252 customers with history, 317 fact rows
  resolving to a historical version). Shape-based tests pass trivially on an
  empty table; this makes a silently-empty warehouse fail.

Both were verified against a deliberately corrupted warehouse: deleting 10 fact
rows makes `assert_marts` fail on the row count *and* the revenue total, and
`assert_order_items_reconcile_between_facts` report exactly 10 offending rows.
Confirming a test fails when it should is the only way to know it is not passing
vacuously.

## Four bugs worth documenting

All four surfaced only from testing clean state or exercising an untested branch,
never from an incremental happy-path run.

**Terminal status derived from timestamps.** The replay first inferred each
order's final status from which lifecycle timestamps were present. That silently
dropped all 314 `invoiced` and 301 `processing` orders and misclassified 616
more, because the source's recorded status does not always agree with its
timestamps. The replay now reconciles its end state against the source and exits
non-zero on any drift.

**TRUNCATE is invisible to CDC.** `TRUNCATE` is DDL: it writes no per-row
deletes to the WAL, so Debezium never sees the rows leave. Reseeding without
dropping the Kafka topics left three full generations of snapshot events stacked
on each topic — 288,288 customer events where 96,096 were expected — while
Postgres itself looked perfectly correct. `make reset` now drops the connector,
the replication slot and the topics, and restarts Kafka Connect so it recreates
its internal config topics.

**Deleted rows came back to life.** The staging models filtered
`op <> 'delete'` *before* ranking events to find the latest one. That removes the
delete from the candidate set, so the row falls back to its earlier insert and
reappears as though still live. Found by inserting one product and deleting it:
it stayed in `dim_product`, and every existing test passed, because uniqueness,
not-null and referential integrity all still held. Only the absolute row count
was wrong, by one. Ranking first and discarding keys whose *newest* event is a
delete is the only order that is correct, and it handles resurrection, since a
key can legitimately be inserted, deleted and inserted again.

**Resetting the stack doubled the event log.** `make reset` cleared Postgres and
Kafka but left the DuckDB file in place. Recreating the topics resets their
offsets to zero, so the resuming consumer reloaded the entire stream and appended
it to an append-only table: 1,279,530 landing rows where 639,764 were expected.
Caught by `assert_no_duplicate_cdc_generations`, the test written for the
Kafka-side version of the same mistake, which is the argument for writing
invariants rather than regression tests for specific incidents. `reset` now drops
the warehouse too, and `consume` is idempotent: rerunning it against an
up-to-date log reports there is nothing to do instead of duplicating, while still
picking up genuinely new events.

## Setup

```bash
make setup     # uv venv on Python 3.12 + deps
make data      # download the Olist CSVs (~45 MB, no credentials needed)
make up        # start Postgres, Kafka, Debezium; waits for health checks
```

`dbt-core` builds a dependency over HTTPS, so it needs a Python with TLS root
certificates. A `uv`-managed interpreter works; the python.org macOS build ships
without a certificate bundle and fails with `CERTIFICATE_VERIFY_FAILED`.

## Run

```bash
make seed       # load initial OLTP state, every order at 'created'
make register   # register the Debezium connector, triggers the snapshot
make replay     # apply 295,272 transitions + 259 address changes
make verify     # assert Kafka matches Postgres exactly
make consume    # drain the topics into DuckDB
make build      # dbt build: 12 models, 88 data tests
```

Or the whole thing from nothing:

```bash
make rebuild    # reset -> seed -> register -> replay -> consume -> build
```

`make rebuild` is the path worth running before a push. It is what surfaced both
bugs described above, neither of which reproduced on an incremental run. The
chain includes explicit barriers (`wait-snapshot`, `wait-stream`) because
registering a connector starts the snapshot in the background and replayed
UPDATEs reach Kafka after Postgres commits them; without them the next step
races and reconciles against partial state, which looks like a data bug and is
not one.

CI runs the same sequence on every push, plus the mart assertions.

`make ui` starts Kafka UI on localhost:8080 for browsing topics and messages.

## Security note

This stack has no authentication and is intended for local use only:

- Kafka listeners are `PLAINTEXT`
- the Kafka Connect REST API on :8083 is unauthenticated, and Connect can load
  arbitrary connector plugins, so it should never be exposed off localhost
- Postgres uses throwaway credentials committed to this repo

Do not port-forward these services or run this configuration anywhere shared.

## Extending this

- Schema Registry with Avro instead of JSON, to catch producer-side schema
  drift before it reaches the warehouse.
- Debezium's outbox pattern, so status changes carry explicit business events
  rather than being inferred from column diffs.
- Incremental dbt models on `landing.cdc_events` keyed on `kafka_offset`, once
  the log outgrows a full rebuild.
- Orchestration with Dagster, reusing the asset-check pattern from
  [citibike-lakehouse](https://github.com/nadhiffh/citibike-lakehouse).
