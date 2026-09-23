-- Guards the specific bug this project hit: TRUNCATE writes no per-row deletes
-- to the WAL, so reseeding Postgres without dropping the Kafka topics left
-- three full generations of snapshot events stacked on each topic (288,288
-- customer events where 96,096 were expected) while Postgres itself looked
-- perfectly correct.
--
-- A snapshot read happens once per row per connector lifetime. More than one
-- 'read' event for the same key means the event log spans multiple snapshots
-- and every downstream count is inflated.

select
    source_table,
    pk,
    count(*) as snapshot_reads
from {{ source('landing', 'cdc_events') }}
where op = 'read'
group by 1, 2
having count(*) > 1
