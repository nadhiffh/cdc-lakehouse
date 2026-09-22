-- Logical replication setup for Debezium.
--
-- wal_level=logical is set as a server flag in docker-compose (it needs a
-- restart and cannot be changed from here).

SET search_path TO commerce;

-- REPLICA IDENTITY FULL makes Postgres write the complete pre-image of every
-- updated row into the WAL, so Debezium emits a populated "before" block.
-- The default (identity of the PK only) leaves before = null on updates, which
-- would make it impossible to tell an address correction apart from a genuine
-- relocation when building SCD2. This costs WAL volume and is a deliberate
-- trade: correctness of history over write throughput.
ALTER TABLE customer    REPLICA IDENTITY FULL;
ALTER TABLE seller      REPLICA IDENTITY FULL;
ALTER TABLE product     REPLICA IDENTITY FULL;
ALTER TABLE "order"     REPLICA IDENTITY FULL;
ALTER TABLE order_item  REPLICA IDENTITY FULL;

-- An explicit publication, rather than FOR ALL TABLES, so adding an unrelated
-- table later does not silently start streaming it into Kafka.
CREATE PUBLICATION dbz_commerce FOR TABLE
    customer, seller, product, "order", order_item;

-- Debezium connects as a dedicated role. It needs REPLICATION to read the WAL
-- and ownership of the publication to attach to it, but no write access to the
-- data itself.
CREATE ROLE debezium WITH REPLICATION LOGIN PASSWORD 'debezium';
GRANT CONNECT ON DATABASE commerce TO debezium;
GRANT USAGE ON SCHEMA commerce TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA commerce TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA commerce GRANT SELECT ON TABLES TO debezium;
ALTER PUBLICATION dbz_commerce OWNER TO debezium;
