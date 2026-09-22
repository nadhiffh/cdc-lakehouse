-- OLTP source schema. Mirrors how an operational e-commerce database would
-- actually store this, not how the CSV exports happen to be shaped.
--
-- Column types come from profiling the real Olist export:
--   * all ids are fixed 32-char hex hashes -> CHAR(32)
--   * (order_id, order_item_id) is a verified PK on 112,650 rows
--   * no orphan rows across any FK, so the constraints below are enforceable
--   * prices are strictly positive (min 0.85), freight never negative

CREATE SCHEMA IF NOT EXISTS commerce;
SET search_path TO commerce;

-- customer_unique_id is the real person; customer_id is a per-order alias.
-- Profiling: 99,441 customer_id vs 96,096 customer_unique_id.
-- The dimension keys on the person, so it lives here as its own table.
CREATE TABLE customer (
    customer_unique_id   CHAR(32) PRIMARY KEY,
    zip_code_prefix      VARCHAR(8)  NOT NULL,
    city                 VARCHAR(64) NOT NULL,
    state                CHAR(2)     NOT NULL,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE seller (
    seller_id            CHAR(32) PRIMARY KEY,
    zip_code_prefix      VARCHAR(8)  NOT NULL,
    city                 VARCHAR(64) NOT NULL,
    state                CHAR(2)     NOT NULL,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 610 of 32,951 products have no category, so category stays nullable.
CREATE TABLE product (
    product_id           CHAR(32) PRIMARY KEY,
    category_name        VARCHAR(64),
    category_name_en     VARCHAR(64),
    weight_g             INTEGER,
    length_cm            INTEGER,
    height_cm            INTEGER,
    width_cm             INTEGER,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- order_status is the column CDC exists to track: 392,856 real transitions.
CREATE TABLE "order" (
    order_id                 CHAR(32) PRIMARY KEY,
    customer_id              CHAR(32) NOT NULL,
    customer_unique_id       CHAR(32) NOT NULL REFERENCES customer (customer_unique_id),
    order_status             VARCHAR(16) NOT NULL,
    purchased_at             TIMESTAMP NOT NULL,
    approved_at              TIMESTAMP,
    delivered_carrier_at     TIMESTAMP,
    delivered_customer_at    TIMESTAMP,
    estimated_delivery_at    TIMESTAMP NOT NULL,
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT order_status_known CHECK (order_status IN (
        'created', 'approved', 'invoiced', 'processing',
        'shipped', 'delivered', 'unavailable', 'canceled'
    ))
);

CREATE INDEX order_customer_idx ON "order" (customer_unique_id);
CREATE INDEX order_purchased_idx ON "order" (purchased_at);

CREATE TABLE order_item (
    order_id             CHAR(32) NOT NULL REFERENCES "order" (order_id),
    order_item_id        SMALLINT NOT NULL,
    product_id           CHAR(32) NOT NULL REFERENCES product (product_id),
    seller_id            CHAR(32) NOT NULL REFERENCES seller (seller_id),
    shipping_limit_at    TIMESTAMP,
    price                NUMERIC(10, 2) NOT NULL CHECK (price > 0),
    freight_value        NUMERIC(10, 2) NOT NULL CHECK (freight_value >= 0),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, order_item_id)
);

CREATE INDEX order_item_seller_idx ON order_item (seller_id);
