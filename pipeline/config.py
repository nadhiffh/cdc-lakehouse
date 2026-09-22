"""Shared configuration. Env-overridable so CI and Docker can differ."""

import os
from pathlib import Path

PROJECT_ROOT = Path(os.environ.get("CDC_ROOT", Path(__file__).resolve().parent.parent))
RAW_DIR = PROJECT_ROOT / "data" / "raw"
WAREHOUSE = PROJECT_ROOT / "data" / "warehouse.duckdb"

POSTGRES_DSN = os.environ.get(
    "CDC_POSTGRES_DSN",
    "postgresql://postgres:postgres@localhost:5432/commerce",
)

KAFKA_BOOTSTRAP = os.environ.get("CDC_KAFKA_BOOTSTRAP", "localhost:29092")
CONNECT_URL = os.environ.get("CDC_CONNECT_URL", "http://localhost:8083")

# Debezium topic naming: <topic.prefix>.<schema>.<table>
TOPIC_PREFIX = "commerce"
SOURCE_SCHEMA = "commerce"

# Order lifecycle. Each transition has a backing timestamp column in the
# source, which is what makes the replay a real change stream rather than
# invented churn.
LIFECYCLE = [
    ("approved_at", "approved"),
    ("delivered_carrier_at", "shipped"),
    ("delivered_customer_at", "delivered"),
]

CDC_TABLES = ["customer", "seller", "product", "order", "order_item"]
