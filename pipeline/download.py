"""Fetch the Olist CSVs.

Kaggle is the canonical home but requires an API token, so this pulls from a
public GitHub mirror to keep `make data` credential-free. Sizes are asserted so
a truncated or redirected download fails loudly instead of producing a
half-empty warehouse.
"""

from __future__ import annotations

import sys
import urllib.request

from pipeline import config

BASE = (
    "https://raw.githubusercontent.com/spdrio/"
    "Brazilian-E-Commerce-Public-Dataset-by-Olist/master/files"
)

# name -> expected byte size, verified at download time.
FILES = {
    "olist_orders_dataset": 17_654_914,
    "olist_order_items_dataset": 15_438_671,
    "olist_customers_dataset": 9_033_957,
    "olist_products_dataset": 2_379_446,
    "olist_sellers_dataset": 174_703,
    "product_category_name_translation": 2_542,
}


def main() -> int:
    config.RAW_DIR.mkdir(parents=True, exist_ok=True)
    failures = []

    for name, expected in FILES.items():
        dest = config.RAW_DIR / f"{name}.csv"
        if dest.exists() and dest.stat().st_size == expected:
            print(f"  {name}.csv already present")
            continue
        url = f"{BASE}/{name}.csv"
        print(f"  downloading {name}.csv ...", end=" ", flush=True)
        try:
            urllib.request.urlretrieve(url, dest)
        except OSError as exc:
            print("FAILED")
            failures.append(f"{name}: {exc}")
            continue
        size = dest.stat().st_size
        if size != expected:
            print(f"FAILED (got {size:,} bytes, expected {expected:,})")
            failures.append(f"{name}: size mismatch")
        else:
            print(f"{size:,} bytes")

    if failures:
        for f in failures:
            print(f"  {f}", file=sys.stderr)
        return 1
    print(f"\nsource data ready in {config.RAW_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
