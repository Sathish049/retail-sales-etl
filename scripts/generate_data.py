"""
Generate fake retail data for the ETL pipeline.

Creates:
  data/raw/products.csv          - product master (dimension)
  data/raw/stores.csv            - store master (dimension)
  data/raw/sales_YYYY-MM-DD.csv  - one file of transactions per day (fact)

The daily sales files are deliberately "dirty" so the transform step has real
work to do: duplicates, negative quantities, unknown products, bad timestamps,
messy casing and whitespace.

Usage:
  python scripts/generate_data.py --start 2026-10-01 --days 14
"""
import argparse
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

CATEGORIES = {
    "Electronics": (25, 900),
    "Grocery": (1, 25),
    "Apparel": (10, 120),
    "Home": (8, 300),
    "Sports": (12, 250),
}
STORES = [
    ("S01", "Flagstaff", "AZ", "Southwest"),
    ("S02", "Phoenix", "AZ", "Southwest"),
    ("S03", "Denver", "CO", "Mountain"),
    ("S04", "Salt Lake City", "UT", "Mountain"),
    ("S05", "Seattle", "WA", "Northwest"),
    ("S06", "Portland", "OR", "Northwest"),
    ("S07", "Austin", "TX", "South"),
    ("S08", "Dallas", "TX", "South"),
]
PAYMENT_METHODS = ["card", "cash", "mobile", "gift_card"]
COLUMNS = [
    "transaction_id", "transaction_ts", "store_id", "product_id", "customer_id",
    "quantity", "unit_price", "discount_pct", "payment_method",
]


def write_csv(path: Path, header, rows):
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def build_products(rng: random.Random):
    products = []
    pid = 1
    for category, (low, high) in CATEGORIES.items():
        for i in range(8):
            products.append((
                f"P{pid:03d}",
                f"{category} Item {i + 1}",
                category,
                round(rng.uniform(low, high), 2),
            ))
            pid += 1
    return products


def messy(value: str, rng: random.Random) -> str:
    """Randomly add whitespace / change casing, like real source systems do."""
    roll = rng.random()
    if roll < 0.05:
        return f" {value} "
    if roll < 0.10:
        return value.lower()
    if roll < 0.13:
        return value.upper()
    return value


def build_day(day: date, products, rng: random.Random):
    rows = []
    n = rng.randint(300, 600)
    for i in range(n):
        product_id, _, _, list_price = rng.choice(products)
        store_id = rng.choice(STORES)[0]
        ts = datetime(day.year, day.month, day.day, rng.randint(8, 21),
                      rng.randint(0, 59), rng.randint(0, 59))
        row = [
            f"T{day:%Y%m%d}-{i:05d}",
            ts.isoformat(sep=" "),
            messy(store_id, rng),
            messy(product_id, rng),
            "" if rng.random() < 0.15 else f"C{rng.randint(1, 5000):05d}",  # guest checkout
            rng.choice([1, 1, 1, 2, 2, 3, 4, 5]),
            list_price,
            rng.choice([0, 0, 0, 0, 0.05, 0.10, 0.15, 0.20]),
            messy(rng.choice(PAYMENT_METHODS), rng),
        ]

        # Inject data quality problems (each ~1-2% of rows)
        r = rng.random()
        if r < 0.01:
            row[5] = -row[5]                 # negative quantity
        elif r < 0.02:
            row[3] = "P999"                  # product not in master data
        elif r < 0.03:
            row[1] = "not-a-timestamp"       # unparseable timestamp
        elif r < 0.035:
            row[6] = ""                      # missing price
        elif r < 0.04:
            row[2] = "S99"                   # store not in master data
        rows.append(row)

    # ~2% exact duplicates (e.g. a POS system resending a batch)
    rows.extend(rng.sample(rows, k=max(1, n // 50)))
    rng.shuffle(rows)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", default="2026-10-01", help="first sales date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=14, help="number of daily files")
    parser.add_argument("--out", default=str(Path(__file__).resolve().parents[1] / "data" / "raw"))
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    products = build_products(rng)
    write_csv(out / "products.csv", ["product_id", "product_name", "category", "list_price"], products)
    write_csv(out / "stores.csv", ["store_id", "city", "state", "region"], STORES)

    start = date.fromisoformat(args.start)
    for d in range(args.days):
        day = start + timedelta(days=d)
        rows = build_day(day, products, rng)
        write_csv(out / f"sales_{day:%Y-%m-%d}.csv", COLUMNS, rows)
        print(f"wrote sales_{day:%Y-%m-%d}.csv ({len(rows)} rows)")


if __name__ == "__main__":
    main()
