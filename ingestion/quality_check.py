# ingestion/quality_check.py

import duckdb
from pathlib import Path

DB_PATH = "data/warehouse.duckdb"

# Define which columns matter for each table
TABLE_CHECKS = {
    "bronze_orders":        ["order_id", "customer_id", "order_status"],
    "bronze_order_items":   ["order_id", "product_id", "seller_id"],
    "bronze_order_payments":["order_id", "payment_value"],
    "bronze_order_reviews": ["review_id", "order_id"],
    "bronze_customers":     ["customer_id", "customer_unique_id"],
    "bronze_products":      ["product_id"],
}

def run_quality_checks(con, table_name, key_columns):
    print(f"\n{table_name}")
    print("-" * 40)

    row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"  ✓ Rows: {row_count:,}")

    for col in key_columns:
        null_count = con.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL"
        ).fetchone()[0]
        pct = null_count / row_count * 100
        status = "✓" if null_count == 0 else "⚠️ "
        print(f"  {status} {col}: {null_count} nulls ({pct:.1f}%)")

def main():
    if not Path(DB_PATH).exists():
        print(f"❌ No database found at {DB_PATH}. Run ingestion first.")
        return

    con = duckdb.connect(DB_PATH, read_only=True)  # read_only=True — checks shouldn't modify anything

    for table_name, key_columns in TABLE_CHECKS.items():
        try:
            run_quality_checks(con, table_name, key_columns)
        except Exception as e:
            print(f"\n  ❌ {table_name} failed: {e}")  # table might not exist yet — don't crash the whole script

    con.close()

if __name__ == "__main__":
    main()