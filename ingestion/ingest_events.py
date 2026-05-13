# ingestion/ingest_events.py

import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
RAW_DIR = Path("data/synthetic")  # where your Olist CSVs live
DB_PATH = "data/warehouse.duckdb"  # DuckDB file; created if it doesn't exist

# Map: CSV filename (without path) → bronze table name in DuckDB
# We define this explicitly rather than auto-deriving names,
# so you always know exactly what table comes from what file.
FILE_TABLE_MAP = {
    "customers.csv":     "bronze_customers_synthetic",
    "ab_assignments.csv": "bronze_ab_assignments",
    "sessions.csv":       "bronze_sessions",
    "funnel_events.csv": "bronze_funnel_events",
}


# ---------------------------------------------------------------------------
# CORE FUNCTION
# ---------------------------------------------------------------------------
def ingest_csv_to_bronze(con: duckdb.DuckDBPyConnection, csv_path: Path, table_name: str) -> int:
    """
    Load a single CSV into a DuckDB bronze table.

    - Adds _ingested_at timestamp so you know when data arrived
    - Drops and recreates the table on every run (idempotent)
    - Returns the row count so we can verify after loading
    """
    print(f"  Loading {csv_path.name} → {table_name} ...")

    # Read CSV into pandas. dtype=str keeps everything as raw strings —
    # we do NOT cast types here. Bronze = raw.
    df = pd.read_csv(csv_path, dtype=str, low_memory=False)

    # Add ingestion timestamp. timezone.utc makes it timezone-aware.
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()

    # Register the DataFrame as a temporary view DuckDB can query.
    # Think of this as handing DuckDB a named reference to the DataFrame in memory.
    con.register("_temp_df", df)

    # Drop existing table (if it exists) and recreate from the temp view.
    # This is what makes the script idempotent.
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM _temp_df")

    # Unregister the temp view — good hygiene
    con.unregister("_temp_df")

    # Return row count for verification
    row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    return row_count


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    # Create the data directory if it doesn't exist yet
    Path("data").mkdir(exist_ok=True)

    # Connect to DuckDB. The file is created automatically if it doesn't exist.
    con = duckdb.connect(DB_PATH)
    print(f"Connected to DuckDB at: {DB_PATH}\n")

    results = {}

    for filename, table_name in FILE_TABLE_MAP.items():
        csv_path = RAW_DIR / filename

        # Check the file exists before trying to load it
        if not csv_path.exists():
            print(f"  WARNING: {csv_path} not found — skipping.")
            continue

        row_count = ingest_csv_to_bronze(con, csv_path, table_name)
        results[table_name] = row_count
        print(f"  ✓ {table_name}: {row_count:,} rows\n")

    # ---------------------------------------------------------------------------
    # VERIFICATION — print a summary and check expected row counts
    # ---------------------------------------------------------------------------
    print("=" * 50)
    print("INGESTION SUMMARY")
    print("=" * 50)
    for table, count in results.items():
        print(f"  {table:<35} {count:>10,} rows")

    # Basic sanity check: no table should be empty
    print()
    for table, count in results.items():
        if count == 0:
            print(f"  ❌ ALERT: {table} has 0 rows — something went wrong!")
        else:
            print(f"  ✓ {table} looks populated")

    con.close()
    print("\nDone. DuckDB connection closed.")


if __name__ == "__main__":
    main()