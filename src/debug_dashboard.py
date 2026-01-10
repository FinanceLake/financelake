import duckdb
import os
import pandas as pd

GOLD_PATH = "/app/data/lake/gold"

def check_gold_data():
    print(f"Checking Gold Path: {GOLD_PATH}")
    if not os.path.exists(GOLD_PATH):
        print("Path does not exist!")
        return

    # List files manually to see structure
    print("Directory structure:")
    for root, dirs, files in os.walk(GOLD_PATH):
        for file in files:
            if file.endswith(".parquet"):
                print(os.path.join(root, file))
                break # Just show one per dir
        if len(files) > 0:
            break

    con = duckdb.connect(database=':memory:')
    
    # Try 3: Delta Extension
    print("\n--- Attempt 3: Delta Extension ---")
    try:
        con.execute("INSTALL delta;")
        con.execute("LOAD delta;")
        print("Delta extension loaded.")
        query = f"SELECT * FROM delta_scan('{GOLD_PATH}')"
        df = con.execute(query).df()
        print("Columns:", df.columns.tolist())
        if 'symbol' in df.columns:
            print("SUCCESS: 'symbol' column found via Delta extension.")
    except Exception as e:
        print(f"Delta Extension Error: {e}")

    # Try 4: Explicit Glob
    print("\n--- Attempt 4: Explicit Glob (symbol=*) ---")
    try:
        query = f"SELECT * FROM read_parquet('{GOLD_PATH}/symbol=*/*.parquet', hive_partitioning=true)"
        df = con.execute(query).df()
        print("Columns:", df.columns.tolist())
        if 'symbol' in df.columns:
            print("SUCCESS: 'symbol' column found via Explicit Glob.")
    except Exception as e:
        print(f"Explicit Glob Error: {e}")

if __name__ == "__main__":
    check_gold_data()
