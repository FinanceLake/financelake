import pandas as pd
import numpy as np
import time
from faker import Faker

fake = Faker()

def generate_data(n=1_000_000):
    """Generate a dataset with fake user data."""
    print("Generating data...")
    start = time.time()

    data = {
        "id": np.arange(1, n + 1),
        "name": [fake.name() for _ in range(n)],
        "email": [fake.email() for _ in range(n)],
        "address": [fake.address() for _ in range(n)],
        "created_at": [fake.date_time_this_decade() for _ in range(n)]
    }

    df = pd.DataFrame(data)
    duration = time.time() - start
    print(f"Data generated in {duration:.2f} seconds")
    return df, duration


def process_data(df):
    """Simulate processing the data (e.g., cleaning, transforming)."""
    print("Processing data...")
    start = time.time()

    # Example: lowercase emails and extract year from created_at
    df['email'] = df['email'].str.lower()
    df['year'] = df['created_at'].dt.year

    duration = time.time() - start
    print(f"Data processed in {duration:.2f} seconds")
    return df, duration


def store_data(df, path="output.csv"):
    """Store the data to CSV (can adapt to DB later)."""
    print("Storing data...")
    start = time.time()

    df.to_csv(path, index=False)

    duration = time.time() - start
    print(f"Data stored in {duration:.2f} seconds")
    return duration


def main():
    df, ingest_time = generate_data()
    df, process_time = process_data(df)
    store_time = store_data(df)

    print("\n--- Benchmark Summary ---")
    print(f"Ingest time:   {ingest_time:.2f} seconds")
    print(f"Process time:  {process_time:.2f} seconds")
    print(f"Store time:    {store_time:.2f} seconds")
    print(f"Total time:    {ingest_time + process_time + store_time:.2f} seconds")


if __name__ == "__main__":
    main()

