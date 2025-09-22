import os
import glob
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DB_URL")

def load_csvs_to_db():
    csv_files = glob.glob("*_events_transformed.csv")
    if not csv_files:
        raise FileNotFoundError("No transformed event CSV files found.")

    dfs = []
    for file in csv_files:
        df = pd.read_csv(file)

        expected_cols = ["event_name", "event_date", "description", "link"]
        for col in expected_cols:
            if col not in df.columns:
                df[col] = ""

        df = df[expected_cols].fillna("")
        dfs.append(df)

    final_df = pd.concat(dfs, ignore_index=True)

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            event_name TEXT UNIQUE,
            event_date TIMESTAMP,
            description TEXT,
            link TEXT
        )
    """)

    records = list(final_df.itertuples(index=False, name=None))

    insert_query = """
        INSERT INTO events (event_name, event_date, description, link) 
        VALUES %s
        ON CONFLICT (event_name) DO NOTHING;
    """

    execute_values(cur, insert_query, records)
    conn.commit()

    cur.close()
    conn.close()

    print(f"Inserted {len(records)} rows (duplicates skipped).")

if __name__ == "__main__":
    load_csvs_to_db()
