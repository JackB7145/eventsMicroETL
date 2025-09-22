import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from dateutil import parser
import os

load_dotenv()

DB_URL = os.getenv('DB_URL')
CSV_FILE = "events_london_on.csv"

df = pd.read_csv(CSV_FILE)

df = df.rename(columns={
    "title": "event_name",
    "date.start_date": "event_date",
    "description": "description",
    "link": "link"
})
expected_cols = ["event_name", "event_date", "description", "link"]
df = df[[c for c in expected_cols if c in df.columns]].fillna("")

def safe_parse_date(val):
    try:
        if not val or str(val).strip() == "":
            return None
        return parser.parse(str(val), fuzzy=True).date()
    except Exception:
        return None

df["event_date"] = df["event_date"].apply(safe_parse_date)

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

records = list(df.itertuples(index=False, name=None))

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
