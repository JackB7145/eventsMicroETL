import pandas as pd
from dateutil import parser
import os
from datetime import date

INPUT_CSV = "events_london_on.csv"
OUTPUT_CSV = "serpapi_events_transformed.csv"

df = pd.read_csv(INPUT_CSV)

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

today = date.today()
df = df[df["event_date"].notna() & (df["event_date"] >= today)]

df = df.drop_duplicates(subset=["event_name"], keep="first")

df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
print(f"Transformed {len(df)} events → {OUTPUT_CSV}")
