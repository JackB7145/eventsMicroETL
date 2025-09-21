from serpapi import GoogleSearch
from dotenv import load_dotenv
import pandas as pd
import os, json, time

load_dotenv()

API_KEY = os.getenv("SERPAPI_KEY") or os.getenv("API_KEY")
if not API_KEY:
    raise RuntimeError("Missing SERPAPI_KEY/API_KEY in environment")

params = {
    "engine": "google_events",
    "q": "events in London, Ontario",   
    "hl": "en",
    "gl": "ca",
    "google_domain": "google.ca",
    "location": "London, Ontario, Canada",
    "start": 0
}

rows = []
page = 1
while True:
    search = GoogleSearch({**params, "api_key": API_KEY})
    data = search.get_dict()

    if "error" in data:
        print("SerpApi error:", data["error"])
        break

    events = data.get("events_results", [])
    if not events:
        break

    rows.extend(events)

    if not data.get("serpapi_pagination", {}).get("next"):
        break

    params["start"] += 10
    page += 1
    time.sleep(0.2)

if not rows:
    print("No events returned. Try broadening the query, or remove 'location' and keep gl/google_domain='ca'.")
else:
    df = pd.json_normalize(rows, sep=".")

    preferred_cols = [
        "title",
        "date",                
        "date.start_date",     
        "date.when",
        "address",
        "venue.name",
        "ticket_info.link",
        "ticket_info.price",
        "link",
        "description",
        "event_location_map.latitude",
        "event_location_map.longitude",
        "event_location",
        "thumbnail",
        "place_id"
    ]
    ordered = [c for c in preferred_cols if c in df.columns] + [c for c in df.columns if c not in preferred_cols]
    df = df[ordered]

    dedupe_keys = [c for c in ["link", "title", "date", "date.start_date"] if c in df.columns]
    if dedupe_keys:
        df = df.drop_duplicates(subset=dedupe_keys, keep="first")

    out_path = "events_london_on.csv"
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Wrote {len(df):,} rows → {out_path}")
