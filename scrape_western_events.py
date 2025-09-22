import requests
import pandas as pd

URL = "https://www.uwo.ca/events/_data/current-live.json"

def run():
    resp = requests.get(URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # Inspect structure; some feeds wrap events under a key like "items" or "events"
    if isinstance(data, dict):
        events = data.get("items") or data.get("events") or []
    elif isinstance(data, list):
        events = data
    else:
        raise ValueError("Unexpected JSON structure from Western events feed")

    results = []
    for ev in events:
        results.append({
            "event_name": ev.get("title", "").strip(),
            "event_date": ev.get("startDate", "").strip(),
            "description": ev.get("description", "").strip(),
            "link": ev.get("url", "").strip(),
        })

    df = pd.DataFrame(results)
    df.to_csv("western_events.csv", index=False, encoding="utf-8")
    print(f"Scraped {len(df)} events → western_events.csv")

if __name__ == "__main__":
    run()
