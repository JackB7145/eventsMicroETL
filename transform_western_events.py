import pandas as pd
from datetime import datetime

INPUT_CSV = "western_events.csv"
OUTPUT_CSV = "western_events_transformed.csv"

def main():
    df = pd.read_csv(INPUT_CSV)

    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce", utc=True)

    today = datetime.utcnow().date()
    df = df[df["event_date"].dt.date > today]

    positive_keywords = [
        "music", "concert", "performance", "festival", "party", "dance",
        "theatre", "play", "film", "movie", "art", "exhibit", "show",
        "cultural", "lecture", "seminar", "yoga", "fitness", "zumba",
        "wellness", "career", "networking", "fair"
    ]

    negative_keywords = [
        "orientation", "resume", "advising", "training", "drop-in",
        "staff only", "research talk", "academic", "department meeting", "lecture"
    ]

    def is_couples_event(row):
        text = f"{row['event_name']} {row.get('description', '')}".lower()
        has_positive = any(kw in text for kw in positive_keywords)
        has_negative = any(kw in text for kw in negative_keywords)
        return has_positive and not has_negative

    df = df[df.apply(is_couples_event, axis=1)]

    df["event_name"] = df["event_name"].apply(lambda x: f"Western: {x.strip()}" if isinstance(x, str) else x)

    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Transformed {len(df)} events → {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
