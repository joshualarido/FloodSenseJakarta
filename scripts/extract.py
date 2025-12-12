import pandas as pd
import json

def extract_clean(path):
    #load raw data
    df = pd.read_csv(path)

    #uid from pkey, lat, lon, timestamp from date + time, flood depth
    df['uid'] = df['pkey'].astype(str)
    df['lat'] = df['lat']
    df['lon'] = df['long']

    #drop rows with missing coordinates
    df = df.dropna(subset=['lat', 'lon'])

    #timedate
    def clean_timestamp(value):
        value = str(value).strip()

        # Format is always: "DD/MM/YYYY something"
        date_part, time_part = value.split(" ")

        # Case 1 — time is valid (contains ":")
        if ":" in time_part:
            ts = pd.to_datetime(value, format="%d/%m/%Y %H:%M:%S", errors="coerce")
            if ts is not None:
                return ts, 0

        # Case 2 — time is integer → assign 12:00:00
        fixed = f"{date_part} 12:00:00"
        ts = pd.to_datetime(fixed, format="%d/%m/%Y %H:%M:%S")
        return ts, 1

    #apply timestamp cleaning
    df['raw_timestamp'] = df['date'].astype(str) + " " + df['time'].astype(str)
    timestamps = df['raw_timestamp'].apply(clean_timestamp)
    df['timestamp_utc'] = timestamps.apply(lambda x: x[0])
    df['missing_time_flag'] = timestamps.apply(lambda x: x[1])

    #flood depth extraction from JSON in report_dat
    def extract_depth(row):
        try:
            data = json.loads(row)
            if "flood_depth" in data:
                return data["flood_depth"]
        except:
            return None
        return None

    df['flood_depth_cm'] = df['report_dat'].apply(extract_depth)

    #final cleaned dataframe
    cleaned = df[['uid', 'lat', 'lon', 'timestamp_utc', 'missing_time_flag', 'flood_depth_cm']]

    return cleaned


if __name__ == "__main__":
    cleaned = extract_clean("./data_raw/jakarta_disaster_2020_2021.csv")

    # Save to file
    cleaned.to_csv("./data/cleaned_events.csv", index=False)

    print("\n===== TOP 20 CLEANED ROWS =====\n")
    print(cleaned.head(20))
