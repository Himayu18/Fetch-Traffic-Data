import requests
from datetime import datetime, timezone
from datetime import datetime
from pymongo import MongoClient
import os
import sys

API_KEY = os.getenv('TOMTOM_API_KEY')
MONGO_URI = os.getenv('MONGO_URI')

if not API_KEY or not MONGO_URI:
    print("ERROR: Missing API_KEY or MONGO_URI environment variables.")
    sys.exit(1)

base_url = "https://api.tomtom.com"
version_number = "4"
style = "absolute"
zoom = "10"
response_format = "json"
unit = "KMPH"
thickness = "2"
open_lr = "false"

road_points = {
    # your points dictionary here
}

def fetch_traffic_data(point):
    url = (
        f"{base_url}/traffic/services/{version_number}/flowSegmentData/"
        f"{style}/{zoom}/{response_format}?"
        f"key={API_KEY}&point={point}&unit={unit}&thickness={thickness}"
        f"&openLr={open_lr}&jsonp="
    )
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def main():
    client = MongoClient(MONGO_URI)
    db = client['thane_traffic']
    collection = db['traffic_flow_data']

    now = datetime.now(timezone.utc)  # UTC timestamp is better for logging

    hour = now.hour
    if not (6 <= hour < 24):
        print(f"Outside allowed hours ({hour}), exiting.")
        return

    for road, points in road_points.items():
        print(f"Fetching data for {road} ...")
        for pt in points:
            try:
                data = fetch_traffic_data(pt)
                # Extract required fields safely
                flow_segment = data.get('flowSegmentData', {})

                record = {
                    "road": road,
                    "point": pt,
                    "timestamp": now,
                    "roadName": flow_segment.get('roadName'),
                    "frc": flow_segment.get('frc'),
                    "currentSpeed": flow_segment.get('currentSpeed'),
                    "freeFlowSpeed": flow_segment.get('freeFlowSpeed'),
                    "currentTravelTime": flow_segment.get('currentTravelTime'),
                    "freeFlowTravelTime": flow_segment.get('freeFlowTravelTime'),
                    "confidence": flow_segment.get('confidence'),
                    "roadClosure": flow_segment.get('roadClosure')
                }
                collection.insert_one(record)
                print(f"Inserted data for {road} at {pt}")
            except Exception as e:
                print(f"Error fetching/inserting data for {road} at {pt}: {e}")

if __name__ == "__main__":
    main()
