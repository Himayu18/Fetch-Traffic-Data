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
    "SH-42 (Ghodbunder Road)": [
        "19.2500,73.0500",  # Kapurbawdi approx.
        "19.2500,72.9800"   # Near Ghodbunder junction
    ],
    "Eastern Express Highway": [
        "19.2183,72.9781",  # Thane approx.
        "19.0760,72.8777"   # Mumbai approx.
    ],
    "LBS Marg": [
        "19.1960,72.9600",  # Thane start
        "19.0500,72.8700"   # Sion approx.
    ],
    "Thane-Belapur Road": [
        "19.1800,73.0100",  # Kalwa
        "19.0450,73.0150"   # Navi Mumbai (Turbhe)
    ],
    "Sion-Panvel Expressway": [
        "19.0600,72.8850",  # Sion
        "18.9800,73.1100"   # Kalamboli
    ],
    "Mumbai-Nashik Expressway (NH 3)": [
        "19.2183,72.9781",  # Thane start
        "20.0110,73.7900"   # Nashik approx.
    ],
    "MDR 64": [
        "19.4100,73.1800",  # Murbad
        "19.4300,73.4800"   # Shahapur
    ],
    "SH 40": [
        "19.3200,73.1000",  # Shilphata
        "19.3000,73.1300"   # Bhiwandi
    ],
    "MDR 62": [
        "19.2200,73.0000"   # Local villages approx.
    ],
    "Ring Road (Kalyan-Dombivli)": [
        "19.2400,73.1300",  # Kalyan
        "19.2200,73.1000"   # Dombivli
    ],
    "Multi-Modal Corridor": [
        "18.6500,72.8700",  # Alibaug
        "19.4200,72.8400"   # Virar
    ]
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

    now = datetime.now(timezone.utc)  
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
