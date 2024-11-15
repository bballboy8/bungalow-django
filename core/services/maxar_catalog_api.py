import requests
import geohash2
from datetime import datetime, timedelta
import csv
import io
import time
import logging
import json
from dateutil import parser
import argparse
import os
from tqdm import tqdm
import pygeohash as pgh
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil
from utils import check_csv_and_rename_output_dir
from decouple import config

# Get the terminal size
columns = shutil.get_terminal_size().columns

# Configuration
AUTH_TOKEN = config("MAXAR_API_KEY")
MAXAR_BASE_URL = "https://api.maxar.com/discovery/v1"
MAX_THREADS = 10
BATCH_SIZE = 28



def latlon_to_geohash(lat, lon, range_km):
    # Map the range to geohash precision
    precision = (
        2
        if range_km > 100
        else 4 if range_km > 20 else 6 if range_km > 5 else 8 if range_km > 1 else 10
    )
    return geohash2.encode(lat, lon, precision=precision)



def get_geohash_corners(geohash: str) -> str:
    center_lat, center_lon = pgh.decode(geohash)
    lat_err, lon_err = pgh.decode_exactly(geohash)[-2:]
    
    top_left = (center_lat + lat_err, center_lon - lon_err)
    top_right = (center_lat + lat_err, center_lon + lon_err)
    bottom_left = (center_lat - lat_err, center_lon - lon_err)
    bottom_right = (center_lat - lat_err, center_lon + lon_err)
    
    lats = [top_left[0], top_right[0], bottom_left[0], bottom_right[0]]
    lons = [top_left[1], top_right[1], bottom_left[1], bottom_right[1]]
    
    xmin = math.ceil(min(lons))
    ymin = math.ceil(min(lats))
    xmax = math.ceil(max(lons))
    ymax = math.ceil(max(lats))
    
    # Format as a bbox string
    return f"{xmin},{ymin},{xmax},{ymax}"


def calculate_withhold_time(acquisition_date, publication_date):
    """Calculate the withhold time as total hours and human-readable format."""
    acq_date = parser.isoparse(acquisition_date)
    pub_date = parser.isoparse(publication_date)
    delta = pub_date - acq_date
    total_hours = int(delta.total_seconds() / 3600)  # convert to hours
    days = delta.days
    hours = delta.seconds // 3600
    return f"{days} days {hours} hours", total_hours


def sanitize_value(value):
    """Ensure values are suitable for GeoJSON by converting them to strings if necessary, except for None."""
    if isinstance(value, (int, float)):
        return str(value)
    if value is None:
        return None  # Keep None as is to maintain distinction in outputs
    return value


def format_datetime(datetime_str):
    """Format datetime string to 'YYYY-MM-DD HH:MM:SS.xx'."""
    try:
        dt = parser.isoparse(datetime_str)
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[
            :-4
        ]  # Truncate to two decimal places
    except (ValueError, TypeError):
        return datetime_str


def format_float(value, precision=2):
    """Format float to a string with the given precision."""
    try:
        return f"{float(value):.{precision}f}"
    except (ValueError, TypeError):
        return None


def get_maxar_collections(
    auth_token,
    limit=1,
    page=1,
    bbox=None,
    datetime_range=None,
):
    """
    Fetches collections from the Maxar API.
    """
    collections = [ "wv01", "wv02"]
    collections_str = ",".join(collections)
    url = f"https://api.maxar.com/discovery/v1/search?collections={collections_str}&bbox={bbox}&datetime={datetime_range}&limit={limit}&page={page}"

    headers = {"Accept": "application/json", "MAXAR-API-KEY": auth_token}

    try:
        response = requests.request("GET",url, headers=headers)
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {response.text}")

    return None

def save_image(feature):
    """Downloads an image from the provided URL and saves it to the specified path."""
    try:
        url = feature.get('assets', {}).get('browse', {}).get('href')
        save_path_tif = os.path.join(OUTPUT_GEOTIFFS_FOLDER, f"{feature.get('id')}.tif")
        save_path_png = os.path.join(OUTPUT_THUMBNAILS_FOLDER, f"{feature.get('id')}.png")
        headers = {"Accept": "application/json", "MAXAR-API-KEY": AUTH_TOKEN}
        response = requests.get(url, stream=True, headers=headers, timeout=(10, 30))
        response.raise_for_status()
        content = response.content
        with open(save_path_tif, 'wb') as out_file:
            out_file.write(content)

        with open(save_path_png, 'wb') as out_file:
            out_file.write(content)
        
        return True
    except requests.RequestException as e:
        return False

def download_thumbnails(features):
    """Download and save thumbnail images for the given features."""

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(save_image, feature): feature for feature in features}

        for future in as_completed(futures):
            feature = futures[future]
            try:
                result = future.result()
                if result:
                    # print(f"Successfully downloaded thumbnail for feature {feature.get('id')}")
                    pass
                else:
                    # print(f"Failed to download thumbnail for feature {feature.get('id')}")
                    pass
            except Exception as e:
                # print(f"Exception occurred while downloading thumbnail for feature {feature.get('id')}: {e}")
                pass

def process_geojson(features):
    """Saves each feature as a separate GeoJSON file."""
    for feature in features:
        feature_id = feature.get('id', 'unknown')
        geojson_data = {
            "type": "FeatureCollection",
            "features": [feature]  # Save each feature individually
        }

        geojson_filename = f"{feature_id}.geojson"
        geojson_path = os.path.join(OUTPUT_GEOJSON_FOLDER, geojson_filename)

        with open(geojson_path, 'w') as geojson_file:
            json.dump(geojson_data, geojson_file, indent=4)
        
def process_csv(features):
    """Appends feature properties and geometries to the CSV file."""
    write_header = not os.path.exists(OUTPUT_CSV_FILE) or os.path.getsize(OUTPUT_CSV_FILE) == 0

    with open(OUTPUT_CSV_FILE, mode='a', newline='') as csv_file:
        csv_writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)

        if features:
            header = ["id"] + list(features[0].get('properties', {}).keys()) + ["geometry"]
            if write_header:
                csv_writer.writerow(header)

            for feature in features:
                feature_id = feature.get('id', 'unknown')
                properties = feature.get('properties', {})
                geometry = feature.get('geometry', {})
                
                row = [feature_id] + [sanitize_value(properties.get(key)) for key in header[1:-1]] + [json.dumps(geometry)]
                csv_writer.writerow(row)

def fetch_and_process_records(auth_token, bbox, start_time, end_time):
    """Fetches records from the Maxar API and processes them."""
    page = 1
    all_features = []

    while True:
        records = get_maxar_collections(auth_token, bbox=bbox, datetime_range=f"{start_time}/{end_time}", page=page)
        if not records:
            break
        
        features = records.get('features', [])
        all_features.extend(features)

        if not any(link.get("rel") == "next" for link in records.get("links", [])):
            break
        
        page += 1

    # Process and save data
    process_geojson(all_features)    # Separate GeoJSON for each feature
    process_csv(all_features)        # CSV for all features
    download_thumbnails(all_features) # Thumbnails for each feature

def main(START_DATE, END_DATE, OUTPUT_DIR, BBOX):
    bboxes = [BBOX]
    current_date = datetime.strptime(START_DATE, '%Y-%m-%d')
    end_date = datetime.strptime(END_DATE, '%Y-%m-%d')
    global BATCH_SIZE
    date_difference = (end_date - current_date).days + 1  # Inclusive of end_date
    if date_difference < BATCH_SIZE:
            BATCH_SIZE = date_difference
    duration = math.ceil(date_difference / BATCH_SIZE)

    print("-" * columns)
    description = (f"Processing Maxar Catalog \nDate Range: {current_date.date()} to {end_date.date()} \n"
                   f"lat: {LAT} and lon: {LON} Range: {RANGE} \nOutput Directory: {OUTPUT_DIR}")
    print(description)
    print("-" * columns)
    print("Batch Size: ", BATCH_SIZE, ", days: ", date_difference)
    print("Duration :", duration, "batch")

    with tqdm(total=duration, desc="", unit="batch") as pbar:
        while current_date <= end_date:  # Inclusive of end_date
            start_time = current_date.strftime('%Y-%m-%d')
            end_time = (current_date + timedelta(days=BATCH_SIZE)).strftime('%Y-%m-%d')
            for bbox in bboxes:
                fetch_and_process_records(AUTH_TOKEN, bbox, start_time, end_time)

            current_date += timedelta(days=BATCH_SIZE)  # Move to the next day
            pbar.update(1)  # Update progress bar


        pbar.clear()
    tqdm.write("Completed processing Maxar data")


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser(description='Maxar Catelog API Executor')
    argument_parser.add_argument('--start-date', required=True, help='Start date')
    argument_parser.add_argument('--end-date', required=True, help='End date')
    argument_parser.add_argument('--lat', required=True, type=float, help='Latitude')
    argument_parser.add_argument('--long', required=True, type=float, help='Longitude')
    argument_parser.add_argument('--range', required=True, type=float, help='Range value')
    argument_parser.add_argument('--output-dir', required=True, help='Output directory')
    argument_parser.add_argument('--bbox', required=True, help='Bounding Box')

    args = argument_parser.parse_args()
    START_DATE = args.start_date
    END_DATE = args.end_date
    OUTPUT_DIR = args.output_dir + f"/maxar/{START_DATE}_{END_DATE}"

    RANGE = int(args.range)
    LAT, LON = args.lat, args.long
    
    BBOX = args.bbox.replace("t", "-")
    print(f"Generated BBOX: {BBOX}")


    OUTPUT_GEOTIFFS_FOLDER = f"{OUTPUT_DIR}/geotiffs"
    os.makedirs(OUTPUT_GEOTIFFS_FOLDER, exist_ok=True)

    OUTPUT_THUMBNAILS_FOLDER = f"{OUTPUT_DIR}/thumbnails"
    os.makedirs(OUTPUT_THUMBNAILS_FOLDER, exist_ok=True)

    OUTPUT_GEOJSON_FOLDER = f"{OUTPUT_DIR}/geojsons"
    os.makedirs(OUTPUT_GEOJSON_FOLDER, exist_ok=True)

    OUTPUT_CSV_FILE = f"{OUTPUT_DIR}/output_maxar.csv"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Check if the directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    main(
        START_DATE,
        END_DATE,
        OUTPUT_DIR,
        BBOX
    )
    check_csv_and_rename_output_dir(OUTPUT_DIR, START_DATE, END_DATE, args.output_dir, "maxar")
