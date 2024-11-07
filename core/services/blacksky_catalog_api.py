import requests
import geohash2
from datetime import datetime, timedelta
import csv
import json
from dateutil import parser
import argparse
import os
from tqdm import tqdm
import pygeohash as pgh
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil
from PIL import Image, ImageChops
import numpy as np
import rasterio
from rasterio.transform import from_bounds
from utils import check_csv_and_rename_output_dir
from decouple import config

# Get the terminal size
columns = shutil.get_terminal_size().columns

# Configuration
BLACKSKY_BASE_URL = "https://api.blacksky.com"
AUTH_TOKEN = config("BLACKSKY_API_KEY")
MAX_THREADS = 10
BATCH_SIZE = 28


def remove_black_borders(img):
    """Remove black borders from the image."""
    bg = Image.new(img.mode, img.size, img.getpixel((0, 0)))
    diff = ImageChops.difference(img, bg)
    bbox = diff.getbbox()
    if bbox:
        return img.crop(bbox)
    return img

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


def georectify_image(png_path, bbox, geotiffs_folder, image_id, target_resolution=(1500, 1500)):
    try:
        with Image.open(png_path) as img:
            img = remove_black_borders(img)
            img = img.resize(target_resolution, Image.Resampling.LANCZOS)
            img_array = np.array(img)

        width, height = target_resolution
        
        left, bottom, right, top = bbox

        transform = from_bounds(left, bottom, right, top, width, height)

        geotiff_name = f"{image_id}.tif"
        geotiff_path = os.path.join(geotiffs_folder, geotiff_name)

        if len(img_array.shape) == 2:
            img_array = np.expand_dims(img_array, axis=-1)
            count = 1
        else:
            count = img_array.shape[2]

        # Write the GeoTIFF file using rasterio
        with rasterio.open(
                geotiff_path,
                'w',
                driver='GTiff',
                height=img_array.shape[0],
                width=img_array.shape[1],
                count=count,
                dtype=img_array.dtype,
                crs='EPSG:4326',
                transform=transform) as dst:
            for i in range(1, count + 1):
                dst.write(img_array[:, :, i - 1], i)

    except Exception as e:
        pass

def get_blacksky_collections(
    auth_token,
    bbox=None,
    datetime_range=None,
):
    """
    Fetches collections from the BlackSky API.
    """
    url = f"{BLACKSKY_BASE_URL}/v1/catalog/stac/search"

    headers = {"Accept": "application/json", "Authorization": auth_token}
    params = {
        "bbox": bbox,
        "time": datetime_range,
    }

    try:
        response = requests.get(url, params=params, headers=headers)
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")

    return None

def save_image(feature):
    """Downloads an image from the provided URL and saves it to the specified path."""
    try:
        url = feature.get("assets", {}).get("browseUrl", {}).get("href")
        save_path = os.path.join(OUTPUT_THUMBNAILS_FOLDER, f"{feature.get('id')}.png")
        headers = {"Content-Type": "application/json", "Authorization": AUTH_TOKEN}
        response = requests.get(url, headers=headers, stream=True)

        if response.status_code == 200:
            with open(save_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            georectify_image(save_path, feature.get("bbox"), OUTPUT_GEOTIFF_FOLDER, feature.get("id"))
        else:
            print(f"Error during download: {response.status_code}")
            print(response.text)
    except Exception as e:
        return False


def download_thumbnails(features):
    """Download and save thumbnail images for the given features."""

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {
            executor.submit(save_image, feature): feature for feature in features
        }

        for future in as_completed(futures):
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
        feature_id = feature.get("id", "unknown")
        geojson_data = {
            "type": "FeatureCollection",
            "features": [feature],  # Save each feature individually
        }

        geojson_filename = f"{feature_id}.geojson"
        geojson_path = os.path.join(OUTPUT_GEOJSON_FOLDER, geojson_filename)

        with open(geojson_path, "w") as geojson_file:
            json.dump(geojson_data, geojson_file, indent=4)


def process_csv(features):
    """Appends feature properties and geometries to the CSV file."""
    write_header = (
        not os.path.exists(OUTPUT_CSV_FILE) or os.path.getsize(OUTPUT_CSV_FILE) == 0
    )

    with open(OUTPUT_CSV_FILE, mode="a", newline="") as csv_file:
        csv_writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)

        if features:
            header = (
                ["id"] + list(features[0].get("properties", {}).keys()) + ["geometry"]
            )
            if write_header:
                csv_writer.writerow(header)

            for feature in features:
                feature_id = feature.get("id", "unknown")
                properties = feature.get("properties", {})
                geometry = feature.get("geometry", {})

                row = (
                    [feature_id]
                    + [sanitize_value(properties.get(key)) for key in header[1:-1]]
                    + [json.dumps(geometry)]
                )
                csv_writer.writerow(row)


def fetch_and_process_records(auth_token, bbox, start_time, end_time):
    """Fetches records from the BlackSky API and processes them."""
    records = get_blacksky_collections(
        auth_token, bbox=bbox, datetime_range=f"{start_time}/{end_time}"
    )
    if records is None:
        return
    features = records.get("features", [])
    # Process and save data
    process_csv(features)  # CSV for all features
    process_geojson(features)  # Separate GeoJSON for each feature
    download_thumbnails(features)  # Thumbnails for each feature


def main(START_DATE, END_DATE, OUTPUT_DIR, BBOX):
    bboxes = [BBOX]
    current_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date = datetime.strptime(END_DATE, "%Y-%m-%d")
    global BATCH_SIZE
    date_difference = (end_date - current_date).days + 1
    if date_difference < BATCH_SIZE:
        BATCH_SIZE = date_difference
    duration = math.ceil(date_difference / BATCH_SIZE)
    print("-" * columns)
    description = (
        f"Processing Blacksky Catalog \nDate Range: {current_date.date()} to {end_date.date()} \n"
        f"lat: {LAT} and lon: {LON} Range: {RANGE} \nOutput Directory: {OUTPUT_DIR}"
    )
    print(description)
    print("-" * columns)
    print("Batch Size: ", BATCH_SIZE, ", days: ", date_difference)
    print("Duration :", duration, "batch")

    with tqdm(total=duration, desc="", unit="batch") as pbar:
        while current_date <= end_date:  # Inclusive of end_date
            start_time = current_date.strftime("%Y-%m-%d")
            end_time = (current_date + timedelta(days=BATCH_SIZE)).strftime("%Y-%m-%d")

            for bbox in bboxes:
                fetch_and_process_records(AUTH_TOKEN, bbox, start_time, end_time)

            current_date += timedelta(days=BATCH_SIZE)  # Move to the next day
            pbar.update(1)  # Update progress bar

        pbar.clear()
    tqdm.write("Completed processing BlackSky data")


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser(
        description="BlackSky Catelog API Executor"
    )
    argument_parser.add_argument("--start-date", required=True, help="Start date")
    argument_parser.add_argument("--end-date", required=True, help="End date")
    argument_parser.add_argument("--lat", required=True, type=float, help="Latitude")
    argument_parser.add_argument("--long", required=True, type=float, help="Longitude")
    argument_parser.add_argument(
        "--range", required=True, type=float, help="Range value"
    )
    argument_parser.add_argument("--output-dir", required=True, help="Output directory")
    argument_parser.add_argument('--bbox', required=True, help='Bounding Box')

    args = argument_parser.parse_args()
    START_DATE = args.start_date
    END_DATE = args.end_date
    OUTPUT_DIR = args.output_dir + f"/blacksky/{START_DATE}_{END_DATE}"

    RANGE = int(args.range)
    LAT, LON = args.lat, args.long

    BBOX = args.bbox.replace("t", "-")
    print(f"Generated BBOX: {BBOX}")

    OUTPUT_THUMBNAILS_FOLDER = f"{OUTPUT_DIR}/thumbnails"
    os.makedirs(OUTPUT_THUMBNAILS_FOLDER, exist_ok=True)

    OUTPUT_GEOJSON_FOLDER = f"{OUTPUT_DIR}/geojsons"
    os.makedirs(OUTPUT_GEOJSON_FOLDER, exist_ok=True)

    OUTPUT_GEOTIFF_FOLDER = f"{OUTPUT_DIR}/geotiffs"
    os.makedirs(OUTPUT_GEOTIFF_FOLDER, exist_ok=True)

    OUTPUT_CSV_FILE = f"{OUTPUT_DIR}/output_blacksky.csv"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    main(START_DATE, END_DATE, OUTPUT_DIR, BBOX)

    check_csv_and_rename_output_dir(OUTPUT_DIR, START_DATE, END_DATE, args.output_dir, "blacksky")
