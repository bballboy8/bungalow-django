'''

url:  https://github.com/planetlabs/notebooks/blob/master/jupyter-notebooks/Data-API/planet_python_client_introduction.ipynb

'''

# Set your API key here
import requests
import json
import csv
import io
import pygeohash as pgh
from datetime import datetime, timedelta
from dateutil import parser
import argparse
import os
from tqdm import tqdm
import math
import shutil
from utils import check_csv_and_rename_output_dir, latlon_to_geojson, calculate_bbox_npolygons
from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil
from PIL import Image, ImageChops
import numpy as np
import rasterio
from rasterio.transform import from_bounds
from decouple import config

# Get the terminal size
columns = shutil.get_terminal_size().columns

# Input configuration
API_KEY = config("PLANET_API_KEY") 
# START_DATE = '2024-01-01'  # Specify the start date in 'YYYY-MM-DD' format
# END_DATE = '2024-08-07'    # Specify the end date in 'YYYY-MM-DD' format
GEOHASH = 'w'              # Specify the initial geohash
GEOHASH_LENGTH = 2         # Specify the desired geohash length
ITEM_TYPE = "SkySatCollect"  # Specify the item type
BATCH_SIZE = 28
MAX_THREADS = 10
# Output files
# OUTPUT_CSV_FILE = r'O:\Professional__Work\Heimdall\planet\output_planet.csv'
# OUTPUT_GEOJSON_FILE = r'O:\Professional__Work\Heimdall\planet\output_planet.geojson'


# Function to get the corners of the geohash
def get_geohash_corners(geohash):
    center_lat, center_lon = pgh.decode(geohash)
    lat_err, lon_err = pgh.decode_exactly(geohash)[-2:]
    top_left = (center_lat + lat_err, center_lon - lon_err)
    top_right = (center_lat + lat_err, center_lon + lon_err)
    bottom_left = (center_lat - lat_err, center_lon - lon_err)
    bottom_right = (center_lat - lat_err, center_lon + lon_err)
    return {
        "top_left": top_left,
        "top_right": top_right,
        "bottom_left": bottom_left,
        "bottom_right": bottom_right
    }


# Function to convert geohash to GeoJSON
def geohash_to_geojson(geohash_str: str) -> dict:
    corners = get_geohash_corners(geohash_str)
    return {
        "type": "Polygon",
        "coordinates": [[
            [corners["top_left"][1], corners["top_left"][0]],  # NW corner
            [corners["top_right"][1], corners["top_right"][0]],  # NE corner
            [corners["bottom_right"][1], corners["bottom_right"][0]],  # SE corner
            [corners["bottom_left"][1], corners["bottom_left"][0]],  # SW corner
            [corners["top_left"][1], corners["top_left"][0]]  # NW corner again to close the polygon
        ]]
    }

def latlon_to_geohash(lat, lon, range_km):
    # Map the range to geohash precision
    precision = (
        2 if range_km > 100 else
        4 if range_km > 20 else
        6 if range_km > 5 else
        8 if range_km > 1 else
        10
    )
    return pgh.encode(lat, lon, precision=precision)


# Function to generate an array of geohashes from a seed geohash
def generate_geohashes(seed_geohash, child_length):
    base32_chars = '0123456789bcdefghjkmnpqrstuvwxyz'

    def generate_geohashes_recursive(current_geohash, target_length, result):
        if len(current_geohash) == target_length:
            result.append(current_geohash)
            return
        for char in base32_chars:
            next_geohash = current_geohash + char
            generate_geohashes_recursive(next_geohash, target_length, result)

    result = []
    generate_geohashes_recursive(seed_geohash, len(seed_geohash) + child_length, result)
    return result


# Function to calculate the withhold time
def calculate_withhold_time(acquisition_date, publication_date):
    """Calculate the withhold time as total hours and formatted string."""
    acq_date = parser.isoparse(acquisition_date)
    pub_date = parser.isoparse(publication_date)
    delta = pub_date - acq_date
    total_hours = int(delta.total_seconds() / 3600)
    days, remaining_hours = divmod(total_hours, 24)
    readable = f"{days} days {remaining_hours} hours"
    return readable, total_hours


# Function to format datetime
def format_datetime(datetime_str):
    """Format datetime string to 'YYYY-MM-DD HH:MM:SS.xx'."""
    try:
        dt = parser.isoparse(datetime_str)
        return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-4]  # Truncate to two decimal places
    except (ValueError, TypeError):
        return datetime_str


# Function to format float
def format_float(value, precision=2):
    """Format float to a string with the given precision."""
    try:
        return f"{float(value):.{precision}f}"
    except ValueError:
        return "0.00"  # Default if there's an error


# Function to query Planet data
def query_planet_data(aoi_geojson, start_date, end_date, item_type):
    search_endpoint = "https://api.planet.com/data/v1/quick-search"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'api-key ' + API_KEY
    }
    request_payload = {
        "item_types": [item_type],
        "filter": {
            "type": "AndFilter",
            "config": [
                {
                    "type": "GeometryFilter",
                    "field_name": "geometry",
                    "config": aoi_geojson
                },
                {
                    "type": "DateRangeFilter",
                    "field_name": "acquired",
                    "config": {
                        "gte": start_date,
                        "lte": end_date
                    }
                }
            ]
        }
    }

    try:
        response = requests.post(search_endpoint, headers=headers, json=request_payload)
        return response.json()
    except requests.RequestException as e:
        # print(f"Failed to fetch data: {str(e)}")
        return []

def query_planet_paginated_data(next_url):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'api-key ' + API_KEY
    }
    try:
        response = requests.get(next_url, headers=headers)
        return response.json()
    except requests.RequestException as e:
        # print(f"Failed to fetch data: {str(e)}")
        return []

def process_geojson(features):
    """Saves each feature as a separate GeoJSON file."""
    for feature in features:
        feature_id = feature.get("properties", {}).get("id", "")
        geojson_filename = f"{feature_id}.geojson"
        geojson_path = os.path.join(OUTPUT_GEOJSON_FOLDER, geojson_filename)

        with open(geojson_path, "w") as geojson_file:
            json.dump(feature, geojson_file, indent=4)


def remove_black_borders(img):
    """Remove black borders from the image."""
    bg = Image.new(img.mode, img.size, img.getpixel((0, 0)))
    diff = ImageChops.difference(img, bg)
    bbox = diff.getbbox()
    if bbox:
        return img.crop(bbox)
    return img


def georectify_image(
    png_path, bbox, geotiffs_folder, image_id, target_resolution=(1500, 1500)
):
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
            "w",
            driver="GTiff",
            height=img_array.shape[0],
            width=img_array.shape[1],
            count=count,
            dtype=img_array.dtype,
            crs="EPSG:4326",
            transform=transform,
        ) as dst:
            for i in range(1, count + 1):
                dst.write(img_array[:, :, i - 1], i)

    except Exception as e:
        pass


def save_image(feature):
    """Downloads an image from the provided URL and saves it to the specified path."""
    try:
        url = feature.get("_links", {}).get("thumbnail", {})
        feature['bbox'] = calculate_bbox_npolygons(feature.get('geometry', {}))
        save_path = os.path.join(OUTPUT_THUMBNAILS_FOLDER, f"{feature.get('id')}.png")
        headers = {
            "Content-Type": "application/json",
            "Authorization": "api-key " + API_KEY,
        }
        response = requests.get(url, headers=headers, stream=True)

        if response.status_code == 200:
            with open(save_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            georectify_image(
                save_path, feature.get("bbox"), OUTPUT_GEOTIFF_FOLDER, feature.get("id")
            )
        else:
            # print(f"Error during download: {response.status_code}")
            # print(response.text)
            pass
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


# Function to save features to CSV and GeoJSON
def save_features_to_files(features, output_dir='.'):

    # Prepare GeoJSON output
    geojson_features = []

    with open(OUTPUT_CSV_FILE, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)

        # Write the header row
        csv_writer.writerow([
            'id', 'geometry', 'acquired', 'cloud_percent',
            'item_type', 'provider', 'published',
            'satellite_azimuth', 'satellite_id', 'view_angle',
            'pixel_resolution', 'withhold_readable', 'withhold_hours'
        ])

        

        # Write the data rows
        for feature in features:
            properties = feature.get('properties', {})
            geometry = feature.get('geometry', {})
            acquisition_date = format_datetime(properties.get('acquired', ''))
            publication_date = format_datetime(properties.get('published', ''))
            withhold_readable, withhold_hours = calculate_withhold_time(properties.get('acquired', ''),
                                                                        properties.get('published', ''))
            satellite_azimuth = format_float(properties.get('satellite_azimuth', ''), 2)
            view_angle = format_float(properties.get('view_angle', ''), 2)
            pixel_resolution = format_float(properties.get('pixel_resolution', ''), 2)

            csv_writer.writerow([
                feature.get('id', ''),
                json.dumps(geometry),  # Geometry as a JSON string
                acquisition_date,
                properties.get('cloud_percent', ''),
                properties.get('item_type', ''),
                properties.get('provider', ''),
                publication_date,
                satellite_azimuth,
                properties.get('satellite_id', ''),
                view_angle,
                pixel_resolution,
                withhold_readable,
                withhold_hours
            ])

            # Create a GeoJSON feature
            geojson_feature = {
                "type": "Feature",
                "geometry": geometry,
                "properties": {
                    "id": feature.get('id', ''),
                    "acquired": acquisition_date,
                    "cloud_percent": properties.get('cloud_percent', ''),
                    "item_type": properties.get('item_type', ''),
                    "provider": properties.get('provider', ''),
                    "published": publication_date,
                    "satellite_azimuth": satellite_azimuth,
                    "satellite_id": properties.get('satellite_id', ''),
                    "view_angle": view_angle,
                    "pixel_resolution": pixel_resolution,
                    "withhold_readable": withhold_readable,
                    "withhold_hours": withhold_hours
                }
            }
            geojson_features.append(geojson_feature)

    process_geojson(geojson_features)
    download_thumbnails(features)


# Main function to process all dates first and then save the files
def main(START_DATE, END_DATE, OUTPUT_DIR, BBOX):
    # seed_geohash = GEOHASH
    # child_length = int(GEOHASH_LENGTH) - 1
    # geohashes = generate_geohashes(seed_geohash, child_length)

    bboxes = [BBOX]

    current_date = datetime.strptime(START_DATE, '%Y-%m-%d')
    end_date = datetime.strptime(END_DATE, '%Y-%m-%d')

    global BATCH_SIZE
    date_difference = (end_date - current_date).days + 1
    if date_difference < BATCH_SIZE:
        BATCH_SIZE = date_difference
    duration = math.ceil(date_difference / BATCH_SIZE)

    all_features = []  # Collect all features for all dates
    print("-"*columns)
    description = f"Processing Planet Catalog \nDate Range: {current_date.date()} to {end_date.date()} \n lat: {LAT} and lon: {LON} Range:{RANGE} \nOutput Directory: {OUTPUT_DIR}"
    print(description)
    print("-"*columns)
    print("Batch Size: ", BATCH_SIZE, ", days: ", date_difference)
    print("Duration :", duration, "batch")
    # Iterate over each day in the date range
    with tqdm(total=duration, desc="", unit="batch") as pbar:

        while current_date <= end_date:
            start_time = current_date.strftime('%Y-%m-%dT00:00:00Z')
            end_time = (current_date + timedelta(days=BATCH_SIZE)).strftime('%Y-%m-%dT00:00:00Z')

            for bbox in bboxes:
                features = query_planet_data(bbox, start_time, end_time, ITEM_TYPE)
                all_features.extend(features['features'])
                while True:
                    try:
                        if features['features'] and features.get("_links", {}).get("_next"):
                            features = query_planet_paginated_data(features["_links"]["_next"])
                            all_features.extend(features['features'])
                        else:
                            break
                    except Exception as e:
                        break

            current_date += timedelta(days=BATCH_SIZE)

            pbar.refresh()
            pbar.update(1)

        pbar.clear()
    tqdm.write("Completed processing Planet data")

    # Save all collected features to files after processing all days
    save_features_to_files(all_features, OUTPUT_DIR)


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser(description='Plant Catelog API Executor')
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
    OUTPUT_DIR = args.output_dir + f"/planet/{START_DATE}_{END_DATE}"

    RANGE = int(args.range)
    LAT, LON = args.lat, args.long
    BBOX = latlon_to_geojson(LAT, LON, RANGE)
    print(f"Generated BBOX: {BBOX}")

    OUTPUT_THUMBNAILS_FOLDER = f"{OUTPUT_DIR}/thumbnails"
    os.makedirs(OUTPUT_THUMBNAILS_FOLDER, exist_ok=True)

    OUTPUT_GEOJSON_FOLDER = f"{OUTPUT_DIR}/geojsons"
    os.makedirs(OUTPUT_GEOJSON_FOLDER, exist_ok=True)

    OUTPUT_GEOTIFF_FOLDER = f"{OUTPUT_DIR}/geotiffs"
    os.makedirs(OUTPUT_GEOTIFF_FOLDER, exist_ok=True)

    OUTPUT_CSV_FILE = f"{OUTPUT_DIR}/output_planet.csv"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Check if the directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    main(
        START_DATE,
        END_DATE,
        OUTPUT_DIR,
        BBOX
    )

    check_csv_and_rename_output_dir(OUTPUT_DIR, START_DATE, END_DATE, args.output_dir, "planet")
