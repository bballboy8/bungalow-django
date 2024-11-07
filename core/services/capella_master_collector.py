import os
import base64
import logging
import time
import requests
import rasterio
from rasterio.transform import from_bounds
import numpy as np
import pygeohash as pgh
from PIL import Image, ImageChops
import geojson
from shapely.geometry import shape, box, Polygon
from datetime import datetime, timedelta
import csv
import re
import argparse
from tqdm import tqdm
import geohash2
import shutil
import math
from pyproj import Geod
from utils import check_csv_and_rename_output_dir
from decouple import config

# Get the terminal size
columns = shutil.get_terminal_size().columns

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# base_output_folder = r'O:\Heimdall\Engagements_ajg\20240827_capella_collection_of_target_deck'

# Dates for search
# START_DATE = "2024-07-01"
# END_DATE = "2024-08-26"
# Number of days to process at a time
DAYS_PER_BATCH = 1

# Define the filter keyword (e.g., "GEO" or "GEC")
FILTER_KEYWORD = "GEC"

# Choose mode of operation: 'geohash', 'location', or 'geojson'
MODE = "location"

# Geohash and locations
geohash_level_1 = ["w7y8"]


LOCATIONS = [
    # {"name": "Shipyard - Huludao Bohai Shipyard", "lat": 40.71235, "lon": 121.019},
    # {"name": "Huludao Naval Base", "lat": 40.70926, "lon": 120.99756},
    # {"name": "Dalianwan Ferry Terminal", "lat": 39.02887, "lon": 121.73669},
    # {"name": "Shipyard - Dalian Dagushan", "lat": 38.965, "lon": 121.832},
    # {"name": "Dagushan Naval Base", "lat": 38.96081, "lon": 121.83053},
    # {"name": "Shipyard - Dalian Naval Shipyard", "lat": 38.9376, "lon": 121.64381},
    # {"name": "Shipyard - Dalian West", "lat": 38.93566, "lon": 121.61286},
    # {"name": "Dalian Ferry Terminal", "lat": 38.934, "lon": 121.658},
    # {"name": "Xiaopingdao Submarine Base", "lat": 38.82, "lon": 121.492},
    # {"name": "Lushunkou Ferry Terminal", "lat": 38.8162, "lon": 121.13087},
    # {"name": "Lushun Naval Base", "lat": 38.8032, "lon": 121.24285},
    # {"name": "Shipyard - Dalian Liaonan Shipyard", "lat": 38.80187, "lon": 121.26524},
    # {"name": "Lushun Submarine Base", "lat": 38.79002, "lon": 121.24029},
    # {"name": "Penglai Ferry Terminal", "lat": 37.82986, "lon": 120.73988},
    # {"name": "Longkou Ferry Terminal", "lat": 37.65096, "lon": 120.31985},
    # {"name": "Shipyard - Yantai Raffles Shipyard", "lat": 37.59572, "lon": 121.39167},
    # {"name": "Yantai Harbor Ferry Terminals", "lat": 37.59295, "lon": 121.38762},
    # {"name": "Yantai Ferry Terminals South", "lat": 37.55573, "lon": 121.37468},
    # {"name": "Weihai Ferry Terminal", "lat": 37.43401, "lon": 122.18463},
    # {"name": "Jianggezhuang Submarine Base", "lat": 36.112, "lon": 120.576},
    # {"name": "Qingdao Jiaozhou Bay Base", "lat": 36.094, "lon": 120.312},
    # {"name": "Xiaowan Houbei Base", "lat": 35.9135, "lon": 120.15507},
    # {"name": "Yuchi Naval Base (CV base)", "lat": 35.7277053, "lon": 119.9868509},
    # {"name": "Lianyungang Naval Base", "lat": 34.754352, "lon": 119.455865},
    # {"name": "Wanfu Naval Base (LSTs, 056s)", "lat": 32.24356, "lon": 119.68885},
    # {"name": "Huangshan Base (Space Support Ships)", "lat": 31.94404, "lon": 120.28903},
    # {"name": "Shanghai Naval Base (Wusong)", "lat": 31.384999, "lon": 121.501287},
    # {"name": "Shanghai Naval Base (Qiujiang)", "lat": 31.30828, "lon": 121.55447},
    # {"name": "Shipyard - Jiangnan Shipyard  (Changxing Island)", "lat": 31.341089, "lon": 121.750677},
    # {"name": "Shipyard - Hudong Zhonghua  (Changxing Island)", "lat": 31.32548, "lon": 121.76279},
    # {"name": "Shanghai Naval Base (Pudong, 5th Flotilla)", "lat": 31.306539, "lon": 121.701668},
    # {"name": "Shipyard - Wuchang Shipbuilding Industry Group", "lat": 30.58615, "lon": 114.679},
    # {"name": "Chang Tu Naval Base (East)", "lat": 30.25229, "lon": 122.30798},
    # {"name": "Chang Tu Naval Base (West)", "lat": 30.25104, "lon": 122.28824},
    # {"name": "Zhoushan Island Naval Base (West, Surf Combatants)", "lat": 30.00888, "lon": 122.01653},
    # {"name": "Zhoushan Island Naval Base (Center, Dinghai)", "lat": 30.00883, "lon": 122.06291},
    # {"name": "Zhoushan Island Naval Base (East, Dinghai, Auxiliaries)", "lat": 29.99933, "lon": 122.1114},
    # {"name": "Pu Tuo Shan Naval Base (Houbeis, 056s)", "lat": 29.97998, "lon": 122.36896},
    # {"name": "Daxie Dao Submarine Base", "lat": 29.898, "lon": 121.968},
    # {"name": "Shiyan Naval Base (Rescue Ships)", "lat": 29.55145, "lon": 121.66031},
    # {"name": "Xiangshan Submarine Base", "lat": 29.537, "lon": 121.77},
    # {"name": "Tiawangyu Naval Base (Houbeis)", "lat": 29.17715, "lon": 121.95053},
    # {"name": "Yueqing Bay Naval Base (LSTs, MCM)", "lat": 28.0656, "lon": 121.14318},
    # {"name": "Wenzhou Naval Base (v. small, no units)", "lat": 27.97204, "lon": 120.77371},
    # {"name": "Luochun Naval Base (Houbeis)", "lat": 27.23399, "lon": 120.38038},
    # {"name": "Jiaotou Naval Base", "lat": 26.615, "lon": 119.680278},
    # {"name": "Xiamen Zhonghua Naval Base", "lat": 24.44981, "lon": 118.07445},
    # {"name": "Xiamen Zhaiqian Naval Base", "lat": 24.44923, "lon": 118.04069},
    # {"name": "Shantou Naval Base (East)", "lat": 23.34512, "lon": 116.678},
    # {"name": "Shantou Naval Base (West, Houbeis)", "lat": 23.3391, "lon": 116.65376},
    # {"name": "Haizhu Naval Base (AGS)", "lat": 23.08959, "lon": 113.41023},
    # {"name": "Shipyard - Huangpu Shipyard International", "lat": 23.08344, "lon": 113.4059},
    # {"name": "Huangpu Wenchong Shipyard", "lat": 23.0886, "lon": 113.4725},
    # {"name": "Shipyard - Jiang Tongfang New Shipbuilding USV Shipyard", "lat": 29.76576, "lon": 116.24326},
    # {"name": "Shanwei Naval Base [NOTE: nothing of note seen]", "lat": 22.780828, "lon": 115.341454},
    # {"name": "Ngong Shuen Chau (HK) Naval Base", "lat": 22.322222, "lon": 114.136111},
    # {"name": "Jiangmen Naval Base (Houbeis, 056, MCM)", "lat": 22.2836, "lon": 113.07507},
    # {"name": "Xiachuan Dao Submarine Base", "lat": 21.596, "lon": 112.55},
    # {"name": "Behai Naval Base", "lat": 21.4858, "lon": 109.074338},
    # {"name": "Nanpo Logistics Base", "lat": 21.33147, "lon": 110.41171},
    # {"name": "Zhanjiang Naval Base (West)", "lat": 21.23769, "lon": 110.41965},
    # {"name": "Zhanjiang Naval 0Base (East, Amphib Base)", "lat": 21.22688, "lon": 110.43888},
    # {"name": "Xuwen Ferry Terminal (Hainan Strait)", "lat": 20.23548, "lon": 110.1366},
    # {"name": "Xinhai Ferry Terminal (Hainan Strait)", "lat": 20.05593, "lon": 110.15151},
    # {"name": "Haikou Coast Guard Base", "lat": 20.031537, "lon": 110.278354},
    # {"name": "Yulin Naval Base", "lat": 18.220541, "lon": 109.545956},
    # {"name": "Yalong Carrier Base (Yulin East)", "lat": 18.23193, "lon": 109.68058},
    # {"name": "Yalong Submarine Base (Yulin East)", "lat": 18.214, "lon": 109.697},
    # {"name": "Woody Island", "lat": 16.83438, "lon": 112.3412},
    # {"name": "Lincoln Island Garrison [Nothing seen]", "lat": 16.667685, "lon": 112.729363},
    # {"name": "Triton Island Garrison", "lat": 15.784741, "lon": 111.20221},
    # {"name": "Djibouti Naval Base", "lat": 11.59081, "lon": 43.06359},
    # {"name": "Subi Reef Naval Base", "lat": 10.92492, "lon": 114.0839},
    # {"name": "Cambodia Ream Naval Base", "lat": 10.50633, "lon": 103.61229},
    # {"name": "Mischief Reef Naval Base", "lat": 9.90389, "lon": 115.53561},
    {"name": "capella"}
]



# API and Authentication setup
API_URL = "https://api.capellaspace.com/catalog/search"
TOKEN_URL = "https://api.capellaspace.com/token"
USERNAME = config("CAPELLA_API_USERNAME")
PASSWORD = config("CAPELLA_API_PASSWORD")


BBOX_SIZE = 0.0045  # Approx. 500m in degrees latitude/longitude
TARGET_RESOLUTION = (1500, 1500)  # Desired resolution
RETRY_LIMIT = 5  # Number of retries before failing

def latlon_to_geohash(lat, lon, range_km):
    # Map the range to geohash precision
    precision = (
        2 if range_km > 100 else
        4 if range_km > 20 else
        6 if range_km > 5 else
        8 if range_km > 1 else
        10
    )
    return geohash2.encode(lat, lon, precision=precision)

def get_access_token(username, password):
    credentials = f"{username}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {encoded_credentials}"
    }

    try:
        response = requests.post(TOKEN_URL, headers=headers)
        response.raise_for_status()

        token_info = response.json()
        return token_info

    except requests.RequestException as e:
        # logging.error(f"Failed to retrieve token: {e}")
        return None

def sanitize_filename(name):
    """Sanitize the location name to remove or replace invalid characters for file names."""
    return re.sub(r'[<>:"/\\|?*]', '_', name)

def create_bbox(lat, lon, bbox_size):
    """Create a bounding box around the given latitude and longitude."""
    lat_min = lat - bbox_size
    lat_max = lat + bbox_size
    lon_min = lon - bbox_size
    lon_max = lon + bbox_size
    return [lon_min, lat_min, lon_max, lat_max]

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

def geohash_to_polygon(geohash):
    corners = get_geohash_corners(geohash)
    return Polygon([
        (corners["top_left"][1], corners["top_left"][0]),
        (corners["top_right"][1], corners["top_right"][0]),
        (corners["bottom_right"][1], corners["bottom_right"][0]),
        (corners["bottom_left"][1], corners["bottom_left"][0]),
        (corners["top_left"][1], corners["top_left"][0])
    ])

def load_geojson_file(geojson_path):
    """Load and return the polygon geometry from a GeoJSON file."""
    with open(geojson_path, 'r') as file:
        geojson_data = geojson.load(file)
        return shape(geojson_data['features'][0]['geometry'])

def remove_black_borders(img):
    """Remove black borders from the image."""
    bg = Image.new(img.mode, img.size, img.getpixel((0, 0)))
    diff = ImageChops.difference(img, bg)
    bbox = diff.getbbox()
    if bbox:
        return img.crop(bbox)
    return img

def georectify_image(png_path, bbox, geotiffs_folder, geojsons_folder, image_prefix, image_id, date, target_resolution):
    """Georectify the PNG image, crop padding, upscale to target resolution, save as a GeoTIFF, and output GeoJSON."""
    try:
        with Image.open(png_path) as img:
            img = remove_black_borders(img)
            img = img.resize(target_resolution, Image.Resampling.LANCZOS)
            img_array = np.array(img)

            width, height = target_resolution
            left, bottom, right, top = bbox

            transform = from_bounds(left, bottom, right, top, width, height)

            geotiff_name = f"{image_prefix}_{date}_{image_id[-8:]}.tif"
            geotiff_path = os.path.join(geotiffs_folder, geotiff_name)

            if len(img_array.shape) == 2:
                img_array = np.expand_dims(img_array, axis=-1)
                count = 1
            else:
                count = img_array.shape[2]

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

            polygon = box(left, bottom, right, top)
            geojson_data = geojson.FeatureCollection([geojson.Feature(geometry=polygon, properties={})])
            geojson_name = f"{image_prefix}_{date}_{image_id[-8:]}.geojson"
            geojson_path = os.path.join(geojsons_folder, geojson_name)

            with open(geojson_path, 'w') as geojson_file:
                geojson.dump(geojson_data, geojson_file)

    except Exception as e:
        # logging.error(f"Failed to georectify image {png_path}: {e}")
        pass

def save_image(url, save_path):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(save_path, 'wb') as out_file:
            out_file.write(response.content)
        return True
    except Exception as e:
        # logging.error(f"Failed to download image from {url}: {e}")
        return False

def download_thumbnail(feature, output_folder, image_prefix, date, filter_keyword):
    """Download and save the thumbnail image if it contains the filter keyword."""
    image_id = feature['id']
    bbox = feature['bbox']

    assets = feature.get('assets', {})
    thumbnail_url = assets.get('thumbnail', {}).get('href', None)

    if not thumbnail_url:
        return False

    if filter_keyword in image_id:
        # logging.info(f"Found thumbnail for image ID {image_id} on {date}")

        if save_image(thumbnail_url, os.path.join(output_folder, f"{image_prefix}_{date}_{image_id[-8:]}.png")):
            georectify_image(
                os.path.join(output_folder, f"{image_prefix}_{date}_{image_id[-8:]}.png"),
                bbox, os.path.join(output_folder, "geotiffs"), os.path.join(output_folder, "geojsons"),
                image_prefix, image_id, date, TARGET_RESOLUTION
            )
            # logging.info(f"Successfully downloaded and processed thumbnail for image ID {image_id} on {date}")
            return True
        else:
            # logging.error(f"Failed to download thumbnail for image ID {image_id} on {date}")
            pass
    return False

def query_api_with_retries(access_token, bbox, start_datetime, end_datetime):
    """Query the API with retries and token refresh handling."""
    bbox = list(map(float, bbox.split(',')))
    retry_count = 0
    while retry_count < RETRY_LIMIT:
        try:
            request_body = {
                "bbox": bbox,
                "datetime": f"{start_datetime}/{end_datetime}",
                "collections": ["capella-geo"],
                "limit": 100
            }

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"
            }

            response = requests.post(API_URL, json=request_body, headers=headers)
            response.raise_for_status()

            return response.json()

        except requests.RequestException as e:
            logging.error(f"API request failed: {e}")
            retry_count += 1

            if retry_count >= RETRY_LIMIT:
                logging.error(f"Max retries reached. Exiting after {retry_count} attempts.")
                break

            # Attempt to get a new token and retry
            # logging.info("Attempting to get a new access token...")
            token_info = get_access_token(USERNAME, PASSWORD)
            if token_info:
                access_token = token_info["accessToken"]
                logging.info(f"New token acquired. Retrying... ({retry_count}/{RETRY_LIMIT})")
            else:
                logging.error("Failed to obtain new access token. Pausing for 10 minutes...")
                time.sleep(600)  # Sleep for 10 minutes before retrying

            time.sleep(300)

    return None

def process_features(api_result, writer, thumbnails_folder, geotiffs_folder, geojsons_folder, date):
    if not api_result or 'features' not in api_result or not api_result['features']:
        # logging.warning("No data to write to CSV. Skipping...")
        return

    identified_count = len(api_result['features'])
    success_count = 0
    failure_count = 0

    for feature in api_result['features']:
        # Extract necessary fields
        feature_id = feature['id']
        bbox = feature['bbox']
        instruments = feature['properties'].get('instruments', ['N/A'])[0]
        datetime_str = feature['properties']['datetime']

        # Adjusted datetime parsing to handle fractional seconds
        datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        formatted_datetime = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

        sar_instrument_mode = feature['properties'].get('sar:instrument_mode', 'N/A')
        sar_pixel_spacing_range = feature['properties'].get('sar:pixel_spacing_range', 'N/A')
        image_length = feature['properties'].get('capella:image_length', 'N/A')
        image_width = feature['properties'].get('capella:image_width', 'N/A')

        thumbnail_url = feature['assets']['thumbnail']['href']

        # Save thumbnail image with a global sequence number
        image_name_base = f"{datetime_obj.strftime('%Y-%m-%d')}_{feature_id}_thumbnail"
        thumbnail_image_path = os.path.join(thumbnails_folder, f"{image_name_base}.png")

        # Try to save the thumbnail image
        if save_image(thumbnail_url, thumbnail_image_path):
            # Georectify the thumbnail image and save as GeoTIFF
            georectify_image(thumbnail_image_path, bbox, geotiffs_folder, geojsons_folder, image_name_base, feature_id,
                             datetime_obj.strftime('%Y-%m-%d'), TARGET_RESOLUTION)
            success_count += 1
        else:
            # logging.error(f"Failed to process thumbnail for feature ID {feature_id}")
            failure_count += 1

        # Write data to CSV
        writer.writerow({
            'id': feature_id,
            'bbox': bbox,
            'instruments': instruments,
            'datetime': formatted_datetime,
            'sar:instrument_mode': sar_instrument_mode,
            'sar:pixel_spacing_range': sar_pixel_spacing_range,
            'capella:image_length': image_length,
            'capella:image_width': image_width,
            'thumbnail_url': thumbnail_url
        })

    # logging.info(f"Date {date}: Identified {identified_count} thumbnails, "
    #              f"Successfully downloaded {success_count}, "
    #              f"Failed {failure_count}")

def search_images(lat, lon, bbox_size, start_date, end_date, access_token, output_folder, image_prefix, filter_keyword, csv_file_path):
    # Use the passed-in output_folder for location-specific output
    thumbnails_folder = os.path.join(output_folder, "thumbnails")
    geotiffs_folder = os.path.join(output_folder, "geotiffs")
    geojsons_folder = os.path.join(output_folder, "geojsons")

    bbox = BBOX

    current_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')

    duration =  ((end_date_dt - current_date).days + 1) / DAYS_PER_BATCH
    if duration < 1:
        duration = 1

    with tqdm(total=duration, desc="", unit="date") as pbar:

        while current_date <= end_date_dt:
            batch_end_date = min(current_date + timedelta(days=DAYS_PER_BATCH - 1), end_date_dt)
            # logging.info(f"Scanning from {current_date.strftime('%Y-%m-%d')} to {batch_end_date.strftime('%Y-%m-%d')}")  # Log in YYYY-MM-DD format

            result = query_api_with_retries(access_token, bbox, current_date.strftime('%Y-%m-%dT00:00:00Z'),
                                            batch_end_date.strftime('%Y-%m-%dT23:59:59Z'))
            if result:
                with open(csv_file_path, 'a', newline='') as csvfile:
                    fieldnames = ['id', 'bbox', 'instruments', 'datetime', 'sar:instrument_mode',
                                'sar:pixel_spacing_range', 'capella:image_length', 'capella:image_width',
                                'thumbnail_url']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                    if csvfile.tell() == 0:  # Write header if the file is empty
                        writer.writeheader()

                    for date in [current_date + timedelta(days=i) for i in range((batch_end_date - current_date).days + 1)]:
                        process_features(result, writer, thumbnails_folder, geotiffs_folder, geojsons_folder, date.strftime('%Y-%m-%d'))

            current_date = batch_end_date + timedelta(days=1)

            pbar.update(1)
            pbar.refresh()
            
    tqdm.write(f"Completed Processing Capella")


def process_locations(locations, start_date, end_date, access_token, output_folder, filter_keyword, lat, lon, bbox_range):

        print("-"*columns)
        description = f"Processing Capella Locations:\nDate Range {start_date} to {end_date} \n lat: {lat} and lon: {lon} \n Range:{BBOX_RANGE} \nOutput Directory: {output_folder}"
        print(description)
        print("-"*columns)
        print("Total locations to process: ", len(LOCATIONS))
        print(f"Processing in Batches of {DAYS_PER_BATCH} days")

        # current_date = datetime.strptime(start_date, '%Y-%m-%d')
        # end_date = datetime.strptime(end_date, '%Y-%m-%d')

        for location in locations:
            location_output_folder = os.path.join(output_folder)
            sanitized_name = "capella"
            # Create location-specific folders for thumbnails, geotiffs, geojsons, and CSV
            thumbnails_folder = os.path.join(location_output_folder, "thumbnails")
            geotiffs_folder = os.path.join(location_output_folder, "geotiffs")
            geojsons_folder = os.path.join(location_output_folder, "geojsons")
            csv_file_path = os.path.join(location_output_folder, f"output_{sanitized_name}.csv")

            # Ensure the directories exist
            os.makedirs(thumbnails_folder, exist_ok=True)
            os.makedirs(geotiffs_folder, exist_ok=True)
            os.makedirs(geojsons_folder, exist_ok=True)

            search_images(lat, lon, bbox_range, start_date, end_date, access_token,
                        location_output_folder, sanitized_name, filter_keyword, csv_file_path)
            
        check_csv_and_rename_output_dir(
            base_output_folder,
            start_date,
            end_date,
            OUTPUT_DIR,
            "capella"
        )


def process_geojson_files(geojson_folder, start_date, end_date, access_token, output_folder, filter_keyword):
    geojson_files = [f for f in os.listdir(geojson_folder) if f.endswith('.geojson')]

    for geojson_file in geojson_files:
        geojson_path = os.path.join(geojson_folder, geojson_file)
        polygon = load_geojson_file(geojson_path)
        bbox = polygon.bounds  # Get bounding box of the GeoJSON geometry
        bbox_list = [bbox[0], bbox[1], bbox[2], bbox[3]]  # Convert to list [min_lon, min_lat, max_lon, max_lat]
        sanitized_name = sanitize_filename(os.path.splitext(geojson_file)[0])

        # logging.info(f"Processing GeoJSON file: {geojson_file}")

        # Create location-specific folders for thumbnails, geotiffs, geojsons, and CSV
        output_folder_for_geojson = os.path.join(output_folder, sanitized_name)
        thumbnails_folder = os.path.join(output_folder_for_geojson, "thumbnails")
        geotiffs_folder = os.path.join(output_folder_for_geojson, "geotiffs")
        geojsons_folder = os.path.join(output_folder_for_geojson, "geojsons")
        csv_file_path = os.path.join(output_folder_for_geojson, f"output_{sanitized_name}.csv")

        # Ensure the directories exist
        os.makedirs(thumbnails_folder, exist_ok=True)
        os.makedirs(geotiffs_folder, exist_ok=True)
        os.makedirs(geojsons_folder, exist_ok=True)

        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')

        duration = (end_date_dt - current_date).days + 1
        print("-"*columns)
        description = f"Processing Capella Catalog GeoJSON:\n Date Range {start_date} to {end_date} \nOutput Directory: {output_folder_for_geojson}"
        print(description)
        print("-"*columns)
        with tqdm(total=duration, desc="", unit="day") as pbar:

            while current_date <= end_date_dt:
                batch_end_date = min(current_date + timedelta(days=DAYS_PER_BATCH - 1), end_date_dt)
                # logging.info(f"Scanning from {current_date.strftime('%Y-%m-%d')} to {batch_end_date.strftime('%Y-%m-%d')}")

                result = query_api_with_retries(access_token, bbox_list, current_date.strftime('%Y-%m-%dT00:00:00Z'),
                                                batch_end_date.strftime('%Y-%m-%dT23:59:59Z'))

                if result:
                    with open(csv_file_path, 'a', newline='') as csvfile:
                        fieldnames = ['id', 'bbox', 'instruments', 'datetime', 'sar:instrument_mode',
                                    'sar:pixel_spacing_range', 'capella:image_length', 'capella:image_width',
                                    'thumbnail_url']
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                        if csvfile.tell() == 0:  # Write header if the file is empty
                            writer.writeheader()

                        for date in [current_date + timedelta(days=i) for i in
                                    range((batch_end_date - current_date).days + 1)]:
                            process_features(result, writer, thumbnails_folder, geotiffs_folder, geojsons_folder,
                                            date.strftime('%Y-%m-%d'))

                current_date = batch_end_date + timedelta(days=1)
                pbar.update(1)
                pbar.refresh()

            
            tqdm.write(f"Completed Processing Capella GeoJson Files")


def geo_hash_handler(
        base_output_folder,
        start_date,
        end_date,
        BBOX,
        GEOHASH
):  
    for bbox in [BBOX]:
        sanitized_name = sanitize_filename(GEOHASH)
        location_output_folder = os.path.join(base_output_folder, sanitized_name)

        # Create location-specific folders for thumbnails, geotiffs, geojsons, and CSV
        thumbnails_folder = os.path.join(location_output_folder, "thumbnails")
        geotiffs_folder = os.path.join(location_output_folder, "geotiffs")
        geojsons_folder = os.path.join(location_output_folder, "geojsons")
        csv_file_path = os.path.join(location_output_folder, f"output_{sanitized_name}.csv")

        # Ensure the directories exist
        os.makedirs(thumbnails_folder, exist_ok=True)
        os.makedirs(geotiffs_folder, exist_ok=True)
        os.makedirs(geojsons_folder, exist_ok=True)


        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')

        duration = (end_date_dt - current_date).days + 1

        print("-"*columns)

        description = f"Processing Capella Geo Hashes\nDate Range: {start_date} to {end_date}\nOutput Directory: {base_output_folder}"
        print(description)

        print("-"*columns)

        with tqdm(total=duration, desc="", unit="day") as pbar:

            while current_date <= end_date_dt:
                batch_end_date = min(current_date + timedelta(days=DAYS_PER_BATCH - 1), end_date_dt)
                # logging.info(f"Scanning from {current_date.strftime('%Y-%m-%d')} to {batch_end_date.strftime('%Y-%m-%d')}")

                result = query_api_with_retries(
                    access_token, bbox,
                    current_date.strftime('%Y-%m-%dT00:00:00Z'),
                    batch_end_date.strftime('%Y-%m-%dT23:59:59Z')
                )

                if result:
                    with open(csv_file_path, 'a', newline='') as csvfile:
                        fieldnames = ['id', 'bbox', 'instruments', 'datetime', 'sar:instrument_mode',
                                        'sar:pixel_spacing_range', 'capella:image_length', 'capella:image_width',
                                        'thumbnail_url']
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                        if csvfile.tell() == 0:  # Write header if the file is empty
                            writer.writeheader()

                        for date in [current_date + timedelta(days=i) for i in range((batch_end_date - current_date).days + 1)]:
                            process_features(result, writer, thumbnails_folder, geotiffs_folder, geojsons_folder, date.strftime('%Y-%m-%d'))

                current_date = batch_end_date + timedelta(days=1)

                pbar.update(1)
                pbar.refresh()

        tqdm.write(f"Completed Processing Capella Geo Hashes")


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser(description='Capella Catalog API Executor')
    argument_parser.add_argument('--start-date', required=True, help='Start date')
    argument_parser.add_argument('--end-date', required=True, help='End date')
    argument_parser.add_argument('--lat', required=True, type=float, help='Latitude')
    argument_parser.add_argument('--long', required=True, type=float, help='Longitude')
    argument_parser.add_argument('--range', required=True, type=float, help='Range value')
    argument_parser.add_argument('--output-dir', required=True, help='Output directory')
    argument_parser.add_argument('--bbox', required=True, help='Bounding box')

    args = argument_parser.parse_args()
    START_DATE = args.start_date
    END_DATE = args.end_date
    LAT = args.lat
    LON = args.long
    BBOX_RANGE = args.range
    OUTPUT_DIR = args.output_dir
    base_output_folder = args.output_dir + f"/capella/{START_DATE}_{END_DATE}"

    GEOJSON_FOLDER = f"{base_output_folder}/geojsons"

    BBOX = args.bbox.replace("t", "-")
    GEOHASH = latlon_to_geohash(LAT, LON, range_km=BBOX_RANGE)
    print(f"Generated BBOX: {BBOX}")

    token_info = get_access_token(USERNAME, PASSWORD)
    if token_info:
        access_token = token_info["accessToken"]

        if MODE == "location":
            # Process locations
            process_locations(LOCATIONS, START_DATE, END_DATE, access_token, base_output_folder, FILTER_KEYWORD, LAT, LON,BBOX_RANGE )
        elif MODE == "geohash":
            # Process geohashes
            geo_hash_handler(
                base_output_folder,
                START_DATE,
                END_DATE,
                BBOX,
                GEOHASH
            )
            
        elif MODE == "geojson":
            # Process GeoJSON files
            process_geojson_files(GEOJSON_FOLDER, START_DATE, END_DATE, access_token, base_output_folder, FILTER_KEYWORD)

        else:
            # logging.error("Invalid MODE. Please set MODE to 'location', 'geohash', or 'geojson'.")
            pass

    else:
        # logging.error("Failed to authenticate. Exiting.")
        pass
