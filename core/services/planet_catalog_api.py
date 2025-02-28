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
from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil
from PIL import Image, ImageChops
import numpy as np
import rasterio
from rasterio.transform import from_bounds
from decouple import config
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from core.utils import save_image_in_s3_and_get_url, process_database_catalog, get_holdback_seconds, get_centroid_and_region_and_location_polygon, get_centroid_region_and_local
from botocore.exceptions import NoCredentialsError
import numpy as np
from rasterio.transform import from_bounds
from rasterio.io import MemoryFile
from io import BytesIO
from PIL import Image
from bungalowbe.utils import get_utc_time, convert_iso_to_datetime
from core.models import SatelliteDateRetrievalPipelineHistory
import pytz
from core.services.utils import calculate_bbox_npolygons, calculate_area_from_geojson

# Get the terminal size
columns = shutil.get_terminal_size().columns

# Input configuration
API_KEY = config("PLANET_API_KEY") 
GEOHASH = 'w'              # Specify the initial geohash
GEOHASH_LENGTH = 2         # Specify the desired geohash length
ITEM_TYPE = "SkySatCollect"  # Specify the item type
BATCH_SIZE = 28
MAX_THREADS = 10




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

def get_polygon_bounding_box(polygon):
    """Extracts the bounding box from a polygon's coordinates."""
    min_lon = min([point[0] for point in polygon["coordinates"][0]])
    max_lon = max([point[0] for point in polygon["coordinates"][0]])
    min_lat = min([point[1] for point in polygon["coordinates"][0]])
    max_lat = max([point[1] for point in polygon["coordinates"][0]])

    return min_lon, min_lat, max_lon, max_lat


def geotiff_conversion_and_s3_upload(content, filename, tiff_folder, polygon=None):
    img = Image.open(BytesIO(content))
    img_array = np.array(img)

    # Define the transform based on polygon bounds
    if polygon:
        min_lon, min_lat, max_lon, max_lat = get_polygon_bounding_box(polygon)
        transform = from_bounds(
            min_lon, min_lat, max_lon, max_lat, img_array.shape[1], img_array.shape[0]
        )
    else:
        return False

    # Step 3: Convert to GeoTIFF and save to S3
    with MemoryFile() as memfile:
        with memfile.open(
            driver="GTiff",
            height=img_array.shape[0],
            width=img_array.shape[1],
            count=img_array.shape[2] if len(img_array.shape) == 3 else 1,
            dtype=img_array.dtype,
            crs="EPSG:4326",
            transform=transform,
        ) as dst:
            if len(img_array.shape) == 2:  # Grayscale
                dst.write(img_array, 1)
            else:  # RGB or multi-channel
                for i in range(img_array.shape[2]):
                    dst.write(img_array[:, :, i], i + 1)

        # Upload the GeoTIFF to S3
        geotiff_url = save_image_in_s3_and_get_url(
            memfile.read(), filename, tiff_folder, "tif"
        )
        return geotiff_url


def upload_to_s3(feature, folder="thumbnails"):
    """Downloads an image from the URL in the feature and uploads it to S3."""
    try:
        url = feature.get("_links", {}).get("thumbnail", {})
        feature['bbox'] = calculate_bbox_npolygons(feature.get('geometry', {}))
        headers = {
            "Content-Type": "application/json",
            "Authorization": "api-key " + API_KEY,
        }
        url = url + "?width=512"
        response = requests.get(url, headers=headers, stream=True, timeout=(10, 30))
        response.raise_for_status()
        filename = feature.get("id")
        content = response.content
        response_url = save_image_in_s3_and_get_url(content, filename, folder)
        # response_geotiff = geotiff_conversion_and_s3_upload(
        #     content, filename, "planet/geotiffs", feature.get("geometry")
        # )
        return response_url

    except requests.exceptions.RequestException as e:
        print(f"Failed to download {url}: {e}")
        return False
    except NoCredentialsError:
        print("S3 credentials not available")
        return False
    except Exception as e:
        print(f"Failed to upload {url}: {e}")
        return False


def download_and_upload_images(features, path, max_workers=5):
    """Download images from URLs in features and upload them to S3."""

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(upload_to_s3, feature, path): feature
            for feature in features
        }

        for future in as_completed(futures):
            feature = futures[future]
            try:
                result = future.result()
                if result:
                    pass
                else:
                    print(f"Failed to upload image for feature {feature.get('id')}")
            except Exception as e:
                print(f"Exception occurred for feature {feature.get('id')}: {e}")

def calculate_withhold(acquisition_datetime, publication_datetime):
    """Calculate the holdback period based on acquisition and publication dates."""
    try:
        holdback = (publication_datetime - acquisition_datetime).total_seconds()
        return holdback
    except Exception as e:
        print(f"Failed to calculate holdback hours: {e}")
        return -1

def process_single_feature(feature):
    """Process a single feature from the Planet API."""
    try:
        properties = feature.get("properties", {})
        geometry = feature.get("geometry", {})
        acquisition_datetime = datetime.fromisoformat(
            properties["acquired"].replace("Z", "+00:00")
        )
        publication_datetime = datetime.now(pytz.utc).replace(microsecond=0)
        centroid_dict = get_centroid_and_region_and_location_polygon(feature["geometry"])

        model_params = {
            "acquisition_datetime": acquisition_datetime,
            "cloud_cover_percent": properties["cloud_percent"],
            "vendor_id": feature["id"],
            "vendor_name": "planet",
            "sensor": properties["item_type"],
            "area": calculate_area_from_geojson(geometry, feature["id"]),
            "sun_elevation": properties["sun_azimuth"],
            "resolution": f"{properties['gsd']}m",
            "location_polygon": geometry,
            "coordinates_record": geometry,
            "metadata": feature,
            "gsd": float(properties["gsd"]),
            "constellation": properties["satellite_id"],
            "offnadir": None,
            "platform": properties["satellite_id"],
            "azimuth_angle": properties.get("satellite_azimuth"),
            "illumination_azimuth_angle": properties.get("sun_azimuth"),
            "illumination_elevation_angle": properties.get("sun_elevation"),
            "holdback_seconds": calculate_withhold(acquisition_datetime, publication_datetime),
            "publication_datetime": publication_datetime,
            **centroid_dict,
        }
        return model_params
    except Exception as e:
        print(f"Error: {e}")
        return None


def process_features(features):
    converted_features = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(process_single_feature, feature): feature
            for feature in features
        }

        for future in as_completed(futures):
            result = future.result()
            if result:
                model_params = result
                converted_features.append(model_params)
    converted_features = [feature for feature in converted_features if feature]
    converted_features = sorted(
        converted_features, key=lambda x: x["acquisition_datetime"], reverse=True
    )
    converted_features = get_centroid_region_and_local(converted_features)
    return converted_features[::-1]

def main(START_DATE, END_DATE, BBOX, is_bulk):
    bboxes = [BBOX]
    current_date = START_DATE
    end_date = END_DATE

    global BATCH_SIZE
    date_difference = (end_date - current_date).days + 1
    if date_difference < BATCH_SIZE:
        BATCH_SIZE = date_difference
    duration = math.ceil(date_difference / BATCH_SIZE)

    all_features = []  # Collect all features for all dates
    print("-"*columns)
    print("Batch Size: ", BATCH_SIZE, ", days: ", date_difference)
    print("Duration :", duration, "batch")
    while current_date <= end_date:
        start_time = current_date.isoformat()
        if (end_date - current_date).days > 1:
            end_time = (current_date + timedelta(days=BATCH_SIZE)).isoformat()
        else:
            end_time = end_date.isoformat()

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
    print(f"Total features: {len(all_features)}")
    converted_features = process_features(all_features)
    # download_and_upload_images(all_features, "planet/thumbnails")
    process_database_catalog(converted_features, START_DATE.isoformat(), END_DATE.isoformat(), "planet", is_bulk)


def bbox_to_geojson(bbox_str):
    min_lon, min_lat, max_lon, max_lat = map(float, bbox_str.split(","))
    coordinates = [
        [
            [min_lon, min_lat],  
            [min_lon, max_lat],  
            [max_lon, max_lat],  
            [max_lon, min_lat],  
            [min_lon, min_lat]   
        ]
    ]
    
    # Create the GeoJSON structure
    geojson = {
        "type": "Polygon",
        "coordinates": coordinates
    }
    return geojson

def run_planet_catalog_api():
    BBOX = "-180,-90,180,90"
    BBOX = bbox_to_geojson(BBOX)
    START_DATE = (
        SatelliteDateRetrievalPipelineHistory.objects.filter(vendor_name="planet")
        .order_by("-end_datetime")
        .first()
    )
    if not START_DATE:
        START_DATE = datetime(
            datetime.now().year,
            datetime.now().month,
            datetime.now().day,
            tzinfo=pytz.utc,
        )
    else:
        START_DATE = START_DATE.end_datetime
        print(f"From DB: {START_DATE}")

    END_DATE = get_utc_time()
    print(f"Start Date: {START_DATE}, End Date: {END_DATE}")
    response = main(START_DATE, END_DATE, BBOX, False)
    return response


def run_planet_catalog_bulk_api():
    BBOX = "-180,-90,180,90"
    BBOX = bbox_to_geojson(BBOX)
    START_DATE = datetime(2024, 1, 1, tzinfo=pytz.utc)
    END_LIMIT = datetime(2024, 1, 2, tzinfo=pytz.utc)

    import time
    while START_DATE < END_LIMIT:
        END_DATE = min(START_DATE + timedelta(days=1), END_LIMIT)
        print(f"Start Date: {START_DATE}, End Date: {END_DATE}")
        month_start_time = time.time()
        
        response = main(START_DATE, END_DATE, BBOX, True)
        month_end_time = time.time()
        print(f"Time taken to process the interval: {month_end_time - month_start_time}")

        START_DATE = END_DATE
    return response

def run_planet_catalog_bulk_api_for_last_35_days_from_now():
    BBOX = "-180,-90,180,90"
    BBOX = bbox_to_geojson(BBOX)
    START_DATE = (datetime.now(pytz.utc) - timedelta(days=35)).replace(hour=0, minute=0, second=0, microsecond=0)
    END_LIMIT = datetime.now(pytz.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    import time
    while START_DATE < END_LIMIT:
        END_DATE = min(START_DATE + timedelta(days=1), END_LIMIT)
        print(f"Start Date: {START_DATE}, End Date: {END_DATE}")
        month_start_time = time.time()
        
        response = main(START_DATE, END_DATE, BBOX, True)
        month_end_time = time.time()
        print(f"Time taken to process the interval: {month_end_time - month_start_time}")
        time.sleep(5)
        START_DATE = END_DATE
    return "Planet 35 days bulk processing completed"

# from core.services.planet_catalog_api import run_planet_catalog_bulk_api
# run_planet_catalog_bulk_api()

# from core.services.planet_catalog_api import run_planet_catalog_bulk_api_for_last_35_days_from_now
# run_planet_catalog_bulk_api_for_last_35_days_from_now()