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
from core.serializers import (
    SatelliteCaptureCatalogSerializer,
    SatelliteDateRetrievalPipelineHistorySerializer,
)
from datetime import datetime
from django.contrib.gis.geos import Polygon
from concurrent.futures import ThreadPoolExecutor, as_completed
from core.utils import save_image_in_s3_and_get_url
from botocore.exceptions import NoCredentialsError
import numpy as np
from rasterio.transform import from_bounds
from rasterio.io import MemoryFile
from io import BytesIO
from PIL import Image
from bungalowbe.utils import get_utc_time, convert_iso_to_datetime
from django.db.utils import IntegrityError
from core.models import SatelliteCaptureCatalog, SatelliteDateRetrievalPipelineHistory, SatelliteCaptureCatalogMetadata
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


def process_database_catalog(features, start_time, end_time):
    valid_features = []
    invalid_features = []
    metadata = []

    for feature in features:
        serializer = SatelliteCaptureCatalogSerializer(data=feature)
        if serializer.is_valid():
            valid_features.append(serializer.validated_data)
            metadata.append({
                    "vendor_name": feature["vendor_name"],
                    "vendor_id": feature["vendor_id"],
                    "acquisition_datetime": feature["acquisition_datetime"],
                    "metadata": feature["metadata"],
                })
        else:
            invalid_features.append(feature)
    
    print(f"Total records: {len(features)}, Valid records: {len(valid_features)}, Invalid records: {len(invalid_features)}")
    if valid_features:
        try:
            SatelliteCaptureCatalog.objects.bulk_create(
                [SatelliteCaptureCatalog(**feature) for feature in valid_features]
            )
            SatelliteCaptureCatalogMetadata.objects.bulk_create(
                [SatelliteCaptureCatalogMetadata(**meta) for meta in metadata]
            )
        except IntegrityError as e:
            print(f"Error during bulk insert: {e}")

    if not valid_features:
        print(f"No records Found for {start_time} to {end_time}")
        return

    try:
        last_acquisition_datetime = valid_features[0]["acquisition_datetime"]
        last_acquisition_datetime = datetime.strftime(
            last_acquisition_datetime, "%Y-%m-%d %H:%M:%S%z"
        )
    except Exception as e:
        last_acquisition_datetime = end_time

    history_serializer = SatelliteDateRetrievalPipelineHistorySerializer(
        data={
            "start_datetime": convert_iso_to_datetime(start_time),
            "end_datetime": convert_iso_to_datetime(last_acquisition_datetime),
            "vendor_name": "planet",
            "message": {
                "total_records": len(features),
                "valid_records": len(valid_features),
                "invalid_records": len(invalid_features),
            },
        }
    )
    if history_serializer.is_valid():
        history_serializer.save()
    else:
        print(f"Error in history serializer: {history_serializer.errors}")

def process_features(features):
    response = []

    for feature in features:
        try:
            properties = feature.get('properties', {})
            geometry = feature.get('geometry', {})
            model_params = {
                "acquisition_datetime": datetime.fromisoformat(
                properties["acquired"].replace("Z", "+00:00")
                ),
                "cloud_cover": properties["cloud_cover"],
                "vendor_id": feature["id"],
                "vendor_name": "planet",
                "sensor": properties["item_type"],
                "area": calculate_area_from_geojson(geometry, feature["id"]),
                "type": (
                    "Day"
                    if 6
                    <= datetime.fromisoformat(
                        properties["acquired"].replace("Z", "+00:00")
                    ).hour
                    <= 18
                    else "Night"
                ),
                "sun_elevation": properties["sun_azimuth"],
                "resolution": f"{properties['gsd']}m",
                "location_polygon": geometry,
                "coordinates_record": geometry,
                "metadata": feature
            }
            response.append(model_params)
        except Exception as e:
            print(f"Error: {e}")
            continue
    return response

def main(START_DATE, END_DATE, BBOX):
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
    process_database_catalog(converted_features, START_DATE.isoformat(), END_DATE.isoformat())


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
    response = main(START_DATE, END_DATE, BBOX)
    return response


def run_planet_catalog_bulk_api():
    BBOX = "-180,-90,180,90"
    BBOX = bbox_to_geojson(BBOX)
    START_DATE = datetime(2024, 12, 6, tzinfo=pytz.utc)
    END_LIMIT = datetime(2024, 12, 14, tzinfo=pytz.utc)

    import time
    while START_DATE < END_LIMIT:
        END_DATE = min(START_DATE + timedelta(days=1), END_LIMIT)
        print(f"Start Date: {START_DATE}, End Date: {END_DATE}")
        month_start_time = time.time()
        
        response = main(START_DATE, END_DATE, BBOX)
        month_end_time = time.time()
        print(f"Time taken to process the interval: {month_end_time - month_start_time}")

        START_DATE = END_DATE
    return response

# from core.services.planet_catalog_api import run_planet_catalog_bulk_api
# run_planet_catalog_bulk_api()