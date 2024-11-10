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
from core.models import SatelliteCaptureCatalog, SatelliteDateRetrievalPipelineHistory
import pytz

# Get the terminal size
columns = shutil.get_terminal_size().columns

BATCH_SIZE = 28

# API and Authentication setup
API_URL = "https://api.capellaspace.com/catalog/search"
TOKEN_URL = "https://api.capellaspace.com/token"
USERNAME = config("CAPELLA_API_USERNAME")
PASSWORD = config("CAPELLA_API_PASSWORD")


TARGET_RESOLUTION = (1500, 1500)  # Desired resolution
RETRY_LIMIT = 5  # Number of retries before failing


def get_access_token(username, password):
    credentials = f"{username}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {encoded_credentials}",
    }

    try:
        response = requests.post(TOKEN_URL, headers=headers)
        response.raise_for_status()

        token_info = response.json()
        return token_info

    except requests.RequestException as e:
        # logging.error(f"Failed to retrieve token: {e}")
        return None



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
        url = feature.get("thumbnail_url")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        filename = feature.get("id")
        content = response.content
        response_url = save_image_in_s3_and_get_url(content, filename, folder)
        response_geotiff = geotiff_conversion_and_s3_upload(
            content, filename, "geotiffs", feature.get("geometry")
        )
        return True

    except requests.exceptions.RequestException as e:
        print(f"Failed to download {url}: {e}")
        return False
    except NoCredentialsError:
        print("S3 credentials not available")
        return False
    except Exception as e:
        print(f"Failed to upload {url}: {e}")
        return False


def download_and_upload_images(features, path, max_workers=20):
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

def query_api_with_retries(access_token, bbox, start_datetime, end_datetime):
    """Query the API with retries and token refresh handling."""
    bbox = list(map(float, bbox.split(",")))
    retry_count = 0
    all_features = []
    next_url = API_URL
    page = 1
    try:
        while True:
            request_body = {
                "bbox": bbox,
                "datetime": f"{start_datetime}/{end_datetime}",
                "limit": 100,
                "page": page,
                "fields": {
                    "include": [
                    "id",
                    "properties.datetime",
                    "properties.view:incidence_angle",
                    "properties.instruments",
                    "properties.capella:resolution_ground_range",
                    "properties.eo:cloud_cover"
                    "geometry",
                    "assets:thumbnail",
                ]
                },
            }
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }
            response = requests.post(next_url, json=request_body, headers=headers)
            response.raise_for_status()
            response_json = response.json()
            if response_json.get("features"):
                all_features += response_json.get("features")
            if response_json.get("links") and response_json["links"][0]['rel'] == "next":
                page += 1
            else:
                break
        return all_features
    except requests.RequestException as e:
        logging.error(f"API request failed: {e}")

        token_info = get_access_token(USERNAME, PASSWORD)
        if token_info:
            access_token = token_info["accessToken"]
            logging.info(
                f"New token acquired. Retrying... ({retry_count}/{RETRY_LIMIT})"
            )
        else:
            logging.error(
                "Failed to obtain new access token. Pausing for 10 minutes..."
            )
            time.sleep(600)  # Sleep for 10 minutes before retrying

        time.sleep(300)
    except Exception as e:
        import traceback
        traceback.print_exc()



def process_features(features):
    response = []
    for feature in features:
        try:
            feature_id = feature["id"]
            datetime_str = feature["properties"]["datetime"]
            thumbnail_url = feature["assets"]["thumbnail"]["href"]
            location_polygon = Polygon(feature["geometry"]["coordinates"][0], srid=4326)
            model_params = {
                "acquisition_datetime": datetime.fromisoformat(
                    datetime_str.replace("Z", "+00:00")
                ),
                "vendor_id": feature_id,
                "vendor_name": "capella",
                "sensor": feature["properties"]["instruments"][0] if feature["properties"]["instruments"] else "",
                "area": location_polygon.area,
                "type": (
                    "Day"
                    if 6
                    <= datetime.fromisoformat(
                        datetime_str.replace("Z", "+00:00")
                    ).hour
                    <= 18
                    else "Night"
                ),
                "sun_elevation": feature["properties"]["view:incidence_angle"],
                "resolution": f"{feature['properties']['capella:resolution_ground_range']}m",
                "location_polygon": feature["geometry"],
                "coordinates_record": feature["geometry"],
                "thumbnail_url": thumbnail_url,
                "geometry": feature["geometry"],
            }
            response.append(model_params)

        except Exception as e:
            logging.error(f"Error processing feature: {e}")
            continue
    return response


def process_database_catalog(features, start_time, end_time):
    valid_features = []
    invalid_features = []

    for feature in features:
        serializer = SatelliteCaptureCatalogSerializer(data=feature)
        if serializer.is_valid():
            valid_features.append(serializer.validated_data)
        else:
            invalid_features.append(feature)
    
    print(f"Total records: {len(features)}, Valid records: {len(valid_features)}, Invalid records: {len(invalid_features)}")
    if valid_features:
        try:
            SatelliteCaptureCatalog.objects.bulk_create(
                [SatelliteCaptureCatalog(**feature) for feature in valid_features]
            )
        except IntegrityError as e:
            print(f"Error during bulk insert: {e}")

    if not valid_features:
        print(f"No records Found for {start_time} to {end_time}")
        return

    try:
        last_acquisition_datetime = valid_features[-1]["acquisition_datetime"]
        last_acquisition_datetime = datetime.strftime(
            last_acquisition_datetime, "%Y-%m-%d %H:%M:%S%z"
        )
    except Exception as e:
        last_acquisition_datetime = end_time

    history_serializer = SatelliteDateRetrievalPipelineHistorySerializer(
        data={
            "start_datetime": convert_iso_to_datetime(start_time),
            "end_datetime": convert_iso_to_datetime(last_acquisition_datetime),
            "vendor_name": "capella",
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


def search_images(start_date, end_date, bbox):
    bboxes = [bbox]
    access_token = get_access_token(USERNAME, PASSWORD)
    token_info = get_access_token(USERNAME, PASSWORD)
    if token_info:
        access_token = token_info["accessToken"]
    else:
        logging.error("Failed to obtain access token. Exiting...")
        return
    current_date = start_date
    global BATCH_SIZE
    date_difference = (end_date - current_date).days + 1
    if date_difference < BATCH_SIZE:
        BATCH_SIZE = date_difference
    duration = math.ceil(date_difference / BATCH_SIZE)
    print("-" * columns)
    print("Batch Size: ", BATCH_SIZE, ", days: ", date_difference)
    print("Duration :", duration, "batch")
    total_records = []
    while current_date <= end_date:
        start_time = current_date.isoformat()
        if (end_date - current_date).days > 1:
            end_time = (current_date + timedelta(days=BATCH_SIZE)).isoformat()
        else:
            end_time = end_date.isoformat()

        for bbox in bboxes:
            response = query_api_with_retries(
                access_token,
                bbox,
                start_time,
                end_time,
            )
            if response:
                total_records += response

        current_date += timedelta(days=BATCH_SIZE)

    print("Total Records: ", len(total_records))
    converted_records = process_features(total_records)
    download_and_upload_images(converted_records, "thumbnails")
    process_database_catalog(converted_records, start_date.isoformat(), end_date.isoformat())

def run_capella_catalog_api():
    BBOX = "-180,-90,180,90"
    START_DATE = (
        SatelliteDateRetrievalPipelineHistory.objects.filter(vendor_name="capella")
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
    response = search_images(START_DATE, END_DATE, BBOX)
    return response
