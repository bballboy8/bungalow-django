"""

https://app.skyfi.com/platform-api/redoc#tag/Archive/operation/find_archives_archives_post

# Configure your API key
API_KEY = "ryan@bungalowventures.com:a774e6372c5f172d16ed72d6fb98356763fbe2df4a20ea19a01fb4c72b0337f7"

"""

import requests
import logging
import json
from datetime import datetime, timedelta
import time
import concurrent.futures
import os
import io
from shapely import wkt
from shapely.geometry import mapping, shape, box
import pygeohash as pgh
from shapely.geometry import Polygon
import geojson
from PIL import Image, ImageChops
import numpy as np
import rasterio
from rasterio.transform import from_bounds
import argparse
from tqdm import tqdm
import threading
from pyproj import Geod
import math

import shutil
import csv
from decouple import config

from datetime import datetime, timezone
from django.contrib.gis.geos import Polygon
from concurrent.futures import ThreadPoolExecutor, as_completed
from core.utils import save_image_in_s3_and_get_url, get_holdback_seconds, process_database_catalog, get_centroid_and_region_and_location_polygon, get_centroid_region_and_local
from botocore.exceptions import NoCredentialsError
import numpy as np
from rasterio.transform import from_bounds
from rasterio.io import MemoryFile
from io import BytesIO
from PIL import Image
from bungalowbe.utils import get_utc_time, convert_iso_to_datetime
from core.models import SatelliteDateRetrievalPipelineHistory
import pytz
from shapely import wkt
from shapely.geometry import mapping
import pandas as pd
import pvlib

# Get the terminal size
columns = shutil.get_terminal_size().columns

BATCH_SIZE = 28


# Configure your API key
API_KEY = config("SKYFI_API_KEY")


def wkt_to_geojson(wkt_string):
    polygon = wkt.loads(wkt_string)
    geojson_dict = mapping(polygon)
    return geojson_dict


def convert_to_utc(capture_timestamp):
    local_time = datetime.fromisoformat(capture_timestamp)
    utc_time = local_time.astimezone(timezone.utc)
    return utc_time


def estimate_sun_angles(capture_time, footprint_wkt):
    try:
        capture_timestamp = datetime.fromisoformat(capture_time)
        polygon = wkt.loads(footprint_wkt)
        centroid = polygon.centroid
        latitude, longitude = centroid.y, centroid.x
        times = pd.DatetimeIndex([capture_timestamp])
        solar_position = pvlib.solarposition.get_solarposition(
            times, latitude, longitude
        )
        azimuth = solar_position["azimuth"].iloc[0]
        return float(azimuth)
    except Exception as e:
        print(f"Error in sun angle calculation: {e}")
        return 0

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
        url = feature.get("thumbnail_url", {}).values()
        if not url:
            return False
        url = list(url)[0]
        response = requests.get(url, stream=True, timeout=(10, 30))
        response.raise_for_status()
        filename = feature.get("vendor_id")
        content = response.content
        response_url = save_image_in_s3_and_get_url(content, filename, folder)
        # response_geotiff = geotiff_conversion_and_s3_upload(
        #     content, filename, "skyfi/geotiffs", feature.get("location_polygon")
        # )
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
                    print(f"Failed to upload image for feature {feature.get('vendor_id')}")
            except Exception as e:
                print(f"Exception occurred for feature {feature.get('vendor_id')}: {e}")


def process_single_feature(feature):
    try:
        location_polygon = wkt_to_geojson(feature["footprint"])
        utc_time = convert_to_utc(feature["captureTimestamp"])
        publication_datetime = datetime.now(pytz.utc).replace(microsecond=0)
        centroid_dict = get_centroid_and_region_and_location_polygon(location_polygon)

        model_params = {
            "acquisition_datetime": utc_time,
            "cloud_cover_percent": -1,
            "vendor_id": feature["archiveId"],
            "vendor_name": f"skyfi-{feature['provider'].lower()}",
            "sensor": feature["constellation"],
            "area": feature["totalAreaSquareKm"],
            "sun_elevation": estimate_sun_angles(
                feature["captureTimestamp"], feature["footprint"]
            ),
            "resolution": f"{feature['gsd']}m",
            "thumbnail_url": feature["thumbnailUrls"],
            "location_polygon": location_polygon,
            "coordinates_record": location_polygon,
            "metadata": feature,
            "gsd": float(feature["gsd"]) / 100,
            "offnadir": feature.get("offNadirAngle"),
            "constellation": feature["constellation"],
            "platform": feature["provider"],
            "azimuth_angle": None,
            "illumination_azimuth_angle": None,
            "illumination_elevation_angle": None,
            "holdback_seconds": get_holdback_seconds(utc_time, publication_datetime),
            "publication_datetime": publication_datetime,
            **centroid_dict,
        }
        return model_params
    except Exception as e:
        print(e)
        return None


def convert_to_model_params(features):
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


# Function to search the SkyFi archive with pagination
def search_skyfi_archive(aoi, from_date, to_date):
    url = "https://app.skyfi.com/platform-api/archives"
    headers = {"X-Skyfi-Api-Key": API_KEY, "Content-Type": "application/json"}
    next_page = 0
    all_archives = []
    while True:
        payload = {
            "aoi": aoi,
            "fromDate": from_date,
            "toDate": to_date,
            "pageNumber": next_page,
            "pageSize": 100,
            "productType": "SAR",
            "providers": ["UMBRA"],

        }
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            archives = response.json()
            if "archives" in archives and archives["archives"]:
                all_archives.extend(archives["archives"])
                time.sleep(1)
            else:
                break
            if "nextPage" in archives and archives["nextPage"] is not None:
                next_page = archives["nextPage"]
            else:
                break
        elif response.status_code == 429:
            time.sleep(1)
        else:
            break
    return all_archives


def worker(start_date, end_date, aoi, results):
    try:
        archives = search_skyfi_archive(aoi, start_date, end_date)
        if archives:
            results.extend(archives)
    except Exception as e:
        print(e)
        time.sleep(1)


def skyfi_executor(START_DATE, END_DATE, LAND_POLYGONS_WKT,IS_BULK):
    results = []
    current_date = START_DATE
    end_date = END_DATE

    global BATCH_SIZE
    date_difference = (end_date - current_date).days + 1
    if date_difference < BATCH_SIZE:
        BATCH_SIZE = date_difference
    duration = math.ceil(date_difference / BATCH_SIZE)

    print("-" * columns)
    print("Batch Size: ", BATCH_SIZE, ", days: ", date_difference)
    print("Duration :", duration, "batch")
    while current_date <= end_date:  # Inclusive of end_date
        start_time = current_date.isoformat()
        if (end_date - current_date).days > 1:
            end_time = (current_date + timedelta(days=BATCH_SIZE)).isoformat()
        else:
            end_time = end_date.isoformat()
        print("Start Time: ", start_time, "End Time: ", end_time)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(worker, start_time, end_time, bbox, results)
                for bbox in LAND_POLYGONS_WKT
            ]
            for future in concurrent.futures.as_completed(futures):
                future.result()

        current_date += timedelta(days=BATCH_SIZE)

    print("Total records: ", len(results))
    converted_features = convert_to_model_params(results)
    # download_and_upload_images(converted_features, "skyfi/thumbnails")
    process_database_catalog(
        converted_features, START_DATE.isoformat(), end_date.isoformat(), "skyfi-umbra", IS_BULK
    )


def run_skyfi_catalog_api():
    START_DATE = (
        SatelliteDateRetrievalPipelineHistory.objects.filter(vendor_name="skyfi-umbra")
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
    START_DATE = START_DATE.replace(hour=0, minute=0, second=0, microsecond=0)
    print(f"Start Date: {START_DATE}, End Date: {END_DATE}")
    land_polygons_wkt = []
    with open("core/services/land_polygons.json", "r") as file:
        land_polygons_wkt = json.load(file)
    response = skyfi_executor(START_DATE, END_DATE, land_polygons_wkt, False)
    return response

def run_skfyfi_catalog_api_bulk():
    START_DATE = datetime(2025, 2, 11, tzinfo=pytz.utc)
    END_LIMIT = datetime(2025, 2, 12, tzinfo=pytz.utc)
    import time
    while START_DATE < END_LIMIT:
        END_DATE = min(START_DATE + timedelta(days=1), END_LIMIT)
        print(f"Start Date: {START_DATE}, End Date: {END_DATE}")
        month_start_time = time.time()
        land_polygons_wkt = []

        with open("core/services/land_polygons.json", "r") as file:
            land_polygons_wkt = json.load(file)
        
        response = skyfi_executor(START_DATE, END_DATE, land_polygons_wkt, True)
        month_end_time = time.time()
        print(f"Time taken to process the interval: {month_end_time - month_start_time}")

        START_DATE = END_DATE
    return response

def run_skfyfi_catalog_api_bulk_for_last_35_days_from_now():
    START_DATE = (datetime.now(pytz.utc) - timedelta(days=35)).replace(hour=0, minute=0, second=0, microsecond=0)
    END_LIMIT = (datetime.now(pytz.utc)).replace(hour=0, minute=0, second=0, microsecond=0)
    import time
    while START_DATE < END_LIMIT:
        END_DATE = min(START_DATE + timedelta(days=1), END_LIMIT)
        print(f"Start Date: {START_DATE}, End Date: {END_DATE}")
        month_start_time = time.time()
        land_polygons_wkt = []

        with open("core/services/land_polygons.json", "r") as file:
            land_polygons_wkt = json.load(file)
        
        response = skyfi_executor(START_DATE, END_DATE, land_polygons_wkt, True)
        month_end_time = time.time()
        print(f"Time taken to process the interval: {month_end_time - month_start_time}")
        time.sleep(10)
        START_DATE = END_DATE

    return "Skfyfi 35 days bulk processing completed"

# from core.services.skyfi_catalog_api import run_skfyfi_catalog_api_bulk
# run_skfyfi_catalog_api_bulk()

# from core.services.skyfi_catalog_api import run_skfyfi_catalog_api_bulk_for_last_35_days_from_now
# run_skfyfi_catalog_api_bulk_for_last_35_days_from_now()