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
# Input Variables

# Mode of operation: "array" or "length"
mode = "length"  # Change to "length" to use length-based generation

# Geohash array input (used if mode is "array")
geohash_input = ["wxnp"]

# Geohash length input (used if mode is "length")
# The length is 'how many more' -- so 2 would provide a geohash3
geohash_seed = "w"
geohash_length = 2
BATCH_SIZE = 28


product_types = ["DAY"]
open_data = False


# Configure your API key
API_KEY = config("SKYFI_API_KEY")


# Function to read the bounding box from the GeoJSON file
def read_bbox_from_geojson(geojson_path):
    """Read the bounding box from a GeoJSON file."""
    try:
        with open(geojson_path, "r") as f:
            data = geojson.load(f)
            # Assuming the polygon is the first feature
            coordinates = data["features"][0]["geometry"]["coordinates"][0]

            # Extract all the longitude and latitude values
            lons = [coord[0] for coord in coordinates]
            lats = [coord[1] for coord in coordinates]

            # Determine the bounding box corners
            bottom_left = [min(lons), min(lats)]
            bottom_right = [max(lons), min(lats)]
            top_right = [max(lons), max(lats)]
            top_left = [min(lons), max(lats)]

            # Return the points in a flat format (lon, lat pairs)
            return [bottom_left, bottom_right, top_right, top_left]
    except Exception as e:
        # logging.error(f"Error reading bounding box from GeoJSON file {geojson_path}: {e}")
        return None


# Function to remove black borders from the image
def remove_black_borders(img):
    """Remove black borders from the image."""
    bg = Image.new(img.mode, img.size, img.getpixel((0, 0)))
    diff = ImageChops.difference(img, bg)
    bbox = diff.getbbox()
    if bbox:
        # logging.info("Black borders detected and removed.")
        return img.crop(bbox)
    # logging.info("No black borders detected.")
    return img


# Function to georectify the image and save as GeoTIFF
def georectify_image(
    png_path,
    geojson_path,
    geotiffs_folder,
    image_prefix,
    image_id,
    date,
    target_resolution,
):
    """
    Georectify the PNG image, crop padding, upscale to target resolution, and save as a 2-band black and white GeoTIFF.
    """
    # logging.info(f"Starting georectification for {png_path} using GeoJSON at {geojson_path}")
    try:
        # Open the image
        with Image.open(png_path) as img:
            # Ensure image is converted to grayscale (if not already)
            img = img.convert("L")  # Convert to grayscale

            # Remove black borders
            img = remove_black_borders(img)

            # Resize image to target resolution
            img = img.resize(target_resolution, Image.Resampling.LANCZOS)

            # Convert grayscale image to 2-band (black and white)
            img_array = np.array(img)
            img_array = np.stack(
                (img_array, img_array), axis=-1
            )  # Stack to create 2 bands

            # Read bounding box from GeoJSON
            corners = read_bbox_from_geojson(geojson_path)
            if not corners:
                # logging.error("Failed to extract bounding box from GeoJSON.")
                return

            # Extract bounding box coordinates for each corner
            bottom_left, bottom_right, top_right, top_left = corners

            # Flatten the corner coordinates
            left, bottom = bottom_left[0], bottom_left[1]
            right, top = top_right[0], top_right[1]

            # Compute affine transform from the bounding box corners
            width, height = target_resolution
            transform = from_bounds(left, bottom, right, top, width, height)

            # Correct naming convention for the GeoTIFF file
            base_name = f"{image_prefix}_{date}_{image_id}"
            geotiff_name = f"{base_name}.tif"
            geotiff_path = os.path.join(geotiffs_folder, geotiff_name)

            # logging.info(f"Saving GeoTIFF to {geotiff_path}")

            # Save the image as a 2-band GeoTIFF
            with rasterio.open(
                geotiff_path,
                "w",
                driver="GTiff",
                height=img_array.shape[0],
                width=img_array.shape[1],
                count=2,  # 2 bands
                dtype=img_array.dtype,
                crs="EPSG:4326",  # WGS 84 CRS
                transform=transform,
            ) as dst:
                dst.write(img_array[:, :, 0], 1)  # Write first band
                dst.write(img_array[:, :, 1], 2)  # Write second band

            # logging.info(f"GeoTIFF saved as {geotiff_path}")

    except Exception as e:
        # logging.error(f"Failed to georectify image {png_path}: {e}")
        pass


# Function to search the SkyFi archive with pagination
def search_skyfi_archive(aoi, from_date, to_date, product_types):
    url = "https://app.skyfi.com/platform-api/archives"
    headers = {"X-Skyfi-Api-Key": API_KEY, "Content-Type": "application/json"}
    next_page = 0
    all_archives = []
    while True:
        payload = {
            "aoi": aoi,
            'fromDate': '2024-11-01T00:00:00+00:00',
            'toDate': '2024-11-10T00:00:00+00:00',
            'pageNumber': 0,
            'pageSize': 100
        }
        print(payload)

        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            print(response.json())
            archives = response.json()
            if "archives" in archives and archives["archives"]:
                all_archives.extend(archives["archives"])
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

# Function to generate date range
def date_range(start_date, end_date, range_days):
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=range_days)


def process_csv(features, OUTPUT_CSV_FILE):
    """Appends feature properties and geometries to the CSV file."""
    write_header = (
        not os.path.exists(OUTPUT_CSV_FILE) or os.path.getsize(OUTPUT_CSV_FILE) == 0
    )

    with open(OUTPUT_CSV_FILE, mode="a", newline="") as csv_file:
        csv_writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)

        if features:
            header = list(features[0].keys())
            if write_header:
                csv_writer.writerow(header)

            for feature in features:
                row = [(feature.get(key)) for key in header]
                csv_writer.writerow(row)


def worker(start_date, end_date, throttle_time, bboxes, duration, results):
    retry_count = 0
    max_retries = 5
    try:
        from_date_str = start_date.isoformat()
        timedelta_days = BATCH_SIZE
        if duration > 1:
            end_date_str = (start_date + timedelta(days=timedelta_days)).isoformat()
        else:
            end_date_str = end_date.isoformat()
        for i in range(0, len(bboxes)):
            try:
                aoi = bbox_to_wkt(bboxes[i])
                print(aoi)    
                # archives = search_skyfi_archive(aoi, from_date_str, end_date_str, product_types)
                # if archives == "rate_limit":
                #     throttle_time += 0.01
                #     time.sleep(throttle_time)
                #     retry_count += 1
                # elif archives:
                #     for archive in archives:
                #         results.append(archive)
            except Exception as e:
                print(e)
                retry_count += 1
                time.sleep(1)
        print(len(bboxes), "BBOXES")
    except Exception as e:
        print(e)
        import traceback
        traceback.print_exc()
        retry_count += 1
        time.sleep(1)

    if retry_count == max_retries:
        pass

def generate_bboxes(lat_min, lat_max, lon_min, lon_max, step=10):
    bboxes = []
    lat = lat_min
    while lat < lat_max:
        lon = lon_min
        while lon < lon_max:
            bbox = f"{lon},{lat},{lon+step},{lat+step}"
            bboxes.append(bbox)
            lon += step
        lat += step
    return bboxes

def skyfi_executor(START_DATE, END_DATE):
    bboxes = generate_bboxes(-90, 90, -180, 180, step=8)
    print(len(bboxes), "BBOXES")
    throttle_time = 0.001
    results = []
    start_date = START_DATE
    end_date = END_DATE

    global BATCH_SIZE
    date_difference = (end_date - start_date).days + 1
    if date_difference < BATCH_SIZE:
        BATCH_SIZE = date_difference
    duration = math.ceil(date_difference / BATCH_SIZE)

    tqdm_lock = threading.Lock()

    print("-" * columns)
    print("Batch Size: ", BATCH_SIZE, ", days: ", date_difference)
    print("Duration :", duration, "batch")
    i = 0
    with tqdm(total=duration, desc="", unit="batch") as pbar:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            print("Start Date: ", start_date, "End Date: ", end_date)
            for start_date in date_range(start_date, end_date, BATCH_SIZE):
                print(i)
                future = executor.submit(
                    worker, start_date, end_date, throttle_time, bboxes, duration, results
                )
                futures.append(future)
                i += 1

            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    pass
                with tqdm_lock:
                    pbar.update(1)
                    pbar.refresh()

        tqdm.write("Completed Skyfi Processing")

    print("Total Archives: ", len(results))


def bbox_to_wkt(bbox):
    min_lon, min_lat, max_lon, max_lat = map(float, bbox.split(","))
    wkt = f"POLYGON(({min_lon} {min_lat}, {max_lon} {min_lat}, {max_lon} {max_lat}, {min_lon} {max_lat}, {min_lon} {min_lat}))"
    return wkt


def run_skyfi_catalog_api():
    START_DATE = (
        SatelliteDateRetrievalPipelineHistory.objects.filter(vendor_name="skyfi")
        .order_by("-end_datetime")
        .first()
    )
    if not START_DATE:
        START_DATE = datetime(
            datetime.now().year,
            datetime.now().month,
            datetime.now().day ,
            tzinfo=pytz.utc,
        )
    else:
        START_DATE = START_DATE.end_datetime
        print(f"From DB: {START_DATE}")

    END_DATE = get_utc_time()
    print(f"Start Date: {START_DATE}, End Date: {END_DATE}")
    response = skyfi_executor(START_DATE, END_DATE)
    return response
