'''

https://app.skyfi.com/platform-api/redoc#tag/Archive/operation/find_archives_archives_post

# Configure your API key
API_KEY = "ryan@bungalowventures.com:a774e6372c5f172d16ed72d6fb98356763fbe2df4a20ea19a01fb4c72b0337f7"

'''

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
from utils import check_folder_content_and_rename_output_dir, latlon_to_wkt
import math

import shutil
import csv
from decouple import config

# Get the terminal size
columns = shutil.get_terminal_size().columns


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Input Variables

# Mode of operation: "array" or "length"
mode = "length"  # Change to "length" to use length-based generation

# Geohash array input (used if mode is "array")
geohash_input = [
    "wxnp"
]

# Geohash length input (used if mode is "length")
# The length is 'how many more' -- so 2 would provide a geohash3
geohash_seed = "w"
geohash_length = 2
BATCH_SIZE = 28

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

# Define the start and end date range
# START_DATE_STR = '2023-01-01'
# END_DATE_STR = '2024-08-31'

product_types = ["DAY"]
open_data = False



# Configure your API key
API_KEY = config("SKYFI_API_KEY")

def save_image(url, save_path):
    """Save image from URL to the specified path."""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(save_path, 'wb') as out_file:
            out_file.write(response.content)
        # logging.info(f"Image saved successfully at {save_path}")
        return True
    except Exception as e:
        # logging.error(f"Failed to download image from {url}: {e}")
        return False

# Function to get the corners of the geohash

def latlon_to_bbox(lat, lon, range_km):
    """Generate a bounding box from a lat, lon and range in km."""
    geod = Geod(ellps="WGS84")
    north_lat, north_lon, _ = geod.fwd(lon, lat, 0, range_km * 1000)  # move north by range_km
    south_lat, south_lon, _ = geod.fwd(lon, lat, 180, range_km * 1000)  # move south by range_km
    east_lat, east_lon, _ = geod.fwd(lon, lat, 90, range_km * 1000)  # move east by range_km
    west_lat, west_lon, _ = geod.fwd(lon, lat, 270, range_km * 1000)  # move west by range_km
    
    # Format as bbox string: xmin (west), ymin (south), xmax (east), ymax (north)
    return [west_lon, south_lat, east_lon, north_lat]

def get_geohash_corners(geohash):
    try:
        # center_lat, center_lon, lat_err, lon_err = pgh.decode_exactly(geohash)
        # top_left = (center_lat + lat_err, center_lon - lon_err)
        # top_right = (center_lat + lat_err, center_lon + lon_err)
        # bottom_left = (center_lat - lat_err, center_lon - lon_err)
        # bottom_right = (center_lat - lat_err, center_lon + lon_err)
        bbox = latlon_to_bbox(LAT, LON, RANGE)
        return {
            "top_left": bbox[0],
            "top_right": bbox[1],
            "bottom_left": bbox[2],
            "bottom_right": bbox[3]
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        logging.error(f"Error getting geohash corners for {geohash}: {str(e)}")
        return None

# Function to convert geohash to polygon
def geohash_to_polygon(geohash):
    """Convert a geohash to a polygon."""
    corners = get_geohash_corners(geohash)
    if corners:
        return Polygon([
            (corners["top_left"][1], corners["top_left"][0]),
            (corners["top_right"][1], corners["top_right"][0]),
            (corners["bottom_right"][1], corners["bottom_right"][0]),
            (corners["bottom_left"][1], corners["bottom_left"][0]),
            (corners["top_left"][1], corners["top_left"][0])
        ])
    else:
        # logging.error(f"Cannot create polygon for geohash {geohash}. Corners are missing.")
        return None

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

# Function to read the bounding box from the GeoJSON file
def read_bbox_from_geojson(geojson_path):
    """Read the bounding box from a GeoJSON file."""
    try:
        with open(geojson_path, 'r') as f:
            data = geojson.load(f)
            # Assuming the polygon is the first feature
            coordinates = data['features'][0]['geometry']['coordinates'][0]

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
def georectify_image(png_path, geojson_path, geotiffs_folder, image_prefix, image_id, date, target_resolution):
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
            img_array = np.stack((img_array, img_array), axis=-1)  # Stack to create 2 bands

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
                    'w',
                    driver='GTiff',
                    height=img_array.shape[0],
                    width=img_array.shape[1],
                    count=2,  # 2 bands
                    dtype=img_array.dtype,
                    crs='EPSG:4326',  # WGS 84 CRS
                    transform=transform) as dst:
                dst.write(img_array[:, :, 0], 1)  # Write first band
                dst.write(img_array[:, :, 1], 2)  # Write second band

            # logging.info(f"GeoTIFF saved as {geotiff_path}")

    except Exception as e:
        # logging.error(f"Failed to georectify image {png_path}: {e}")
        pass


# Function to search the SkyFi archive with pagination
def search_skyfi_archive(aoi, from_date, to_date, product_types):
    url = "https://app.skyfi.com/platform-api/archives"
    headers = {
        "X-Skyfi-Api-Key": API_KEY,
        "Content-Type": "application/json"
    }
    next_page = 0
    all_archives = []
    while True:
        payload = {
            "aoi": aoi,
            "fromDate": from_date,
            "toDate": to_date,
            "pageNumber": next_page,
            "pageSize": 100
        }

        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            archives = response.json()
            if 'archives' in archives and archives['archives']:
                # logging.info(f"Results found: {len(archives['archives'])} on page {next_page} for date {from_date}")
                all_archives.extend(archives['archives'])
            else:
                # logging.info(f"No results found on page {next_page} for date {from_date}")
                break
            if 'nextPage' in archives and archives['nextPage'] is not None:
                next_page = archives['nextPage']
                # logging.info(f"Moving to next page: {next_page} for date {from_date}")
            else:
                break
        elif response.status_code == 429:
            # logging.warning(f"Rate limit error: {response.status_code}. Response: {response.text}. Increasing wait time.")
            time.sleep(1)
        else:
            # logging.error(f"Error: {response.status_code} - {response.text}. Payload: {json.dumps(payload)}")
            break
    return all_archives

# Function to save polygon data as GeoJSON
def save_geojson(footprint_wkt, properties, filename):
    try:
        # Convert WKT footprint to a polygon
        polygon = wkt.loads(footprint_wkt)

        # Generate GeoJSON data
        geojson_data = {
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "geometry": mapping(polygon),
                "properties": properties  # Include properties in the GeoJSON
            }]
        }

        # Save GeoJSON to file
        with open(filename, 'w') as f:
            json.dump(geojson_data, f)
        # logging.info(f"GeoJSON saved as {filename}")

    except Exception as e:
        pass
        # logging.error(f"Error converting WKT to geometry or saving GeoJSON: {e}")

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

# Worker function for threading
def worker(geohash, single_date, throttle_time, results, output_csv_file, geojson_folder, thumbnails_folder, geotiffs_folder):
    retry_count = 0
    max_retries = 5
    aoi = latlon_to_wkt(LAT, LON, RANGE)
    while retry_count < max_retries:
        from_date = single_date.strftime("%Y-%m-%dT00:00:00+00:00")
        timedelta_days = BATCH_SIZE
        to_date = (single_date + timedelta(days=timedelta_days)).strftime("%Y-%m-%dT00:00:00+00:00")

        # logging.info(f"Submitting request for geohash: {geohash}, date: {single_date.strftime('%Y-%m-%d')}")

        archives = search_skyfi_archive(aoi, from_date, to_date, product_types)
        if archives == 'rate_limit':
            throttle_time += 0.01
            time.sleep(throttle_time)
            retry_count += 1
            # logging.info(f"Retrying ({retry_count}/{max_retries}) for geohash: {geohash}, date: {single_date.strftime('%Y-%m-%d')}")
        elif archives:
            # logging.info(f"Found {len(archives)} results for geohash: {geohash}, date: {single_date.strftime('%Y-%m-%d')}")
            for archive in archives:
                capture_date = archive.get('captureTimestamp', '').split('T')[0]
                # filename_base = f"{capture_date}_{geohash}_{archive.get('provider', '')}_{archive.get('archiveId', '')}"
                filename_base = f"{archive.get('archiveId', '')}"

                # Save geojson file once to ensure it exists
                geojson_filename = os.path.join(geojson_folder, f"{filename_base}.geojson")
                footprint_wkt = archive.get('footprint', '')
                save_geojson(footprint_wkt, archive, geojson_filename)

                # Check if GeoJSON was created before proceeding
                if os.path.exists(geojson_filename):
                    # Download thumbnail if available
                    thumbnail_url = archive.get('thumbnailUrls', {}).get('300x300')
                    if thumbnail_url:
                        thumbnail_filename = os.path.join(thumbnails_folder, f"{filename_base}.png")
                        if save_image(thumbnail_url, thumbnail_filename):
                            # logging.info(f"Thumbnail downloaded successfully: {thumbnail_filename}")

                            # Georectify using GeoJSON file
                            # logging.info(f"Calling georectify_image for {thumbnail_filename} with GeoJSON: {geojson_filename}")
                            georectify_image(thumbnail_filename, geojson_filename, geotiffs_folder, archive.get('provider', ''), archive.get('archiveId', ''), capture_date, (512, 512))
                        else:
                            # logging.error(f"Failed to download thumbnail for archive ID {archive.get('archiveId', '')}")
                            pass
                else:
                    # logging.error(f"GeoJSON file {geojson_filename} does not exist. Georectification skipped.")
                    pass

                results.append(archive)
            process_csv(archives, output_csv_file)
            break
        else:
            # logging.info(f"No results found for geohash: {geohash}, date: {single_date.strftime('%Y-%m-%d')}")
            break
    if retry_count == max_retries:
        pass
        # logging.error(f"Max retries reached for geohash: {geohash}, date: {single_date.strftime('%Y-%m-%d')}")

def skyfi_executor(
   START_DATE_STR,
   END_DATE_STR,
    OUTPUT_DIR,
    OUTPUT_CSV_FILE,
    THUMBNAILS_FOLDER,
    GEOJSON_FOLDER,
    GEOTIFFS_FOLDER,
    GENERATED_BBOX=None
):
        
    # Convert the start and end dates to datetime objects
    start_date = datetime.strptime(START_DATE_STR, '%Y-%m-%d')
    end_date = datetime.strptime(END_DATE_STR, '%Y-%m-%d')

    # Log search criteria
    # logging.info(f"Starting search with mode: {mode} using geohash input {geohash_input if mode == 'array' else geohash_seed} "
    #             f"between dates {START_DATE_STR} and {END_DATE_STR}.")
    throttle_time = 0.001
    results = []

    global BATCH_SIZE
    date_difference = (end_date - start_date).days + 1
    if date_difference < BATCH_SIZE:
        BATCH_SIZE = date_difference
    duration = math.ceil(date_difference / BATCH_SIZE)

    # Determine the list of geohashes to process based on the input mode
    if GENERATED_BBOX:
        geohashes = [GENERATED_BBOX]
    elif mode == "array":
        geohashes = geohash_input
    elif mode == "length":
        geohashes = generate_geohashes(geohash_seed, geohash_length)    
    tqdm_lock = threading.Lock()

    # Create a thread pool executor
    print("-"*columns)
    description = f"Processing Skyfi Catalog \nDate Range: {start_date.date()} to {end_date.date()} \n lat: {LAT} and lon: {LON} \n Range: {RANGE} \nOutput Directory: {OUTPUT_DIR}"
    print(description)
    print("-"*columns)
    print("Batch Size: ", BATCH_SIZE, ", days: ", date_difference)
    print("Duration :", duration, "batch")

    with tqdm(total=duration, desc="", unit="batch") as pbar:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = []


            for geohash in geohashes:
                # Loop through each date in the date range
                for single_date in date_range(start_date, end_date, BATCH_SIZE):
                    future = executor.submit(worker, geohash, single_date, throttle_time, results, OUTPUT_CSV_FILE,  GEOJSON_FOLDER, THUMBNAILS_FOLDER, GEOTIFFS_FOLDER)
                    futures.append(future)

            # Ensure all futures are completed
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    # logging.error(f"Error processing future: {e}")
                    pass
                with tqdm_lock:
                    pbar.update(1)
                    pbar.refresh()

            
        tqdm.write("Completed Skyfi Processing")


    

# Example usage
if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser(description='Skyfi Catelog API Executor')
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
    RANGE = int(args.range)
    LAT, LON = args.lat, args.long
    BBOX = list(map(float, (args.bbox).replace("t", "-").split(",")))
    print(f"Generated BBOX: {BBOX}")
        
    # Output folder variable
    output_folder = args.output_dir + f"/skyfi/{START_DATE}_{END_DATE}"
    os.makedirs(output_folder, exist_ok=True)

    # Create output directories for thumbnails, geojson, and geotiffs
    thumbnails_folder = os.path.join(output_folder, "thumbnails")
    geojson_folder = os.path.join(output_folder, "geojson")
    geotiffs_folder = os.path.join(output_folder, "geotiffs")

    output_csv_file = f"{output_folder}/output_skyfi.csv"

    # Ensure the output folders exist
    os.makedirs(thumbnails_folder, exist_ok=True)
    os.makedirs(geojson_folder, exist_ok=True)
    os.makedirs(geotiffs_folder, exist_ok=True)
    
    skyfi_executor(
        START_DATE,
        END_DATE,
        output_folder,
        output_csv_file,
        thumbnails_folder,
        geojson_folder,
        geotiffs_folder,
        BBOX
    )

    check_folder_content_and_rename_output_dir(
        thumbnails_folder,
        output_folder,
        START_DATE,
        END_DATE,
        args.output_dir,
        "skyfi"
    )
    