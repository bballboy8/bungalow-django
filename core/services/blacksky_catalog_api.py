import requests
from datetime import datetime, timedelta
from tqdm import tqdm
import math
import shutil
from decouple import config
from core.serializers import SatelliteCaptureCatalogSerializer
from datetime import datetime
from django.contrib.gis.geos import Polygon
from concurrent.futures import ThreadPoolExecutor, as_completed
from core.utils import  save_image_in_s3_and_get_url
from botocore.exceptions import NoCredentialsError
import numpy as np
from rasterio.transform import from_bounds
from rasterio.io import MemoryFile
from io import BytesIO
from PIL import Image

columns = shutil.get_terminal_size().columns

# Configuration
BLACKSKY_BASE_URL = "https://api.blacksky.com"
AUTH_TOKEN = config("BLACKSKY_API_KEY")
MAX_THREADS = 10
BATCH_SIZE = 28


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


def process_database_catalog(features):
    serializer = SatelliteCaptureCatalogSerializer(data=features, many=True)
    if serializer.is_valid():
        serializer.save()
    else:
        print(serializer.errors)

def get_polygon_bounding_box(polygon):
    """Extracts the bounding box from a polygon's coordinates."""
    min_lon = min([point[0] for point in polygon['coordinates'][0]])
    max_lon = max([point[0] for point in polygon['coordinates'][0]])
    min_lat = min([point[1] for point in polygon['coordinates'][0]])
    max_lat = max([point[1] for point in polygon['coordinates'][0]])

    return min_lon, min_lat, max_lon, max_lat

def geotiff_conversion_and_s3_upload(content, filename, tiff_folder, polygon=None):
    img = Image.open(BytesIO(content))
    img_array = np.array(img)

    # Define the transform based on polygon bounds
    if polygon:
        min_lon, min_lat, max_lon, max_lat = get_polygon_bounding_box(polygon)
        transform = from_bounds(min_lon, min_lat, max_lon, max_lat, img_array.shape[1], img_array.shape[0])
    else:
        print("Polygon bounds not provided.")
        return False

    # Step 3: Convert to GeoTIFF and save to S3
    with MemoryFile() as memfile:
        with memfile.open(
            driver='GTiff',
            height=img_array.shape[0],
            width=img_array.shape[1],
            count=img_array.shape[2] if len(img_array.shape) == 3 else 1,
            dtype=img_array.dtype,
            crs='EPSG:4326',
            transform=transform
        ) as dst:
            if len(img_array.shape) == 2:  # Grayscale
                dst.write(img_array, 1)
            else:  # RGB or multi-channel
                for i in range(img_array.shape[2]):
                    dst.write(img_array[:, :, i], i + 1)

        # Upload the GeoTIFF to S3
        geotiff_url = save_image_in_s3_and_get_url(memfile.read(), filename, tiff_folder, "tif")
        print(f"Uploaded GeoTIFF for feature {filename} to {geotiff_url}")
        return geotiff_url



def upload_to_s3(feature, folder="thumbnails"):
    """Downloads an image from the URL in the feature and uploads it to S3."""
    try:
        url = feature.get("assets", {}).get("browseUrl", {}).get("href")
        headers = {"Content-Type": "application/json", "Authorization": AUTH_TOKEN}
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()
        filename = feature.get("id")
        content = response.content  
        response_url = save_image_in_s3_and_get_url(content, filename, folder)
        response_geotiff = geotiff_conversion_and_s3_upload(content, filename, "geotiffs", feature.get("geometry"))
        print(f"Uploaded image for feature {feature.get('id')} to {response_url}, {response_geotiff}")
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
                    print(f"Failed to upload image for feature {feature.get('id')}")
            except Exception as e:
                print(f"Exception occurred for feature {feature.get('id')}: {e}")


def convert_to_model_params(features):
    response = []
    for feature in features:
        try:
            location_polygon = Polygon(feature["geometry"]["coordinates"][0], srid=4326)
            model_params = {
                "acquisition_datetime": datetime.fromisoformat(
                    feature["properties"]["datetime"].replace("Z", "+00:00")
                ),
                "cloud_cover": feature["properties"]["cloudPercent"],
                "vendor_id": feature["id"],
                "vendor_name": "blacksky",
                "sensor": feature["properties"]["sensorId"],
                "area": location_polygon.area,
                "type": (
                    "Day"
                    if 6
                    <= datetime.fromisoformat(
                        feature["properties"]["datetime"].replace("Z", "+00:00")
                    ).hour
                    <= 18
                    else "Night"
                ),
                "sun_elevation": feature["properties"]["sunAzimuth"],
                "resolution": f"{feature['properties']['gsd']}m",
                "georeferenced": feature["properties"]["georeferenced"] == "True",
                "location_polygon": feature["geometry"],
            }
            response.append(model_params)
        except Exception as e:
            print(e)
    return response


def fetch_and_process_records(auth_token, bbox, start_time, end_time):
    """Fetches records from the BlackSky API and processes them."""
    records = get_blacksky_collections(
        auth_token, bbox=bbox, datetime_range=f"{start_time}/{end_time}"
    )
    if records is None:
        return
    features = records.get("features", [])
    download_and_upload_images(features, "thumbnails")
    features = convert_to_model_params(features)
    process_database_catalog(features)


def main(START_DATE, END_DATE, BBOX):
    bboxes = [BBOX]
    current_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date = datetime.strptime(END_DATE, "%Y-%m-%d")
    global BATCH_SIZE
    date_difference = (end_date - current_date).days + 1
    if date_difference < BATCH_SIZE:
        BATCH_SIZE = date_difference
    duration = math.ceil(date_difference / BATCH_SIZE)
    print("-" * columns)
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


def run_blacksky_catalog_api():
    BBOX = "-180,-90,180,90"
    print(f"Generated BBOX: {BBOX}")
    START_DATE = "2024-10-25"
    END_DATE = "2024-10-27"
    main(START_DATE, END_DATE, BBOX)
