import requests
from datetime import datetime, timedelta
from tqdm import tqdm
import math
import shutil
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
from core.services.utils import calculate_area_from_geojson

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
    last_scene_id=None,
):
    """
    Fetches collections from the BlackSky API.
    """
    url = f"{BLACKSKY_BASE_URL}/v1/catalog/stac/search"

    headers = {"Accept": "application/json", "Authorization": auth_token}
    params = {
        "bbox": bbox,
        "time": datetime_range,
        "limit": 300,
    }
    if last_scene_id:
        params["searchAfterId"] = last_scene_id
    try:
        response = requests.get(url, params=params, headers=headers)
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")

    return None


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
            "vendor_name": "blacksky",
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
        url = feature.get("assets", {}).get("browseUrl", {}).get("href")
        headers = {"Content-Type": "application/json", "Authorization": AUTH_TOKEN}
        response = requests.get(url, headers=headers, stream=True, timeout=(10, 30))
        response.raise_for_status()
        filename = feature.get("id")
        content = response.content
        response_url = save_image_in_s3_and_get_url(content, filename, folder)
        # response_geotiff = geotiff_conversion_and_s3_upload(
        #     content, filename, "blacksky/geotiffs", feature.get("geometry")
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
                    print(f"Failed to upload image for feature {feature.get('id')}")
            except Exception as e:
                print(f"Exception occurred for feature {feature.get('id')}: {e}")


def convert_to_model_params(features):
    response = []
    for feature in features:
        try:
            model_params = {
                "acquisition_datetime": datetime.fromisoformat(
                    feature["properties"]["datetime"].replace("Z", "+00:00")
                ),
                "cloud_cover": feature["properties"]["cloudPercent"],
                "vendor_id": feature["id"],
                "vendor_name": "blacksky",
                "sensor": feature["properties"]["sensorId"],
                "area": calculate_area_from_geojson(feature["geometry"], feature["id"]),
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
                "georeferenced": feature["properties"]["georeferenced"],
                "location_polygon": feature["geometry"],
                "coordinates_record": feature["geometry"],
            }
            response.append(model_params)
        except Exception as e:
            print(e)
    return response


def fetch_and_process_records(auth_token, bbox, start_time, end_time, last_scene_id):
    """Fetches records from the BlackSky API and processes them."""
    all_records = []
    last_record_scene_id = last_scene_id
    while True:
        if last_record_scene_id:
            records = get_blacksky_collections(
                auth_token,
                bbox=bbox,
                datetime_range=f"{start_time}/{end_time}",
                last_scene_id=last_record_scene_id,
            )
        else:
            records = get_blacksky_collections(
                auth_token, bbox=bbox, datetime_range=f"{start_time}/{end_time}"
            )
        if not records.get("features", []):
            break

        all_records.extend(records.get("features", []))
        last_record = records.get("features", [])[-1]
        last_record_scene_id = last_record.get("id")

    if not all_records:
        return 0
    download_and_upload_images(all_records, "blacksky/thumbnails")
    converted_features = convert_to_model_params(all_records)
    process_database_catalog(converted_features, start_time, end_time)
    return len(all_records)


def main(START_DATE, END_DATE, BBOX, last_scene_id):
    bboxes = [BBOX]
    current_date = START_DATE
    end_date = END_DATE
    global BATCH_SIZE
    date_difference = (end_date - current_date).days + 1
    if date_difference < BATCH_SIZE:
        BATCH_SIZE = date_difference
    duration = math.ceil(date_difference / BATCH_SIZE)
    print("-" * columns)
    print("-" * columns)
    print("Batch Size: ", BATCH_SIZE, ", days: ", date_difference)
    print("Duration :", duration, "batch")
    total_records = 0
    while current_date <= end_date:
        start_time = current_date.isoformat()
        if (end_date - current_date).days > 1:
            end_time = (current_date + timedelta(days=BATCH_SIZE)).isoformat()
        else:
            end_time = end_date.isoformat()

        for bbox in bboxes:
            response = fetch_and_process_records(
                AUTH_TOKEN, bbox, start_time, end_time, last_scene_id
            )
            if response:
                total_records += response

        current_date += timedelta(days=BATCH_SIZE)
    print("Completed processing BlackSky data")
    return total_records


def run_blacksky_catalog_api():
    BBOX = "-180,-90,180,90"
    START_DATE = (
        SatelliteDateRetrievalPipelineHistory.objects.filter(vendor_name="blacksky")
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
        last_scene_id = None
    else:
        START_DATE = START_DATE.end_datetime
        last_scene_id = (
            SatelliteCaptureCatalog.objects.filter(vendor_name="blacksky")
            .order_by("-acquisition_datetime")
            .first()
            .vendor_id
        )
        print(f"From DB: {START_DATE}")

    END_DATE = get_utc_time()
    print(f"Start Date: {START_DATE}, End Date: {END_DATE}")
    response = main(START_DATE, END_DATE, BBOX, last_scene_id)
    return response