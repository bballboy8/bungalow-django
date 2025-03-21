import requests
from datetime import datetime, timedelta
import math
import shutil
from decouple import config
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from core.utils import save_image_in_s3_and_get_url, process_database_catalog, get_holdback_seconds, get_centroid_and_region_and_location_polygon, get_centroid_region_and_local,remove_z_from_geometry, mark_record_as_purchased
from botocore.exceptions import NoCredentialsError
import numpy as np
from rasterio.transform import from_bounds
from rasterio.io import MemoryFile
from io import BytesIO
from PIL import Image
from bungalowbe.utils import get_utc_time
from core.models import SatelliteDateRetrievalPipelineHistory, CollectionCatalog
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

def get_blacksky_products_collection(auth_token, url=None):
    """
        Fetches Purchased Products from the BlackSky API.
    """
    if not url:
        url = f"{BLACKSKY_BASE_URL}/v1/products/stac/search"
    headers = {"Accept": "application/json", "Authorization": auth_token}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        final_response = response.json()
        return final_response
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")




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

def process_single_feature(feature):
    try:
        acquisition_datetime = datetime.fromisoformat(
            feature["properties"]["datetime"].replace("Z", "+00:00")
        )
        # Make the current time the publication time
        publication_datetime = datetime.now(pytz.utc).replace(microsecond=0)
        if not feature["geometry"].get("type") == "Polygon":
            return None, None
        centroid_dict = get_centroid_and_region_and_location_polygon(remove_z_from_geometry(feature["geometry"]))

        model_params = {
            "acquisition_datetime": acquisition_datetime,
            "cloud_cover_percent": feature["properties"]["cloudPercent"],
            "vendor_id": feature["id"],
            "vendor_name": "blacksky",
            "sensor": feature["properties"]["sensorId"],
            "area": calculate_area_from_geojson(remove_z_from_geometry(feature["geometry"]), feature["id"]),
            "sun_elevation": feature["properties"]["sunAzimuth"],
            "resolution": f"{feature['properties']['gsd']}m",
            "georeferenced": feature["properties"]["georeferenced"],
            "location_polygon": remove_z_from_geometry(feature["geometry"]),
            "coordinates_record": remove_z_from_geometry(feature["geometry"]),
            "metadata": feature,
            "gsd": float(feature["properties"]["gsd"]),
            "constellation": str(feature["id"])[:7],
            "platform": feature["properties"]["vendorId"],
            "offnadir" : feature["properties"]["offNadirAngle"],
            "azimuth_angle" : None,
            "illumination_azimuth_angle": feature["properties"]["sunAzimuth"],
            "illumination_elevation_angle": None,
            "holdback_seconds": get_holdback_seconds(acquisition_datetime, publication_datetime),
            "publication_datetime": publication_datetime,
            **centroid_dict
        }
        return model_params
    except Exception as e:
        print(e)


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


def fetch_and_process_records(auth_token, bbox, start_time, end_time, last_scene_id, is_bulk):
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
    print(len(all_records))
    # download_and_upload_images(all_records, "blacksky/thumbnails")
    converted_features = convert_to_model_params(all_records)
    process_database_catalog(converted_features, start_time, end_time, "blacksky", is_bulk)
    return len(all_records)


def fetch_and_process_products_records():
    """Fetches records from the BlackSky API and processes them."""
    all_records = []
    url = None
    while True:
        records = get_blacksky_products_collection(AUTH_TOKEN, url)
        all_records.extend(records.get("features", []))
        if not records.get("links"):
            break
        next_record = next(
            (record for record in records["links"] if record["rel"] == "next"), None
        )
        if not next_record:
            break
        url = next_record["href"]

    if not all_records:
        return 0
    print(len(all_records))
    # download_and_upload_images(all_records, "blacksky/thumbnails")
    converted_features = convert_to_model_params(all_records)
    for feature in converted_features:
        try:
            hq_product_artifacts_png(feature)
        except Exception as e:
            print(e)

    process_database_catalog(converted_features, "Product", "Product", "blacksky", True)
    mark_record_as_purchased(converted_features)
    return len(all_records)

def retrieve_product_artificats(product_id):
    url = f"{BLACKSKY_BASE_URL}/v1/products/{product_id}/artifacts"
    headers = { "Authorization": AUTH_TOKEN, "Accept": "application/json" }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"Failed to download {url}: {e}")

def download_product_artifacts(product_id, artifact_id, vendor_id):

    url = f"{BLACKSKY_BASE_URL}/v1/products/{product_id}/artifacts/{artifact_id}/download"
    headers = { "Authorization" : AUTH_TOKEN }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        content = response.content
        filename = vendor_id
        print("uploading to s3")
        response_url = save_image_in_s3_and_get_url(content, filename, "blacksky")
        return response_url
    except requests.exceptions.RequestException as e:
        print(f"Failed to download {url}: {e}")

def hq_product_artifacts_png(feature):
    try:
        product_id = feature['metadata']["properties"]["productId"]
        response = retrieve_product_artificats(product_id)
        if response:
            artifacts = response.json()["data"]
            for artifact in artifacts:
                if artifact["format"] == "PNG":
                    print(artifact)
                    response = download_product_artifacts(product_id, artifact["id"], feature["vendor_id"])
                    print(response)
                    break
    except Exception as e:
        print(e)

def main(START_DATE, END_DATE, BBOX, last_scene_id, is_bulk):
    bboxes = [BBOX]
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
    total_records = 0
    while current_date <= end_date:
        start_time = current_date.isoformat()
        if (end_date - current_date).days > 1:
            end_time = (current_date + timedelta(days=BATCH_SIZE)).isoformat()
        else:
            end_time = end_date.isoformat()

        for bbox in bboxes:
            response = fetch_and_process_records(
                AUTH_TOKEN, bbox, start_time, end_time, last_scene_id, is_bulk
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
            CollectionCatalog.objects.filter(vendor_name="blacksky")
            .order_by("-acquisition_datetime")
            .first()
            .vendor_id
        )
        print(f"From DB: {START_DATE}")

    END_DATE = get_utc_time()
    START_DATE = START_DATE.replace(hour=0, minute=0, second=0, microsecond=0)
    print(f"Start Date: {START_DATE}, End Date: {END_DATE}")
    response = main(START_DATE, END_DATE, BBOX, last_scene_id, False)
    return response

def run_blacksky_catalog_bulk_api():
    BBOX = "-180,-90,180,90"
    START_DATE = datetime(2021, 1, 1, tzinfo=pytz.utc)
    END_LIMIT = datetime(2021, 1, 2, tzinfo=pytz.utc)

    import time
    while START_DATE < END_LIMIT:
        END_DATE = min(START_DATE + timedelta(days=1), END_LIMIT)
        print(f"Start Date: {START_DATE}, End Date: {END_DATE}")
        month_start_time = time.time()
        response = main(START_DATE, END_DATE, BBOX, None, True)
        month_end_time = time.time()
        print(f"Time taken to process the interval: {month_end_time - month_start_time}")

        START_DATE = END_DATE
    return response

def run_blacksky_catalog_bulk_api_for_last_35_days_from_now():
    BBOX = "-180,-90,180,90"
    START_DATE = (datetime.now(pytz.utc) - timedelta(days=35)).replace(hour=0, minute=0, second=0, microsecond=0)
    END_LIMIT = datetime.now(pytz.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    import time
    while START_DATE < END_LIMIT:
        END_DATE = min(START_DATE + timedelta(days=1), END_LIMIT)
        print(f"Start Date: {START_DATE}, End Date: {END_DATE}")
        month_start_time = time.time()
        response = main(START_DATE, END_DATE, BBOX, None, True)
        month_end_time = time.time()
        print(f"Time taken to process the interval: {month_end_time - month_start_time}")
        time.sleep(5)
        START_DATE = END_DATE
    return "Blacksky 35 days bulk processing completed"

# from core.services.blacksky_catalog_api import run_blacksky_catalog_bulk_api
# run_blacksky_catalog_bulk_api()

# from core.services.blacksky_catalog_api import run_blacksky_catalog_bulk_api_for_last_35_days_from_now
# run_blacksky_catalog_bulk_api_for_last_35_days_from_now()