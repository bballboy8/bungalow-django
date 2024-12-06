import requests
from datetime import datetime, timedelta
import logging
from dateutil import parser
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

# Get the terminal size
columns = shutil.get_terminal_size().columns


API_KEY = config("AIRBUS_API_KEY")

GEOHASH = "w"
ITEMS_PER_PAGE = 50
START_PAGE = 1
BATCH_SIZE = 28


def get_acces_token():
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = [
        ("apikey", API_KEY),
        ("grant_type", "api_key"),
        ("client_id", "IDP"),
    ]

    # Authenticate and obtain the access token
    auth_response = requests.post(
        "https://authenticate.foundation.api.oneatlas.airbus.com/auth/realms/IDP/protocol/openid-connect/token",
        headers=headers,
        data=data,
    )

    if auth_response.status_code == 200:
        access_token = auth_response.json().get("access_token")
        return access_token


access_token = get_acces_token()


def calculate_withhold_time(acquisition_date, publication_date):
    """Calculate the withhold time as total hours and human-readable format."""
    acq_date = parser.isoparse(acquisition_date)
    pub_date = parser.isoparse(publication_date)
    delta = pub_date - acq_date
    total_hours = int(delta.total_seconds() / 3600)  # convert to hours
    days = delta.days
    hours = delta.seconds // 3600
    return f"{days} days {hours} hours", total_hours


def sanitize_value(value):
    """Ensure values are suitable for GeoJSON by converting them to strings if necessary, except for None."""
    if isinstance(value, (int, float)):
        return str(value)
    if value is None:
        return None
    return value


def format_datetime(datetime_str):
    """Format datetime string to 'YYYY-MM-DD HH:MM:SS.xx'."""
    try:
        dt = parser.isoparse(datetime_str)
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[
            :-4
        ]  # Truncate to two decimal places
    except (ValueError, TypeError):
        return datetime_str


def format_float(value, precision=2):
    """Format float to a string with the given precision."""
    try:
        return f"{float(value):.{precision}f}"
    except (ValueError, TypeError):
        return None

def process_features(all_features):
    thumbnail_urls = []
    converted_features = []
    for feature in all_features:
        try:
            properties = feature.get("properties", {})
            geometry = feature.get("geometry", {})
            model_params = {
                "acquisition_datetime": datetime.fromisoformat(
                    properties.get("acquisitionDate").replace("Z", "+00:00")
                ).replace(microsecond=0),
                "cloud_cover": properties.get("cloudCover", ""),
                "vendor_id": properties.get("id"),
                "vendor_name": "airbus",
                "sensor": properties.get("sensorType"),
                "area": calculate_area_from_geojson(geometry, properties.get("id")),
                "type": (
                    "Day"
                    if 6
                    <= datetime.fromisoformat(
                        properties.get("acquisitionDate").replace("Z", "+00:00")
                    ).hour
                    <= 18
                    else "Night"
                ),
                "sun_elevation": properties.get("azimuthAngle"),
                "resolution": f"{properties.get("resolution")}m",
                "location_polygon": geometry,
                "coordinates_record": geometry,
            }
            converted_features.append(model_params)
            download_thumbnails_dict = {
                "url": feature.get("_links", {}).get("thumbnail", {}).get("href"),
                "id": feature.get("properties").get("id"),
                "geometry": feature.get("geometry"),
            }
            thumbnail_urls.append(download_thumbnails_dict)
        except Exception as e:
            logging.error(f"Failed to process feature: {e}")
            pass
    return converted_features, thumbnail_urls

def upload_to_s3(feature, folder="thumbnails"):
    """Downloads an image from the URL in the feature and uploads it to S3."""
    try:
        headers = {"Authorization": "Bearer " + access_token}
        url = feature.get("url")
        response = requests.get(url, headers=headers, stream=True, timeout=(10, 30))
        response.raise_for_status()
        filename = feature.get("id")
        content = response.content
        response_url = save_image_in_s3_and_get_url(content, filename, folder)
        # response_geotiff = geotiff_conversion_and_s3_upload(
        #     content, filename, "airbus/geotiffs", feature.get("geometry")
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

def download_and_upload_images(images, path, max_workers=5):
    """Download images from URLs in images and upload them to S3."""

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(upload_to_s3, feature, path): feature
            for feature in images
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

def process_database_catalog(features, start_time, end_time, batch_size=100):
    valid_features = []
    invalid_features = []

    for feature in features:
        serializer = SatelliteCaptureCatalogSerializer(data=feature)
        if serializer.is_valid():
            valid_features.append(serializer.validated_data)
        else:
            print(f"Error in serializer: {serializer.errors}")
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
            "vendor_name": "airbus",
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



def airbus_catalog_api(bbox, start_date, end_date, current_page):
    try:
        search_headers = {
            "Authorization": f"Bearer {access_token}",
            "Cache-Control": "no-cache",
        }
        SEARCH_API_ENDPOINT = (
            "https://search.foundation.api.oneatlas.airbus.com/api/v2/opensearch"
        )
        body = {
            "acquisitionDate": f"[{start_date.replace("+00:00", ".000Z")},{end_date.replace("+00:00", ".000Z")}]",
            "itemsPerPage": ITEMS_PER_PAGE,
            "startPage": current_page,
            "bbox": bbox,
        }
        response = requests.post(SEARCH_API_ENDPOINT, json=body, headers=search_headers)
        if response.status_code == 200:
            response_data = response.json()
            return response_data
    except Exception as e:
        logging.error(f"Failed to fetch images: {e}")
        return False


def search_images(bbox, start_date, end_date):
    """Search for images in the Airbus OneAtlas catalog."""
    all_features = []
    if access_token:
        current_date = start_date
        global BATCH_SIZE
        date_difference = (end_date - current_date).days + 1
        if date_difference < BATCH_SIZE:
            BATCH_SIZE = date_difference
        duration = math.ceil(date_difference / BATCH_SIZE)
        print("Batch Size: ", BATCH_SIZE, ", days: ", date_difference)
        print("Duration :", duration, "batch")
        total_items = 0
        while current_date <= end_date:
            current_page = START_PAGE
            while True:
                start_date_str = current_date.isoformat()
                if (end_date - current_date).days > 1 > 1:
                    end_date_str = (
                        current_date + timedelta(days=BATCH_SIZE)
                    ).isoformat()
                else:
                    end_date_str = end_date.isoformat()

                response_data = airbus_catalog_api(
                    bbox, start_date_str, end_date_str, current_page
                )
                if response_data:
                    all_features.extend(response_data.get("features", []))
                    if response_data.get("totalResults", 0) <= (current_page * ITEMS_PER_PAGE):
                        total_items += response_data.get("totalResults", 0)
                        break
                else:
                    break
                current_page += 1
            current_date += timedelta(days=BATCH_SIZE)
        
        data, images = process_features(all_features)
        download_and_upload_images(images, "airbus/thumbnails")
        process_database_catalog(data, start_date.isoformat(), end_date.isoformat())
        print("Completed Processing Airbus: Total Items: {}".format(total_items))
    else:
        logging.error(f"Failed to authenticate")
        pass


def run_airbus_catalog_api():
    BBOX = "-180,-90,180,90"
    START_DATE = (
        SatelliteDateRetrievalPipelineHistory.objects.filter(vendor_name="airbus")
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
    response = search_images(BBOX, START_DATE, END_DATE)
    return response
