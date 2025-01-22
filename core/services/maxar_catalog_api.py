import requests
from datetime import datetime, timedelta
import os
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil
from decouple import config
from bungalowbe.utils import get_utc_time, convert_iso_to_datetime
from django.db.utils import IntegrityError
from core.models import SatelliteCaptureCatalog, SatelliteDateRetrievalPipelineHistory
from core.serializers import SatelliteDateRetrievalPipelineHistorySerializer, SatelliteCaptureCatalogSerializer, SatelliteCaptureCatalogMetadata
import pytz
from core.services.utils import calculate_area_from_geojson
from core.utils import save_image_in_s3_and_get_url
from botocore.exceptions import NoCredentialsError
from PIL import Image
import io

# Get the terminal size
columns = shutil.get_terminal_size().columns

# Configuration
AUTH_TOKEN = config("MAXAR_API_KEY")
MAXAR_BASE_URL = "https://api.maxar.com/discovery/v1"
MAX_THREADS = 9
BATCH_SIZE = 28


def get_maxar_collections(
    limit=100,
    page=1,
    bbox=None,
    datetime_range=None,
):
    """
    Fetches collections from the Maxar API.
    """
    collections = [ "ge01", "wv01", "wv02", "wv03-vnir", "wv04", "lg01", "lg02"]
    collections_str = ",".join(collections)
    url = f"https://api.maxar.com/discovery/v1/search?collections={collections_str}&bbox={bbox}&datetime={datetime_range}&limit={limit}&page={page}&sortby=+properties.datetime"

    headers = {"Accept": "application/json", "MAXAR-API-KEY": AUTH_TOKEN}
    try:
        response = requests.request("GET", url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {response.text}")

    return None


def process_features(all_features):
    converted_features = []
    for feature in all_features:
        try:
            properties = feature.get("properties", {})
            geometry = feature.get("geometry", {})
            assets = feature.get("assets", {})
            model_params = {
                "acquisition_datetime": datetime.fromisoformat(
                    properties.get("datetime").replace("Z", "+00:00")
                ).replace(microsecond=0),
                "cloud_cover": properties.get("eo:cloud_cover", ""),
                "vendor_id": f"{feature.get('id')}-{feature.get('collection')}",
                "vendor_name": "maxar",
                "sensor": properties.get("instruments")[0] if properties.get("instruments") and len(properties.get("instruments")) > 0 else None,
                "area": calculate_area_from_geojson(geometry, properties.get("id")),
                "type": (
                    "Day"
                    if 6
                    <= datetime.fromisoformat(
                        properties.get("datetime").replace("Z", "+00:00")
                    ).hour
                    <= 18
                    else "Night"
                ),
                "sun_elevation": properties.get("view:sun_azimuth"),
                "resolution": f"{properties.get("pan_resolution_avg")}m",
                "location_polygon": geometry,
                "coordinates_record": geometry,
                "assets": assets,
                "metadata": feature,
                "gsd": float(properties.get("pan_resolution_avg")),
            }
            converted_features.append(model_params)
        except Exception as e:
            pass
    # sort by acquisition_datetime
    converted_features = sorted(
        converted_features, key=lambda x: x["acquisition_datetime"], reverse=True
    )
    return converted_features


def upload_to_s3(feature, folder="thumbnails"):
    """Downloads an image from the URL in the feature and uploads it to S3."""
    try:
        headers = {"Accept": "application/json", "MAXAR-API-KEY": AUTH_TOKEN}
        url = feature.get("assets", {}).get("browse", {}).get("href")
        filename = feature.get("vendor_id").split("-")[0]
        url = "https://api.maxar.com/browse-archive/v1/browse/show?image_id=" + filename
        response = requests.get(url, headers=headers, stream=True, timeout=(10, 30))
        response.raise_for_status()
        tif_content = response.content
        save_image_in_s3_and_get_url(tif_content, filename, folder , "tif")
        url = ""
        with Image.open(io.BytesIO(tif_content)) as img:
            png_buffer = io.BytesIO()
            img.save(png_buffer, format="PNG")
            png_buffer.seek(0)
            url = save_image_in_s3_and_get_url(
                png_buffer.getvalue(), filename, folder , "png"
            )
        return url

    except requests.exceptions.RequestException as e:
        print(f"Failed to download {url}: {e}")
        return False
    except NoCredentialsError:
        print("S3 credentials not available")
        return False
    except Exception as e:
        print(f"Failed to upload {url}: {e}")
        return False


def download_thumbnails(features, path):
    """Download and save thumbnail images for the given features."""

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {
            executor.submit(upload_to_s3, feature, path): feature for feature in features
        }

        for future in as_completed(futures):
            feature = futures[future]
            try:
                result = future.result()
                if result:
                    # print(f"Successfully downloaded thumbnail for feature {feature.get('id')}")
                    pass
                else:
                    # print(f"Failed to download thumbnail for feature {feature.get('id')}")
                    pass
            except Exception as e:
                # print(f"Exception occurred while downloading thumbnail for feature {feature.get('id')}: {e}")
                pass


def fetch_and_process_records(bbox, start_time, end_time):
    """Fetches records from the Maxar API and processes them."""
    page = 1
    all_features = []

    while True:
        records = get_maxar_collections(
            bbox=bbox, datetime_range=f"{start_time}/{end_time}", page=page
        )
        if not records:
            break

        features = records.get("features", [])
        all_features.extend(features)

        if not any(link.get("rel") == "next" for link in records.get("links", [])):
            break

        page += 1

    return all_features

def process_database_catalog(features, start_time, end_time):
    valid_features = []
    invalid_features = []
    metadata = []

    for feature in features:
        serializer = SatelliteCaptureCatalogSerializer(data=feature)
        if serializer.is_valid():
            valid_features.append(serializer.validated_data)
        else:
            print(f"Error in serializer: {serializer.errors}")
            invalid_features.append(feature)
        metadata.append(
            {
                "vendor_name": feature["vendor_name"],
                "vendor_id": feature["vendor_id"],
                "acquisition_datetime": feature["acquisition_datetime"],
                "metadata": feature["metadata"],
            }
        )
    
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
            "vendor_name": "maxar",
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

def main(START_DATE, END_DATE, BBOX):
    bboxes = [BBOX]
    current_date = START_DATE
    end_date = END_DATE
    global BATCH_SIZE

    date_difference = (end_date - current_date).days + 1  # Inclusive of end_date
    if date_difference < BATCH_SIZE:
        BATCH_SIZE = date_difference
    duration = math.ceil(date_difference / BATCH_SIZE)

    print("-" * columns)
    print("Batch Size: ", BATCH_SIZE, ", days: ", date_difference)
    print("Duration :", duration, "batch")
    all_features = []

    while current_date <= end_date:
        start_time = current_date
        # Format like this : 2020-01-02T18:01:15.140202Z
        start_time = start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if (end_date - current_date).days > 1:
            end_time = (current_date + timedelta(days=BATCH_SIZE))
        else:
            end_time = end_date
        end_time = end_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        
        print(f"Start Time: {start_time}, End Time: {end_time} Running...")

        for bbox in bboxes:
            response = fetch_and_process_records(bbox, start_time, end_time)
            print(f"Total records: {len(response)}")
            if response:
                all_features.extend(response)

        current_date += timedelta(days=BATCH_SIZE)

    converted_features = process_features(all_features)
    print(f"Total records: {len(all_features)}, Converted records: {len(converted_features)}")
    # download_thumbnails(converted_features, "maxar/thumbnails")
    process_database_catalog(converted_features, START_DATE.isoformat(), END_DATE.isoformat())
    print("Completed", len(converted_features))


def run_maxar_catalog_api():
    BBOX = "-180,-90,180,90"
    START_DATE = (
        SatelliteDateRetrievalPipelineHistory.objects.filter(vendor_name="maxar")
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

def run_maxar_catalog_bulk_api():
    BBOX = "-180,-90,180,90"
    START_DATE = datetime(2024, 12, 6, tzinfo=pytz.utc)
    END_LIMIT = datetime(2024, 12, 14, tzinfo=pytz.utc)

    import time
    while START_DATE < END_LIMIT:
        END_DATE = min(START_DATE + timedelta(days=1), END_LIMIT)
        print(f"Start Date: {START_DATE}, End Date: {END_DATE}")
        month_start_time = time.time()
        
        response =  main(START_DATE, END_DATE, BBOX)
        month_end_time = time.time()
        print(f"Time taken to process the interval: {month_end_time - month_start_time}")

        START_DATE = END_DATE
    return response

# from core.services.maxar_catalog_api import run_maxar_catalog_bulk_api
# run_maxar_catalog_bulk_api()