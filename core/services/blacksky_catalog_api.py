import requests
from datetime import datetime, timedelta
from tqdm import tqdm
import math
import shutil
from decouple import config
from core.serializers import SatelliteCaptureCatalogSerializer
from datetime import datetime
from django.contrib.gis.geos import Polygon

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
    END_DATE = "2024-10-26"
    main(START_DATE, END_DATE, BBOX)
