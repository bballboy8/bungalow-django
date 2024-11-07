import requests
import geohash2
from datetime import datetime, timedelta
import csv
import io
import time
import logging
import json
from dateutil import parser
import argparse
import os
from tqdm import tqdm
import math
import shutil
from utils import check_csv_and_rename_output_dir, download_thumbnails, process_geojson
from decouple import config

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

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


def geohash_to_bbox(geohash):
    """Convert geohash to bounding box."""
    lat, lon, lat_err, lon_err = geohash2.decode_exactly(geohash)
    lat_min = lat - lat_err
    lat_max = lat + lat_err
    lon_min = lon - lon_err
    lon_max = lon + lon_err
    return lon_min, lat_min, lon_max, lat_max


def calculate_withhold_time(acquisition_date, publication_date):
    """Calculate the withhold time as total hours and human-readable format."""
    acq_date = parser.isoparse(acquisition_date)
    pub_date = parser.isoparse(publication_date)
    delta = pub_date - acq_date
    total_hours = int(delta.total_seconds() / 3600)  # convert to hours
    days = delta.days
    hours = delta.seconds // 3600
    return f"{days} days {hours} hours", total_hours


def latlon_to_geohash(lat, lon, range_km):
    # Map the range to geohash precision
    precision = (
        2
        if range_km > 100
        else 4 if range_km > 20 else 6 if range_km > 5 else 8 if range_km > 1 else 10
    )
    return geohash2.encode(lat, lon, precision=precision)


def sanitize_value(value):
    """Ensure values are suitable for GeoJSON by converting them to strings if necessary, except for None."""
    if isinstance(value, (int, float)):
        return str(value)
    if value is None:
        return None  # Keep None as is to maintain distinction in outputs
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


def process_features(response_data, csv_writer, geojson_features):
    features = response_data.get("features", [])

    thumbnail_urls = []
    for feature in features:
        try:
            download_thumbnails_dict = {
                "url": feature.get("_links",{}).get("thumbnail",{}).get("href"),
                "id": feature.get("properties").get("id"),
                "geometry": feature.get("geometry"),
            }
            if not download_thumbnails_dict["url"]:
                continue
            thumbnail_urls.append(download_thumbnails_dict)
            properties = feature.get("properties", {})
            geometry = feature.get("geometry", {})

            # Format dates and numeric values
            acquisition_date = format_datetime(properties.get("acquisitionDate", ""))
            publication_date = format_datetime(properties.get("publicationDate", ""))
            withhold_readable, withhold_hours = calculate_withhold_time(
                properties.get("acquisitionDate", ""), properties.get("publicationDate", "")
            )

            incidence_angle = format_float(properties.get("incidenceAngle", ""), 2)
            azimuth_angle = format_float(properties.get("azimuthAngle", ""), 2)

            # Sanitize values for CSV output
            csv_writer.writerow(
                [
                    properties.get("acquisitionIdentifier", ""),
                    json.dumps(geometry),
                    acquisition_date,
                    publication_date,
                    properties.get("platform", ""),
                    properties.get("sensorType", ""),
                    properties.get("resolution", ""),
                    properties.get("constellation", ""),
                    properties.get("cloudCover", ""),
                    incidence_angle,
                    azimuth_angle,
                    withhold_readable,
                    withhold_hours,
                ]
            )

            # Add properties back with formatted angles and withhold
            geojson_feature = {
                "id": properties.get("id", ""),
                "type": "Feature",
                "geometry": geometry,
                "properties": {
                    "acquisitionIdentifier": sanitize_value(
                        properties.get("acquisitionIdentifier", "")
                    ),
                    "acquisitionDate": acquisition_date,
                    "publicationDate": publication_date,
                    "productPlatform": sanitize_value(properties.get("platform", "")),
                    "sensorType": sanitize_value(properties.get("sensorType", "")),
                    "resolution": sanitize_value(properties.get("resolution", "")),
                    "constellation": sanitize_value(properties.get("constellation", "")),
                    "cloudCover": sanitize_value(properties.get("cloudCover", "")),
                    "incidenceAngle": incidence_angle,
                    "azimuthAngle": azimuth_angle,
                    "withholdReadable": withhold_readable,
                    "withholdHours": withhold_hours,
                },
            }
            geojson_features.append(geojson_feature)
        except Exception as e:
            logging.error(f"Failed to process feature: {e}")
            pass
    download_thumbnails(thumbnail_urls, OUTPUT_THUMBNAILS_FOLDER, OUTPUT_GEOTIFF_FOLDER, access_token)
    process_geojson(geojson_features, OUTPUT_GEOJSON_FOLDER)


def search_images(
    api_key,
    bbox,
    start_date,
    end_date,
    output_csv_file=None,
    output_geojson_file=None,
    lat=None,
    lon=None,
    OUTPUT_DIR=None,
):
    """Search for images in the Airbus OneAtlas catalog."""
    if access_token:
        # Set up headers for the search request
        search_headers = {
            "Authorization": f"Bearer {access_token}",
            "Cache-Control": "no-cache",
        }

        # Prepare CSV output
        csv_output = io.StringIO()
        csv_writer = csv.writer(csv_output, quoting=csv.QUOTE_ALL)

        # Write the header row
        csv_writer.writerow(
            [
                "acquisitionIdentifier",
                "geometry",
                "acquisitionDate",
                "publicationDate",
                "productPlatform",
                "sensorType",
                "resolution",
                "constellation",
                "cloudCover",
                "incidenceAngle",
                "azimuthAngle",
                "withholdReadable",
                "withholdHours",
            ]
        )

        # Prepare a list to hold all GeoJSON features
        geojson_features = []

        # Iterate through each day in the date range
        current_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
        global BATCH_SIZE
        date_difference = (end_date - current_date).days + 1
        if date_difference < BATCH_SIZE:
            BATCH_SIZE = date_difference
        duration = math.ceil(date_difference / BATCH_SIZE)

        print("-" * columns)
        description = f"Processing Airbus Catalog\nDates: {current_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} \n lat: {lat} and lon: {lon} \n Range: {RANGE} \nOutput Directory: {OUTPUT_DIR}"
        print(description)

        print("-" * columns)
        print("Batch Size: ", BATCH_SIZE, ", days: ", date_difference)
        print("Duration :", duration, "batch")

        total_items = 0
        with tqdm(total=duration, desc="", unit="batch") as pbar:
            while current_date <= end_date:
                current_page = START_PAGE
                while True:
                    start_date_str = current_date.strftime("%Y-%m-%dT00:00:00.000Z")
                    end_time = (current_date + timedelta(days=BATCH_SIZE))
                    end_date_str = end_time.strftime("%Y-%m-%dT23:59:59.999Z")
                    SEARCH_API_ENDPOINT = "https://search.foundation.api.oneatlas.airbus.com/api/v2/opensearch"
                    body = {
                        "acquisitionDate" :f"[{start_date_str},{end_date_str}]" ,
                        "itemsPerPage": ITEMS_PER_PAGE,
                        "startPage": current_page,
                        "bbox": bbox,
                    }

                    response = requests.post(
                        SEARCH_API_ENDPOINT, json=body, headers=search_headers
                    )
                    if response.status_code == 200:
                        response_data = response.json()
                        total_records = response_data.get("totalResults", 0)
                        total_items += total_records
                        
                        process_features(
                            response_data,
                            csv_writer,
                            geojson_features,
                        )
                        if total_records <= current_page * ITEMS_PER_PAGE:
                            break
                    else:
                        break
                    current_page += 1

                current_date += timedelta(days=BATCH_SIZE)

                pbar.update(1)
                pbar.refresh()

        tqdm.write("Completed Processing Airbus: Total Items: {}".format(total_items))

        with open(output_csv_file, "w", newline="") as csv_file:
            csv_file.write(csv_output.getvalue())
    else:
        logging.error(f"Failed to authenticate")
        pass


if __name__ == "__main__":
    parser_argument = argparse.ArgumentParser(description="Airbus Catalog API Executor")
    parser_argument.add_argument("--start-date", required=True, help="Start date")
    parser_argument.add_argument("--end-date", required=True, help="End date")
    parser_argument.add_argument("--lat", required=True, type=float, help="Latitude")
    parser_argument.add_argument("--long", required=True, type=float, help="Longitude")
    parser_argument.add_argument(
        "--range", required=True, type=float, help="Range value"
    )
    parser_argument.add_argument("--output-dir", required=True, help="Output directory")
    parser_argument.add_argument("--bbox", required=True, help="Bounding box")

    args = parser_argument.parse_args()
    START_DATE = args.start_date
    END_DATE = args.end_date

    OUTPUT_DIR = args.output_dir + f"/airbus/{START_DATE}_{END_DATE}"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    OUTPUT_THUMBNAILS_FOLDER = f"{OUTPUT_DIR}/thumbnails"
    os.makedirs(OUTPUT_THUMBNAILS_FOLDER, exist_ok=True)

    OUTPUT_GEOJSON_FOLDER = f"{OUTPUT_DIR}/geojsons"
    os.makedirs(OUTPUT_GEOJSON_FOLDER, exist_ok=True)

    OUTPUT_GEOTIFF_FOLDER = f"{OUTPUT_DIR}/geotiffs"
    os.makedirs(OUTPUT_GEOTIFF_FOLDER, exist_ok=True)

    RANGE = int(args.range)
    LAT, LON = args.lat, args.long

    BBOX = args.bbox.replace("t", "-")
    print(f"Generated BBOX: {BBOX}")

    OUTPUT_CSV_FILE = f"{OUTPUT_DIR}/output_airbus.csv"
    OUTPUT_GEOJSON_FILE = f"{OUTPUT_DIR}/output_airbus.geojson"
    search_images(
        API_KEY,
        BBOX,
        args.start_date,
        args.end_date,
        OUTPUT_CSV_FILE,
        OUTPUT_GEOJSON_FILE,
        LAT,
        LON,
        OUTPUT_DIR,
    )

    check_csv_and_rename_output_dir(
        OUTPUT_DIR, START_DATE, END_DATE, args.output_dir, "airbus"
    )
