import time
import logging
import geohash2
from datetime import datetime
import os
import concurrent.futures
from shapely.geometry import shape
from datetime import datetime, timedelta
import geojson
import pandas as pd
import json
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from pprint import pprint

# Configuration
START_DATE = "2024-01-01"  # Start date (YYYY-MM-DD)
END_DATE = "2025-02-03"  # End date (YYYY-MM-DD)


GEOHASH = [
    # 'w',  # China

    # '8', 'x', '2',   # Pacific
    #
    # 't',  # Middle East

    'd', '9', 'c', 'b',  # USA

    'u', 'g', 'f',  # Europe

    'v', 'y', 'z',  # Russia
    #
    # '4', '6', '7',  # South America
    #
    # 'e', 's', 'k', 'm',  # Africa
    #
    # 'q', 'r', 'p',  # Australia and Oceania
    #
    # '0', '1', '3', '5', 'h', 'j', 'n'  # Rest of the World

]  # List of geohashes to process


DB_NAME = "satellites_master"
DB_USER = "postgres"
DB_PASSWORD = "trees"
DB_HOST = "71.126.151.162"
DB_PORT = "5432"


# Set up logging with format including custom service name
logger = logging.getLogger("Sanitizer")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Limit the number of threads for NumExpr (if used in any libraries)
os.environ['NUMEXPR_MAX_THREADS'] = '8'


# Function to calculate the centroid of a GeoJSON polygon
def calculate_centroid(geometry):
    """Calculate the centroid of a GeoJSON Polygon and return lat and lon."""
    if geometry.get("type") == "Polygon":
        polygon_shape = shape(geometry)
        centroid = polygon_shape.centroid
        return centroid.y, centroid.x  # Return as (lat, lon)
    return None, None  # Return None if geometry is not a valid polygon


# Helper Functions
def format_datetime(iso_string):
    """Format ISO datetime string to 'YYYY-MM-DD HH:MM:SS'."""
    if iso_string.endswith("Z"):
        iso_string = iso_string.replace("Z", "+00:00")
    if "." in iso_string:
        main_part, frac_part = iso_string.split(".")
        if "+" in frac_part or "-" in frac_part:
            iso_string = main_part + frac_part[frac_part.index("+"):] if "+" in frac_part else main_part + frac_part[
                                                                                                           frac_part.index(
                                                                                                               "-"):]
        else:
            iso_string = main_part
    if "+" in iso_string[19:] or "-" in iso_string[19:]:
        iso_string = iso_string[:iso_string.find("+", 19)] if "+" in iso_string[19:] else iso_string[
                                                                                          :iso_string.find("-", 19)]
        iso_string += "+00:00"
    try:
        dt = datetime.fromisoformat(iso_string)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        raise ValueError(f"Invalid isoformat string: {iso_string}")


def normalize_cloud_cover(service, cloud_cover):
    """Normalize cloud cover to a numeric value or 'SAR' for SAR services."""
    if cloud_cover is None:
        return -99  # Indicate missing value
    try:
        if service.lower() == 'planet':
            return int(round(float(cloud_cover) * 100))
        elif service.lower() in ['capella', 'skyfi', 'umbra']:
            return -1  # SAR imagery
        else:
            return int(round(float(cloud_cover)))
    except (ValueError, TypeError):
        return -99  # Handle invalid values


def format_to_two_decimals(value):
    """Convert value to two decimal places or return None if invalid."""
    try:
        return round(float(value), 2)
    except (ValueError, TypeError):
        return None  # Use NULL for invalid values




def normalize_airbus(record):
    """Normalize Airbus-specific properties, ensuring all required fields are included and consistent formats, including GeoJSON footprint."""
    properties = record.get('properties', {})
    geometry = record.get('geometry', {})

    # Set current date as the collection date
    collection_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    normalized = {}

    # Service name
    normalized["service"] = "airbus"

    # Acquisition date
    acq_date = properties.get('acqDate', "")
    normalized['m_datetime'] = format_datetime(acq_date) if acq_date else ""
    if not acq_date:
        logging.warning("Missing acquisition date in Airbus record.")

    # Acquisition ID
    normalized['m_imageid'] = str(properties.get('acqId', ""))

    # Off-nadir angle (normalized as offnadirangle)
    normalized['offnadir'] = format_to_two_decimals(properties.get('incidenceAngle', ""))

    # Cloud cover
    normalized['m_cloud'] = normalize_cloud_cover('airbus', properties.get('cloudCover', None))

    # Constellation
    normalized['m_const'] = properties.get('constellation', "")

    # Platform
    normalized['m_platform'] = properties.get('platform', "")

    # Ground Sampling Distance (resolution)
    normalized['m_gsd'] = format_to_two_decimals(properties.get('resolution', ""))

    # Satellite Azimuth Angle (angle)
    normalized['azimuthAngle'] = format_to_two_decimals(properties.get('azAngle', ""))
    # print(properties.get('azAngle'))

    # Sun Azimuth Angle (angle)
    normalized['illuminationAzimuthAngle'] = format_to_two_decimals(properties.get('sun_angle', ""))
    # print(properties.get('sun_angle'))

    # Sun Elevation (angle)
    normalized['illuminationElevationAngle'] = format_to_two_decimals(properties.get('sun_elev', ""))
    # print(properties.get('sun_elev'))

    # Collection date (set to the date when the script is run)
    normalized["collection_date"] = collection_date

    # Geometry centroid coordinates
    if 'geometryCentroid' in properties:
        centroid = properties['geometryCentroid']
        normalized["geometryCentroid_lat"] = centroid.get("lat", "")
        normalized["geometryCentroid_lon"] = centroid.get("lon", "")
    else:
        normalized["geometryCentroid_lat"] = ""
        normalized["geometryCentroid_lon"] = ""

    normalized["holdback"] = None

    # Footprint GeoJSON - ensure both type and coordinates keys are present
    if geometry and geometry.get("type") == "Polygon" and "coordinates" in geometry:
        normalized['footprint_geojson'] = geojson.dumps(geometry)
    else:
        normalized['footprint_geojson'] = ""

    return normalized


def normalize_blacksky(feature):
    """Normalize BlackSky-specific properties, ensuring all required fields are included and consistent formatting."""
    properties = feature.get('properties', {})
    geometry = feature.get('geometry', {})
    image_id = feature.get('id', "")

    # Get the current date as the collection date in YYYY-MM-DD format
    collection_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    normalized = {}

    # Specified field order
    normalized["service"] = "blacksky"  # Service name

    # Acquisition datetime
    normalized['m_datetime'] = format_datetime(properties.get('datetime', ""))


    # Image ID from feature level, not properties
    normalized['m_imageid'] = str(image_id) if image_id else ""

    # Cloud cover - use normalize_cloud_cover for consistent formatting
    normalized['m_cloud'] = normalize_cloud_cover('blacksky', properties.get('cloudPercent', None))

    # Sensor ID (used as constellation)
    normalized['m_const'] = str(image_id)[:7] if image_id else ""

    # Vendor ID (used as platform)
    normalized['m_platform'] = properties.get('vendorId', "")

    # Off-nadir angle (normalized as offnadirangle)
    normalized['offnadir'] = format_to_two_decimals(properties.get('offNadirAngle', ""))

    # Ground Sampling Distance (gsd)
    normalized['m_gsd'] = format_to_two_decimals(properties.get('gsd', ""))

    # Satellite Azimuth Angle (angle)
    normalized['azimuthAngle'] = None

    # Sun Azimuth Angle (angle)
    normalized['illuminationAzimuthAngle'] = format_to_two_decimals(properties.get('sunAzimuth', ""))

    # Sun Elevation (angle)
    normalized['illuminationElevationAngle'] = None

    normalized["holdback"] = None

    # Collection date (set to the date when the script is run)
    normalized["collection_date"] = collection_date

    # Calculate and set geometry centroid coordinates
    lat, lon = calculate_centroid(geometry)
    normalized["geometryCentroid_lat"] = lat if lat is not None else ""
    normalized["geometryCentroid_lon"] = lon if lon is not None else ""

    # Footprint GeoJSON - ensure both type and coordinates keys are present
    if geometry and geometry.get("type") == "Polygon" and "coordinates" in geometry:
        normalized['footprint_geojson'] = geojson.dumps(geometry)
    else:
        normalized['footprint_geojson'] = ""

    return normalized


def normalize_capella(record):
    """Normalize Capella-specific properties, ensuring all required fields are included and consistent formatting."""
    properties = record.get('properties', {})
    geometry = record.get('geometry', {})

    normalized = {}

    # Service name
    normalized["service"] = "capella"  # Fixed service name for Capella

    # Acquisition datetime
    acq_datetime = properties.get('datetime', "")
    normalized['m_datetime'] = format_datetime(acq_datetime) if acq_datetime else ""
    if not acq_datetime:
        logging.warning("Missing acquisition datetime in Capella record.")

    # Holdback assessment
    normalized['holdback_assessment'] = "vendor"

    # Acquisition ID
    normalized['m_imageid'] = str(properties.get('capella:collect_id', ""))

    # Off-nadir angle (normalized as offnadirangle)
    normalized['offnadir'] = format_to_two_decimals(properties.get('view:look_angle', ""))

    # Cloud cover - SAR designation
    normalized['m_cloud'] = normalize_cloud_cover('capella', properties.get('cloudPercent', 'SAR'))

    # Constellation and platform
    normalized['m_const'] = properties.get('constellation', "")
    normalized['m_platform'] = properties.get('instruments', "")

    # Ground Sampling Distance (resolution)
    normalized['m_gsd'] = format_to_two_decimals(properties.get('capella:resolution_ground_range', ""))

    # Collection date (set to the date when the script is run)
    normalized["collection_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Satellite Azimuth Angle (angle)
    normalized['azimuthAngle'] = None

    # Sun Azimuth Angle (angle)
    normalized['illuminationAzimuthAngle'] = None

    # Sun Elevation (angle)
    normalized['illuminationElevationAngle'] = None

    normalized["holdback"] = None

    # Geometry centroid coordinates
    lat, lon = calculate_centroid(geometry)
    normalized["geometryCentroid_lat"] = lat if lat is not None else ""
    normalized["geometryCentroid_lon"] = lon if lon is not None else ""

    # Footprint GeoJSON
    if geometry and geometry.get("type") == "Polygon" and "coordinates" in geometry:
        normalized['footprint_geojson'] = geojson.dumps(geometry)
    else:
        normalized['footprint_geojson'] = ""

    return normalized


def normalize_maxar(feature):
    """Normalize Maxar-specific properties, ensuring all required fields are included and consistent formatting."""
    properties = feature.get('properties', {})
    geometry = feature.get('geometry', {})

    # Get the current date as the collection date in YYYY-MM-DD format
    collection_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    normalized = {}

    # Service name
    normalized["service"] = "maxar"  # Fixed service name for Maxar

    # Acquisition datetime
    normalized['m_datetime'] = format_datetime(properties.get('datetime', ""))

    # Acquisition ID
    normalized['m_imageid'] = properties.get('title', "")


    # Off-nadir angle (normalized as offnadirangle)
    normalized['offnadir'] = format_to_two_decimals(properties.get('off_nadir_avg', ""))

    # Cloud cover - uses normalize_cloud_cover for consistency
    normalized['m_cloud'] = normalize_cloud_cover('maxar', properties.get('eo:cloud_cover', None))

    # Constellation
    normalized['m_const'] = properties.get('platform', "")

    # Platform
    normalized['m_platform'] = properties.get('instruments', "")

    # Ground Sampling Distance (gsd)
    normalized['m_gsd'] = format_to_two_decimals(properties.get('gsd', ""))

    # Collection date (set to the date when the script is run)
    normalized["collection_date"] = collection_date

    # Satellite Azimuth Angle (angle)
    normalized['azimuthAngle'] = format_to_two_decimals(properties.get('view:azimuth', None))

    # Sun Azimuth Angle (angle)
    normalized['illuminationAzimuthAngle'] = format_to_two_decimals(properties.get('view:sun_azimuth', None))

    # Sun Elevation (angle)
    normalized['illuminationElevationAngle'] = format_to_two_decimals(properties.get('view:sun_elevation', None))

    # Holdback
    normalized["holdback"] = None

    # Calculate and set geometry centroid coordinates
    lat, lon = calculate_centroid(geometry)
    normalized["geometryCentroid_lat"] = lat if lat is not None else ""
    normalized["geometryCentroid_lon"] = lon if lon is not None else ""

    # Footprint GeoJSON - ensure both type and coordinates keys are present
    if geometry and geometry.get("type") == "Polygon" and "coordinates" in geometry:
        normalized['footprint_geojson'] = geojson.dumps(geometry)
    else:
        normalized['footprint_geojson'] = ""

    return normalized


def normalize_planet(record):
    """Normalize Planet-specific properties, ensuring all required fields are included and consistent formatting."""
    properties = record.get('properties', {})
    geometry = record.get('geometry', {})

    # Initialize normalized dictionary
    normalized = {}

    # Set service name
    normalized["service"] = "planet"

    # Acquisition datetime
    acquired = properties.get('acquired', "")
    normalized['m_datetime'] = format_datetime(acquired) if acquired else ""
    if not acquired:
        logging.warning("Missing acquisition datetime in Planet record.")

    # Acquisition ID
    normalized['m_imageid'] = str(properties.get('strip_id', ""))

    # Off-nadir angle (normalized as offnadirangle)
    normalized['offnadir'] = format_to_two_decimals(properties.get('view_angle', ""))

    # Cloud cover
    normalized['m_cloud'] = normalize_cloud_cover('planet', properties.get('cloud_cover', None))

    # Constellation
    normalized['m_const'] = properties.get('provider', "")

    # Platform
    normalized['m_platform'] = properties.get('satellite_id', "")

    # Ground Sampling Distance (resolution)
    normalized['m_gsd'] = format_to_two_decimals(properties.get('gsd', ""))

    # Collection date (set to the date when the script is run)
    normalized["collection_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


    # Satellite Azimuth Angle (angle)
    normalized['azimuthAngle'] = format_to_two_decimals(properties.get('satellite_azimuth', ""))

    # Sun Azimuth Angle (angle)
    normalized['illuminationAzimuthAngle'] = format_to_two_decimals(properties.get('sun_azimuth', ""))

    # Sun Elevation (angle)
    normalized['illuminationElevationAngle'] = format_to_two_decimals(properties.get('sun_elevation', ""))

    normalized["holdback"] = None

    # Geometry centroid coordinates
    lat, lon = calculate_centroid(geometry)
    normalized["geometryCentroid_lat"] = lat if lat is not None else ""
    normalized["geometryCentroid_lon"] = lon if lon is not None else ""

    # Footprint GeoJSON - ensure both type and coordinates keys are present
    if geometry and geometry.get("type") == "Polygon" and "coordinates" in geometry:
        normalized['footprint_geojson'] = geojson.dumps(geometry)
    else:
        normalized['footprint_geojson'] = ""

    return normalized




def normalize_satellogic(record):
    """Normalize Satellogic-specific properties, ensuring all required fields are included and consistent formats, including GeoJSON footprint."""
    properties = record.get('properties', {})
    geometry = record.get('geometry', {})

    # Set current date as the collection date
    collection_date = datetime.now().strftime("%Y-%m-%d")
    normalized = {}

    # Service name
    normalized["service"] = "satellogic"

    # Acquisition date
    acq_date = properties.get('datetime', "")
    normalized['m_datetime'] = format_datetime(acq_date) if acq_date else ""
    if not acq_date:
        logging.warning("Missing acquisition date in Satellogic record.")

    # Acquisition ID
    normalized['m_imageid'] = str(record.get('id', ""))

    # Off-nadir angle
    normalized['offnadir'] = format_to_two_decimals(properties.get("view:off_nadir", ""))

    # Cloud cover
    normalized['m_cloud'] = normalize_cloud_cover('satellogic', properties.get("eo:cloud_cover", None))

    # Constellation
    normalized['m_const'] = properties.get('satl:product_name', "")

    # Platform
    normalized['m_platform'] = properties.get('platform', "")

    # Ground Sampling Distance (GSD)
    normalized['m_gsd'] = format_to_two_decimals(properties.get('gsd', ""))

    # Collection date (set to the date when the script is run)
    normalized["collection_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Geometry centroid coordinates
    lat, lon = calculate_centroid(geometry)
    normalized["geometryCentroid_lat"] = lat if lat is not None else ""
    normalized["geometryCentroid_lon"] = lon if lon is not None else ""

    # Footprint GeoJSON - ensure both type and coordinates keys are present
    if geometry and geometry.get("type") == "Polygon" and "coordinates" in geometry:
        normalized['footprint_geojson'] = geojson.dumps(geometry)
    else:
        normalized['footprint_geojson'] = ""

    return normalized


def normalize_skyfi(record):
    logging.info("Starting normalization")
    logging.debug(f"Input Record: {record}")

    # Extract properties and set default values
    properties = record.get('properties', {})
    footprint = record.get('footprint', None)
    collection_date = datetime.now().strftime("%Y-%m-%d")
    normalized = {}

    try:
        # Service name
        normalized["service"] = "umbra"

        # Acquisition date
        acq_date = record.get('captureTimestamp', properties.get('captureTimestamp', ""))
        normalized['m_datetime'] = format_datetime(acq_date) if acq_date else ""
        if not acq_date:
            logging.warning(f"Missing captureTimestamp in record: {record}")

        # Acquisition ID
        normalized['m_imageid'] = record.get('archiveId', properties.get('archiveId', ""))
        if not normalized['m_imageid']:
            logging.warning(f"Missing archiveId in record: {record}")

        # Off-nadir angle
        off_nadir_angle = record.get('offNadirAngle', properties.get('offNadirAngle', ""))
        normalized['offnadir'] = format_to_two_decimals(off_nadir_angle)

        # Cloud cover - SAR designation
        normalized['m_cloud'] = normalize_cloud_cover('skyfi', properties.get('cloudCoveragePercent', 'SAR'))

        # Constellation and platform
        normalized['m_const'] = record.get('constellation', properties.get('constellation', ""))
        normalized['m_platform'] = record.get('provider', properties.get('provider', ""))

        # Ground Sampling Distance (GSD)
        platform_resolution_cm = record.get('platformResolution', properties.get('platformResolution', 0.0))
        normalized['m_gsd'] = format_to_two_decimals(platform_resolution_cm / 100.0)  # Convert cm to meters

        # Collection date
        normalized["collection_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Satellite Azimuth Angle (angle)
        normalized['azimuthAngle'] = None

        # Sun Azimuth Angle (angle)
        normalized['illuminationAzimuthAngle'] = None

        # Sun Elevation (angle)
        normalized['illuminationElevationAngle'] = None

        normalized["holdback"] = None


        # Parse the footprint only when needed
        if footprint:
            try:
                import shapely.wkt
                from shapely.geometry import mapping

                geometry = mapping(shapely.wkt.loads(footprint))  # Convert WKT to GeoJSON
                lat, lon = calculate_centroid(geometry)
                normalized["geometryCentroid_lat"] = lat if lat is not None else ""
                normalized["geometryCentroid_lon"] = lon if lon is not None else ""
                normalized['footprint_geojson'] = geojson.dumps(geometry)  # Save as GeoJSON
            except Exception as e:
                logging.warning(f"Invalid WKT footprint: {footprint}. Error: {e}")
                normalized["geometryCentroid_lat"] = ""
                normalized["geometryCentroid_lon"] = ""
                normalized['footprint_geojson'] = ""
        else:
            logging.warning(f"Missing or empty footprint in record: {record}")
            normalized["geometryCentroid_lat"] = ""
            normalized["geometryCentroid_lon"] = ""
            normalized['footprint_geojson'] = ""

        return normalized

    except Exception as e:
        logging.error(f"Error during normalization: {e}")
        return None






def geohash_to_bbox(geohash):
    """Convert geohash to bounding box."""
    lat, lon, lat_err, lon_err = geohash2.decode_exactly(geohash)
    lat_min = lat - lat_err
    lat_max = lat + lat_err
    lon_min = lon - lon_err
    lon_max = lon + lon_err
    return lon_min, lat_min, lon_max, lat_max


def validate_and_adjust_bbox(bbox, logger):
    """Ensure bounding box values are within valid lat/lon ranges."""
    min_lon, min_lat, max_lon, max_lat = bbox
    if min_lat <= -90:
        min_lat = -89.99999
    if max_lat >= 90:
        max_lat = 89.99999
    if min_lon <= -180:
        min_lon = -179.99999
    if max_lon >= 180:
        max_lon = 179.99999
    logger.info(f"Adjusted bounding box: [{min_lon}, {min_lat}, {max_lon}, {max_lat}]")
    return [min_lon, min_lat, max_lon, max_lat]


def sanitize_geometry(geometry):
    """Ensure that polygons are valid by closing rings if needed."""
    coordinates = geometry.get('coordinates', [])
    if coordinates:
        for ring in coordinates:
            if ring[0] != ring[-1]:  # If the polygon is not closed
                ring.append(ring[0])  # Close the polygon
    return geometry


def clean_properties(properties):
    """Clean and sanitize properties for each feature."""
    field_name_map = {
        'acquisitionIdentifier': 'acqId',
        'acquisitionDate': 'acqDate',
        'sensorType': 'sensor',
        'azimuthAngle': 'azAngle',
        'offnadirangle': 'offnadir'
    }
    sanitized_properties = {}
    for key, value in properties.items():
        new_key = field_name_map.get(key, key)
        if value is None or value == '':
            continue
        if new_key in ['resolution', 'cloudCover', 'azAngle', 'offnadir']:
            value = round(float(value), 2)
        if new_key in ['acqDate']:
            value = value.split('.')[0] + 'Z'
        sanitized_properties[new_key] = value
    return sanitized_properties


def write_to_db(records, batch_size=500):
    """
    Insert records into the satellite_data table and handle footprint_geojson as a JSONB object.
    """
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        values = []

        # Prepare the values for batch insertion
        for record in batch:
            try:
                # Serialize footprint_geojson as JSON string
                raw_geojson = record.get("footprint_geojson", {})
                if isinstance(raw_geojson, dict):
                    record["footprint_geojson"] = json.dumps(raw_geojson)
                elif not isinstance(raw_geojson, str):
                    record["footprint_geojson"] = None  # Handle invalid formats

                value_tuple = (
                    record["service"],
                    record["m_datetime"],
                    record["m_imageid"],
                    record["m_cloud"],
                    record["m_const"],
                    record["m_platform"],
                    record.get("m_gsd"),
                    record.get("offnadir"),
                    record.get("collection_date"),
                    record.get("geometryCentroid_lat"),
                    record.get("geometryCentroid_lon"),
                    record.get("azimuthAngle"),
                    record.get("illuminationAzimuthAngle"),
                    record.get("illuminationElevationAngle"),
                    record.get("holdback"),
                    record.get("footprint_geojson"),
                )

                values.append(value_tuple)
            except (KeyError, json.JSONDecodeError) as e:
                logging.error(f"Error processing record: {record}. Error: {e}")
                continue

        # Insert query
        insert_query = """
            INSERT INTO satellite_data (
                service, m_datetime, m_imageid, m_cloud, m_const, m_platform,
                gsd, offnadir, collection_date, 
                geometryCentroid_lat, geometryCentroid_lon,
                az_angle, sun_angle, sun_elev,
                holdback, footprint_geojson
            ) VALUES %s
            ON CONFLICT (service, m_datetime, footprint_geojson_md5) DO NOTHING;

        """

        try:
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT,
            )
            cursor = conn.cursor()
            execute_values(cursor, insert_query, values)
            conn.commit()
            logging.info(f"Successfully inserted {len(values)} records.")
        except Exception as e:
            logging.error(f"Error during insertion: {e}")
        finally:
            if "cursor" in locals():
                cursor.close()
            if "conn" in locals():
                conn.close()








def generate_geohashes(seed_geohashes, child_length):
    """Generate an array of geohashes from a list of seed geohashes based on the specified child length."""
    base32_chars = '0123456789bcdefghjkmnpqrstuvwxyz'

    def generate_geohashes_recursive(current_geohash, target_length, result):
        if len(current_geohash) == target_length:
            result.append(current_geohash)
            return
        for char in base32_chars:
            next_geohash = current_geohash + char
            generate_geohashes_recursive(next_geohash, target_length, result)

    result = []
    for seed_geohash in seed_geohashes:
        generate_geohashes_recursive(seed_geohash, len(seed_geohash) + child_length, result)
    return result



def geohash_to_bbox(geohash):
    """Convert geohash to bounding box."""
    lat, lon, lat_err, lon_err = geohash2.decode_exactly(geohash)
    lat_min = lat - lat_err
    lat_max = lat + lat_err
    lon_min = lon - lon_err
    lon_max = lon + lon_err
    return lon_min, lat_min, lon_max, lat_max


def validate_and_adjust_bbox(bbox, logger):
    """Ensure bounding box values are within valid lat/lon ranges."""
    min_lon, min_lat, max_lon, max_lat = bbox
    if min_lat <= -90:
        min_lat = -89.99999
    if max_lat >= 90:
        max_lat = 89.99999
    if min_lon <= -180:
        min_lon = -179.99999
    if max_lon >= 180:
        max_lon = 179.99999
    logger.info(f"Adjusted bounding box: [{min_lon}, {min_lat}, {max_lon}, {max_lat}]")
    return [min_lon, min_lat, max_lon, max_lat]


def sanitize_geometry(geometry):
    """Ensure that polygons are valid by closing rings if needed."""
    coordinates = geometry.get('coordinates', [])
    if coordinates:
        for ring in coordinates:
            if ring[0] != ring[-1]:  # If the polygon is not closed
                ring.append(ring[0])  # Close the polygon
    return geometry


def clean_properties(properties):
    """Clean and sanitize properties for each feature."""
    field_name_map = {
        'acquisitionIdentifier': 'acqId',
        'acquisitionDate': 'acqDate',
        'sensorType': 'sensor',
        'azimuthAngle': 'azAngle',
        'illuminationAzimuthAngle': 'sun_angle',
        'illuminationElevationAngle': 'sun_elev'
    }
    sanitized_properties = {}
    for key, value in properties.items():
        new_key = field_name_map.get(key, key)
        if value is None or value == '':
            continue
        if new_key in ['resolution', 'cloudCover', 'azAngle', 'sun_angle', 'sun_elev']:
            value = round(float(value), 2)
        if new_key in ['acqDate']:
            value = value.split('.')[0] + 'Z'
        sanitized_properties[new_key] = value
    return sanitized_properties


def sanitize_record(record):
    """Replace empty strings and None values in the record with NULL."""
    if not isinstance(record, dict):
        logger.error(f"Unexpected record format (expected dict): {record}")
        return None  # Return None to skip this record in the case of an unexpected format

    sanitized_record = {key: (value if value not in ["", None] else None) for key, value in record.items()}
    return sanitized_record


def run_service(service_name, service_script, child_length=1):
    """Run the satellite collection for a given service, aggregating data for date ranges, and writing to the database."""
    logger = logging.getLogger(service_name.upper())
    logger.info("Starting satellite collection")

    start_time = datetime.now()  # Record the start time

    normalization_funcs = {
        'airbus': normalize_airbus,
        'blacksky': normalize_blacksky,
        'capella': normalize_capella,
        'maxar': normalize_maxar,
        'planet': normalize_planet,
        'satellogic': normalize_satellogic,
        'skyfi': normalize_skyfi,
    }

    seen_ids = set()  # Track seen `m_imageid` to prevent duplicates

    try:
        if isinstance(GEOHASH, str):
            geohashes = generate_geohashes([GEOHASH], child_length)
        elif isinstance(GEOHASH, list):
            geohashes = generate_geohashes(GEOHASH, child_length)
        else:
            raise ValueError("GEOHASH must be a string or a list of strings.")

        # Adjusted date range loop to handle 7-day blocks
        start_date = datetime.strptime(START_DATE, '%Y-%m-%d')
        end_date = datetime.strptime(END_DATE, '%Y-%m-%d')
        current_date = start_date

        while current_date <= end_date:
            block_end_date = current_date + timedelta(days=7)
            if block_end_date > end_date:
                block_end_date = end_date

            logger.info(
                f"Processing date range: {current_date.strftime('%Y-%m-%d')} to {block_end_date.strftime('%Y-%m-%d')}")
            daily_records = []  # Collect all records for the current date range

            for geohash in geohashes:
                try:
                    logger.info(
                        f"Processing geohash: {geohash} for date range: {current_date.strftime('%Y-%m-%d')} to {block_end_date.strftime('%Y-%m-%d')}")
                    bbox = geohash_to_bbox(geohash)
                    adjusted_bbox = validate_and_adjust_bbox(bbox, logger)

                    records = service_script.collect_images(
                        geohash,
                        adjusted_bbox,
                        current_date.strftime('%Y-%m-%d'),
                        block_end_date.strftime('%Y-%m-%d'),
                        sanitize_geometry,
                        clean_properties,
                        logger
                    )

                    logger.info(f"Retrieved {len(records) if records else 0} records for geohash {geohash}")

                    # Normalize and sanitize records
                    normalize = normalization_funcs.get(service_name.lower())
                    for record in records:
                        normalized_record = normalize(record)
                        sanitized_record = sanitize_record(normalized_record)
                        if sanitized_record:
                            record_id = sanitized_record["m_imageid"]
                            if record_id not in seen_ids:
                                seen_ids.add(record_id)
                                daily_records.append(sanitized_record)
                            else:
                                logger.debug(f"Duplicate record skipped: {record_id}")

                except Exception as e:
                    logger.error(
                        f"Error processing geohash {geohash} for date range: {current_date.strftime('%Y-%m-%d')} to {block_end_date.strftime('%Y-%m-%d')}: {e}")

            # Write records to the database
            if daily_records:
                try:
                    logger.info(
                        f"Writing {len(daily_records)} records to the database for date range: {current_date.strftime('%Y-%m-%d')} to {block_end_date.strftime('%Y-%m-%d')}")
                    write_to_db(daily_records)
                    logger.info(f"Successfully wrote {len(daily_records)} records to the database.")
                except Exception as e:
                    logger.error(
                        f"Error writing records to the database for date range: {current_date.strftime('%Y-%m-%d')} to {block_end_date.strftime('%Y-%m-%d')}: {e}")

            # Advance to the next block
            current_date = block_end_date + timedelta(days=1)

    except Exception as e:
        logger.error(f"Critical error in {service_name}: {e}")

    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"Service {service_name} started at {start_time.strftime('%H:%M:%S')}, "
                f"ended at {end_time.strftime('%H:%M:%S')}, "
                f"duration: {str(duration).split('.')[0]}")


def run_services_in_parallel(services):
    """Run all services in parallel, normalize results, and write to the database."""
    all_records = []
    seen_ids = set()  # Track unique identifiers to prevent duplicates
    normalization_skipped = 0  # Counter for unintentionally skipped records

    normalization_funcs = {
        'airbus': normalize_airbus,
        'blacksky': normalize_blacksky,
        'capella': normalize_capella,
        'maxar': normalize_maxar,
        'planet': normalize_planet,
        'satellogic': normalize_satellogic,
        'skyfi': normalize_skyfi,
    }

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(run_service, service_name, service_script): service_name for
                   service_name, service_script in services}

        for future in concurrent.futures.as_completed(futures):
            service_name = futures[future]
            try:
                # Retrieve records from the service
                raw_records = future.result()
                if raw_records is None:
                    logging.getLogger(service_name.upper()).error("No records retrieved.")
                    continue

                # Process and deduplicate records
                normalize = normalization_funcs.get(service_name.lower())
                for record in raw_records:
                    try:
                        normalized_record = normalize(record)
                        if normalized_record:
                            record_id = normalized_record.get('m_imageid')

                            # Check for duplicates across services
                            if record_id and record_id not in seen_ids:
                                seen_ids.add(record_id)
                                all_records.append(normalized_record)
                            else:
                                logging.debug(f"Duplicate record detected and skipped for ID: {record_id}")
                        else:
                            normalization_skipped += 1  # Track unintentionally skipped records
                    except Exception as e:
                        logging.error(f"Error normalizing record from {service_name}: {e}")

            except Exception as exc:
                logging.getLogger(service_name.upper()).error(f"Generated an exception: {exc}")

    # Log counts
    logging.warning(f"Unintentionally skipped records due to missing data: {normalization_skipped}")

    if not all_records:
        logging.error("No valid records found across all services.")
        return

    # Write all records to the database
    write_to_db(all_records)


def log_time_statistics(start_time, end_time):
    """
    Logs the start time, end time, and total duration of the script.

    Args:
        start_time (datetime): The time when the script started.
        end_time (datetime): The time when the script ended.
    """
    total_duration = end_time - start_time
    formatted_start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
    formatted_end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
    formatted_total_duration = str(total_duration).split('.')[0]  # Removes microseconds

    print("\nScript Runtime Statistics:")
    print(f"Start Time: {formatted_start_time}")
    print(f"End Time: {formatted_end_time}")
    print(f"Total Duration: {formatted_total_duration}")


if __name__ == "__main__":
    import api_airbus as airbus
    import api_blacksky as blacksky
    import api_capella as capella
    import api_maxar as maxar
    import api_planet as planet
    import api_satellogic as satellogic
    import api_skyfi as skyfi

    services = [
        # ("airbus", airbus),
        # ("blacksky", blacksky),
        # ("capella", capella),
        # ("maxar", maxar),
        # ("planet", planet),
        #### ("satellogic", satellogic),
        ("skyfi", skyfi),
    ]

    child_length = 1  # Specify geohash granularity level

    # Record the start time
    script_start_time = datetime.now()

    # Run all services in parallel
    run_services_in_parallel(services)

    # Record the end time
    script_end_time = datetime.now()

    # Log the runtime statistics
    log_time_statistics(script_start_time, script_end_time)




'''


CREATE TABLE public.satellite_data (
    id SERIAL PRIMARY KEY,
    service TEXT,
    m_datetime TIMESTAMP WITHOUT TIME ZONE,
    m_imageid TEXT,
    m_cloud NUMERIC,
    m_const TEXT,
    m_platform TEXT,
    gsd NUMERIC,
    offnadir NUMERIC,
    collection_date DATE,
    geometrycentroid_lat DOUBLE PRECISION,
    geometrycentroid_lon DOUBLE PRECISION,
    centroid_region TEXT,
    centroid_local TEXT,
    az_angle NUMERIC,
    sun_angle NUMERIC,
    sun_elev NUMERIC,
    holdback NUMERIC,
    footprint_geojson JSONB
);


'''