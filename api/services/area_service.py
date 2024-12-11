from django.contrib.gis.geos import GEOSGeometry
from core.models import SatelliteCaptureCatalog
from shapely.geometry import shape
from logging_module import logger
from django.contrib.gis.geos import Point
from django.contrib.gis.measure import D
from django.core.paginator import Paginator
from django.db.models import Q
from core.utils import s3, bucket_name
from typing import List
from datetime import datetime, timedelta
from api.serializers.area_serializer import NewestInfoSerializer
from decouple import config
import requests
from django.utils.timezone import now
from concurrent.futures import ThreadPoolExecutor
from django.contrib.gis.geos import fromstr
import shapely.wkt
from pyproj import Geod
from api.services.vendor_service import *


def get_area_from_polygon_wkt(polygon_wkt: str):
    logger.info("Inside get area from WKT service")
    try:
        geod = Geod(ellps="WGS84")
        polygon = shapely.wkt.loads(polygon_wkt)
        area = round(abs(geod.geometry_area_perimeter(polygon)[0]) / 1000000.0, 2)
        logger.info("Area fetched successfully")
        return {"data": area, "status_code": 200}
    except Exception as e:
        logger.error(f"Error fetching area from WKT: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 400}



def convert_geojson_to_wkt(geometry):
    logger.info("Inside convert GeoJSON to WKT service")
    try:
        try:
            polygon = shape(geometry)
            wkt = polygon.wkt
        except Exception as e:
            return {"data": f"Invalid GeoJSON: {str(e)}", "status_code": 400}
        
        try:
            geod = Geod(ellps="WGS84")
            polygon = shapely.wkt.loads(wkt)
            area = round(abs(geod.geometry_area_perimeter(polygon)[0]) / 1000000.0, 2)
        except Exception as e:
            return {"data": f"Error calculating area from GeoJSON: {str(e)}", "status_code": 400}

        logger.info("GeoJSON converted to WKT successfully")
        return {"data": wkt, "area": area, "status_code": 200}
    except Exception as e:
        logger.error(f"Error converting GeoJSON to WKT: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 400}


def get_satellite_records(
    page_number: int = 1,
    page_size: int = 10,
    start_date: str = None,
    end_date: str = None,
    wkt_polygon: str = None,
    latitude: float = None,
    longitude: float = None,
    distance: float = None,
    source: str = "home",
    request=None
):
    logger.info("Inside get satellite records service")
    start_time = datetime.now()

    try:
        captures = SatelliteCaptureCatalog.objects.all()
        filters = Q()

        polygon_area = None

        if wkt_polygon:
            try:
                area_response = get_area_from_polygon_wkt(wkt_polygon)
                if area_response["status_code"] == 200:
                    polygon_area = area_response["data"]
                    if polygon_area > 100000000:
                        logger.warning("Area is too large for processing")
                        return {"data": "Area is too large for processing", "status_code": 400}
                else:
                    logger.warning(f"Failed to calculate area: {area_response['data']}")
            except Exception as e:
                logger.error(f"Error calculating polygon area: {str(e)}")
            filters &= Q(location_polygon__intersects=GEOSGeometry(wkt_polygon))

        if latitude and longitude and distance:
            filters &= Q(
                location_polygon__distance_lte=(
                    Point(longitude, latitude, srid=4326),
                    D(km=distance),
                )
            )

        if start_date:
            filters &= Q(acquisition_datetime__gte=start_date)
        if end_date:
            filters &= Q(acquisition_datetime__lte=end_date)

        captures = captures.filter(filters).order_by("-acquisition_datetime")

        if source == "home":
            if not wkt_polygon or (latitude and longitude and distance):
                return {"data": "Please provide a valid polygon or latitude, longitude, and distance", "status_code": 400}
            
            total_records = captures.count()
            captures = list(captures)
            final_response = list(captures)
        else:
            paginator = Paginator(captures, page_size)
            page = paginator.get_page(page_number)

            proxy_urls = {}
            missing_images = [
                {"vendor_name": record.vendor_name, "id": record.vendor_id}
                for record in page
                if not record.image_uploaded
            ]

            capella_ids = [
                record["id"] for record in missing_images if record["vendor_name"] == "capella"
            ]

            if missing_images:
                proxy_urls.update({
                    record["id"]: generate_proxy_url(request, record["vendor_name"], record["id"])
                    for record in missing_images
                    if record["vendor_name"] != "capella"
                })

            if capella_ids:
                response = get_capella_record_thumbnails_by_ids(capella_ids)
                if response["status_code"] == 200:
                    proxy_urls.update({
                        record["id"]: record["thumbnail"]
                        for record in response["data"]
                    })

            final_response = []
            for record in page:
                if record.vendor_name == "maxar":
                    file_name = f"{record.vendor_id.split("-")[0]}.png"
                else:
                    file_name = f"{record.vendor_id}.png"
                record.presigned_url = None

                if record.image_uploaded:
                    record.presigned_url = s3.generate_presigned_url(
                        "get_object",
                        Params={"Bucket": bucket_name, "Key": f"{record.vendor_name}/{file_name}"},
                        ExpiresIn=3600,
                    )
                else:
                    record.presigned_url = proxy_urls.get(record.vendor_id)

                final_response.append(record)

            total_records = paginator.count

        # Success response
        logger.info("Satellite records fetched successfully")
        return {
            "data": final_response,
            "polygon_area_km2": polygon_area,
            "time_taken": str(datetime.now() - start_time),
            "total_records": total_records,
            "page_number": page_number if source != "home" else None,
            "page_size": page_size if source != "home" else None,
            "status_code": 200,
        }

    except Exception as e:
        logger.error(f"Error fetching satellite records: {str(e)}")
        return {"data": str(e), "status_code": 400}


    
def get_presigned_url_by_vendor_name_and_id(record:List[dict]):
    logger.info("Inside get presigned URL by vendor name and id service")
    try:
        all_urls = []
        for record in record:
            file_name = f"{record['id']}.png"
            presigned_url = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket_name, "Key": f"{record['vendor']}/{file_name}"},
                ExpiresIn=3600
            )
            all_urls.append({"id": record["id"], "url": presigned_url})

        logger.info("Presigned URL fetched successfully")
        return {
            "data": all_urls,
            "status_code": 200
        }
    except Exception as e:
        logger.error(f"Error fetching presigned URL: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 400}
    

def group_by_vendor(data):
    """
    Groups the input data by 'vendor_name' and collects IDs for each vendor.

    Args:
        data (list): A list of dictionaries containing 'vendor_name' and 'id'.

    Returns:
        dict: A dictionary with vendor names as keys and lists of IDs as values.
    """
    grouped_data = {}
    for item in data:
        vendor_name = item.get('vendor_name')
        if vendor_name and not item.get("image_uploaded"):
            grouped_data.setdefault(vendor_name, []).append(item['vendor_id'])
    return grouped_data



def get_address_from_lat_long_via_google_maps(latitude: float, longitude: float):
    """
    Get the address from latitude and longitude using Google Maps API.

    Args:
        latitude (float): Latitude of the location.
        longitude (float): Longitude of the location.

    Returns:
        dict: A dictionary containing address data.
    """
    logger.info("Inside get address from latitude and longitude service")
    try:
        maps_api_key = config("GOOGLE_MAPS_API_KEY")
        url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={latitude},{longitude}&key={maps_api_key}"
        response = requests.get(url)
        response_data = response.json()
        address = response_data.get("results")[0].get("formatted_address")
        logger.info("Address fetched successfully")
        return {"data": address, "status_code": 200}
    except Exception as e:
        logger.error(f"Error fetching address from latitude and longitude: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 500}

def calculate_percentage_change(current_count, previous_count):
    """Helper to calculate percentage change."""
    if previous_count > 0:
        return ((current_count - previous_count) / previous_count) * 100
    return 0

def calculate_counts_and_percentages(days, buffered_polygon):
    """Function to calculate counts and percentages for a specific duration."""
    start_time = now() - timedelta(days=days)
    current_count = SatelliteCaptureCatalog.objects.filter(
        location_polygon__intersects=buffered_polygon,
        acquisition_datetime__gte=start_time
    ).count()

    previous_duration_start = start_time - timedelta(days=days)
    previous_count = SatelliteCaptureCatalog.objects.filter(
        location_polygon__intersects=buffered_polygon,
        acquisition_datetime__gte=previous_duration_start,
        acquisition_datetime__lt=start_time
    ).count()
    percentage_change = calculate_percentage_change(current_count, previous_count)
    return days, current_count, previous_count,  percentage_change

def get_pin_selection_analytics_and_location(latitude, longitude, distance):
    """
    Retrieve analytics and location information for a selected pin.

    This function fetches analytics such as the oldest and newest images, the total 
    count of images, the average number of images captured per day, and the percentage 
    change in the number of images captured for the selected area over various durations. 
    The durations include 1 day, 4 days, 30 days, 60 days, 90 days, and 180 days.

    Args:
        latitude (float): Latitude of the selected pin.
        longitude (float): Longitude of the selected pin.
        distance (float): The radius in kilometers around the selected pin for which 
                          analytics are calculated.

    Returns:
        dict: A dictionary containing:
            - `analytics` (dict): Includes the following:
                - `total_count` (int): Total number of images captured in the selected area.
                - `average_per_day` (float): Average number of images captured per day.
                - `oldest_date` (datetime): Acquisition date of the oldest image in the area.
                - `newest_info` (dict): Serialized data for the newest image in the area.
                - `address` (str): The address of the selected location obtained via Google Maps API.
                - `percentages` (dict): Contains percentage change and counts for each duration:
                    - `key` (str): Duration (e.g., "1", "4", "30", "60", "90"").
                    - `percentage_change` (float): Percentage change in the number of images.
                    - `current_count` (int): Count of images captured in the current duration.
                    - `previous_count` (int): Count of images captured in the previous duration.
            - `time_taken` (str): The total time taken to execute the function.
        dict: Error response with a `status_code` and error message if the process fails.

    Raises:
        Exception: If any unexpected errors occur during the execution.

    Percentage Calculation:
        The percentage change is calculated as:
        
            Percentage Change = ((Current Count - Previous Count) / Previous Count) * 100

        Where:
            - `Current Count`: The number of images captured in the selected duration.
            - `Previous Count`: The number of images captured in the equivalent previous duration.

        If `Previous Count` is zero, the percentage change is set to 0 to avoid division errors.
    """
    try:
        logger.info("Inside get pin selection analytics and location service")
        func_start_time = now()

        point = Point(longitude, latitude, srid=4326)
        buffered_polygon = point.buffer(distance / 111.32)

        logger.info(f"Latitude: {latitude}, Longitude: {longitude}, Distance: {distance}")

        address_response = get_address_from_lat_long_via_google_maps(latitude, longitude)
        if address_response["status_code"] != 200:
            return address_response

        # Determine the oldest record start time
        oldest_record_instance = SatelliteCaptureCatalog.objects.filter(
            location_polygon__intersects=buffered_polygon
        ).order_by("acquisition_datetime").first()

        longest_period_start = oldest_record_instance.acquisition_datetime if oldest_record_instance else None
        if not longest_period_start:
            return {"data": "No records found for the given location", "status_code": 404}

        # Multithreaded calculation of counts and percentages
        durations = [1, 4, 30, 60, 90, 180]
        results = {}
        with ThreadPoolExecutor() as executor:
            future_to_duration = {
                executor.submit(calculate_counts_and_percentages, days, buffered_polygon): days
                for days in durations
            }
            for future in future_to_duration:
                days, current_count, previous_count, percentage_change = future.result()
                results[days] = {"current_count": current_count, "previous_count": previous_count, "percentage_change": percentage_change }

        newest_record_instance = SatelliteCaptureCatalog.objects.filter(
            location_polygon__intersects=buffered_polygon
        ).order_by("acquisition_datetime").last()

        # newest clear cloud cover info means cloud cover is 0 also there are null values in cloud cover

        newest_clear_cloud_cover_instance = SatelliteCaptureCatalog.objects.filter(
            location_polygon__intersects=buffered_polygon,
            cloud_cover__lte=0
        ).order_by("-acquisition_datetime").first()

        analytics = {
            "total_count": sum(result["current_count"] for result in results.values()),
            "average_per_day": sum(result["current_count"] for result in results.values()) /
                               (now() - longest_period_start).days,
            "oldest_date": longest_period_start,
            "newest_info": NewestInfoSerializer(newest_record_instance).data if newest_record_instance else None,
            "newest_clear_cloud_cover_info": NewestInfoSerializer(newest_clear_cloud_cover_instance).data if newest_clear_cloud_cover_instance else None,
            "address": address_response["data"],
            "percentages": results
        }

        func_end_time = now()
        net_time = func_end_time - func_start_time
        logger.info(f"Time taken to fetch pin selection analytics and location: {net_time}")

        return {
            "data": {"analytics": analytics, "time_taken": str(net_time)},
            "status_code": 200,
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"Error fetching pin selection analytics and location: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 500}
    

def get_polygon_selection_analytics_and_location_wkt(polygon_wkt):
    """
    Retrieve analytics and location information for a selected area using WKT polygon.

    This function fetches analytics such as the oldest and newest images, the total 
    count of images, the average number of images captured per day, and the percentage 
    change in the number of images captured for the selected area over various durations. 
    The durations include 1 day, 4 days, 30 days, 60 days, 90 days, and 180 days.

    Args:
        polygon_wkt (str): WKT representation of the selected area polygon.

    Returns:
        dict: A dictionary containing analytics and location details, or error details.
    """
    try:
        logger.info("Inside get pin selection analytics and location service with WKT input")
        func_start_time = now()

        # Convert the WKT string to a Polygon object
        polygon = fromstr(polygon_wkt)

        logger.info(f"Polygon WKT: {polygon_wkt}")

        # Fetch address if needed (you could use reverse geocoding here if applicable)
        address_response = get_address_from_lat_long_via_google_maps(polygon.centroid.y, polygon.centroid.x)
        if address_response["status_code"] != 200:
            return address_response

        # Determine the oldest record start time
        oldest_record_instance = SatelliteCaptureCatalog.objects.filter(
            location_polygon__intersects=polygon
        ).order_by("acquisition_datetime").first()

        longest_period_start = oldest_record_instance.acquisition_datetime if oldest_record_instance else None
        if not longest_period_start:
            return {"data": "No records found for the given location", "status_code": 404}

        # Multithreaded calculation of counts and percentages for various durations
        durations = [1, 4, 30, 60, 90, 180]
        results = {}
        with ThreadPoolExecutor() as executor:
            future_to_duration = {
                executor.submit(calculate_counts_and_percentages, days, polygon): days
                for days in durations
            }
            for future in future_to_duration:
                days, current_count, previous_count, percentage_change = future.result()
                results[days] = {"current_count": current_count, "previous_count": previous_count, "percentage_change": percentage_change}

        # Get the newest image record (preferably clear cloud cover if available)
        newest_record_instance = SatelliteCaptureCatalog.objects.filter(
            location_polygon__intersects=polygon
        ).order_by("acquisition_datetime").last()

        # Clear cloud cover info if available
        newest_clear_cloud_cover_instance = SatelliteCaptureCatalog.objects.filter(
            location_polygon__intersects=polygon,
            cloud_cover__lte=0
        ).order_by("-acquisition_datetime").first()

        # Prepare analytics data
        analytics = {
            "total_count": sum(result["current_count"] for result in results.values()),
            "average_per_day": sum(result["current_count"] for result in results.values()) / (now() - longest_period_start).days,
            "oldest_date": longest_period_start,
            "newest_info": NewestInfoSerializer(newest_record_instance).data if newest_record_instance else None,
            "newest_clear_cloud_cover_info": NewestInfoSerializer(newest_clear_cloud_cover_instance).data if newest_clear_cloud_cover_instance else None,
            "address": address_response["data"],
            "percentages": results
        }

        func_end_time = now()
        net_time = func_end_time - func_start_time
        logger.info(f"Time taken to fetch pin selection analytics and location: {net_time}")

        return {
            "data": {"analytics": analytics, "time_taken": str(net_time)},
            "status_code": 200,
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"Error fetching pin selection analytics and location: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 500}
    

def get_polygon_selection_acquisition_calender_days_frequency(polygon_wkt):
    """
    Retrieve the frequency of image captures for each calendar day in the selected area.

    This function calculates the frequency of image captures for each calendar day within the 
    selected area using the WKT polygon representation. The function returns a dictionary with 
    the date as the key and the count of images captured on that date as the value.

    Args:
        polygon_wkt (str): WKT representation of the selected area polygon.

    Returns:
        dict: A dictionary containing the frequency of image captures for each calendar day.
    """
    try:
        logger.info("Inside get polygon selection calendar days frequency service")
        func_start_time = now()

        # Convert the WKT string to a Polygon object
        polygon = fromstr(polygon_wkt)

        logger.info(f"Polygon WKT: {polygon_wkt}")

        # Fetch the records within the selected area
        records = SatelliteCaptureCatalog.objects.filter(
            location_polygon__intersects=polygon
        ).order_by("acquisition_datetime")

        # Calculate the frequency of image captures for each calendar day
        frequency_data = {}
        for record in records:
            date = record.acquisition_datetime.date()
            frequency_data[date.strftime("%Y-%m-%d")] = frequency_data.get(date, 0) + 1

        func_end_time = now()
        net_time = func_end_time - func_start_time
        logger.info(f"Time taken to fetch polygon selection calendar days frequency: {net_time}")

        return {
            "data": frequency_data,
            "time_taken": str(net_time),
            "status_code": 200,
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"Error fetching polygon selection calendar days frequency: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 500}

def get_pin_selection_acquisition_calender_days_frequency(latitude, longitude, distance):
    """
    Retrieve the frequency of image captures for each calendar day around the selected pin.

    This function calculates the frequency of image captures for each calendar day within the 
    selected area around the pin location. The function returns a dictionary with the date as the 
    key and the count of images captured on that date as the value.

    Args:
        latitude (float): Latitude of the selected pin.
        longitude (float): Longitude of the selected pin.
        distance (float): The radius in kilometers around the selected pin for which the frequency is calculated.

    Returns:
        dict: A dictionary containing the frequency of image captures for each calendar day.
    """
    try:
        logger.info("Inside get pin selection calendar days frequency service")
        func_start_time = now()

        point = Point(longitude, latitude, srid=4326)
        buffered_polygon = point.buffer(distance / 111.32)

        logger.info(f"Latitude: {latitude}, Longitude: {longitude}, Distance: {distance}")

        # Fetch the records within the selected area
        records = SatelliteCaptureCatalog.objects.filter(
            location_polygon__intersects=buffered_polygon
        ).order_by("acquisition_datetime")

        # Calculate the frequency of image captures for each calendar day
        frequency_data = {}
        for record in records:
            date = record.acquisition_datetime.date()
            frequency_data[date.strftime("%Y-%m-%d")] = frequency_data.get(date, 0) + 1

        func_end_time = now()
        net_time = func_end_time - func_start_time
        logger.info(f"Time taken to fetch pin selection calendar days frequency: {net_time}")

        return {
            "data": frequency_data,
            "time_taken": str(net_time),
            "status_code": 200,
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"Error fetching pin selection calendar days frequency: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 500}