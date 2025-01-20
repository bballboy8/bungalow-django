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
from api.models import Site
import math
from django.contrib.gis.db.models.functions import Distance
from django.db.models import Count
from django.db.models.functions import TruncDate
from itertools import chain


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
        return {"data": [], "status_code": 400, "error": f"Error: {str(e)}"}


def convert_geojson_to_wkt(geometry):
    logger.info("Inside convert GeoJSON to WKT service")
    try:
        try:
            polygon = shape(geometry)
            wkt = polygon.wkt
        except Exception as e:
            return {"data": [], "status_code": 400, "error": f"Invalid GeoJSON: {str(e)}"}
        
        try:
            geod = Geod(ellps="WGS84")
            polygon = shapely.wkt.loads(wkt)
            area = round(abs(geod.geometry_area_perimeter(polygon)[0]) / 1000000.0, 2)
        except Exception as e:
            return {"data": [], "status_code": 400, "error": f"Error calculating area from GeoJSON: {str(e)}"}

        logger.info("GeoJSON converted to WKT successfully")
        return {"data": wkt, "area": area, "status_code": 200}
    except Exception as e:
        logger.error(f"Error converting GeoJSON to WKT: {str(e)}")
        return {"data": [], "status_code": 400, "error": f"Error: {str(e)}"}


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
    vendor_id: str = None,
    request=None,
    sort_by: str = None,
    sort_order: str = None,
    zoomed_wkt: str = None,
    vendor_name: str = None,
    min_cloud_cover: float = None,
    max_cloud_cover: float = None,
    min_off_nadir_angle: float = None,
    max_off_nadir_angle: float = None,
    min_gsd: float = None,
    max_gsd: float = None,
    
):
    logger.info("Inside get satellite records service")
    start_time = datetime.now()

    try:
        captures = SatelliteCaptureCatalog.objects.all()
        filters = Q()

        polygon_area = None

        if start_date:
            filters &= Q(acquisition_datetime__gte=start_date)
        if end_date:
            filters &= Q(acquisition_datetime__lte=end_date)

        if vendor_id:
            filters &= Q(vendor_id=vendor_id)

        if latitude and longitude and distance:
            filters &= Q(
                location_polygon__distance_lte=(
                    Point(longitude, latitude, srid=4326),
                    D(km=distance),
                )
            )

        if vendor_name and "," in vendor_name:
            vendor_names = vendor_name.split(",")
            filters &= Q(vendor_name__in=vendor_names)
        elif vendor_name:
            filters &= Q(vendor_name=vendor_name)

        if wkt_polygon:
            try:
                area_response = get_area_from_polygon_wkt(wkt_polygon)
                if area_response["status_code"] == 200:
                    polygon_area = area_response["data"]
                    if polygon_area > 1000000000:
                        logger.warning("Area is too large for processing")
                        return {"data": "Area is too large for processing", "status_code": 400}
                else:
                    logger.warning(f"Failed to calculate area: {area_response['data']}")
            except Exception as e:
                logger.error(f"Error calculating polygon area: {str(e)}")
            logger.debug("Polygon WKT provided")
            wkt_polygon_geom = GEOSGeometry(wkt_polygon)
            filters &= Q(location_polygon__intersects=wkt_polygon_geom)

                    
        if min_cloud_cover is not None and max_cloud_cover is not None:
            logger.debug(f"Cloud cover filters: {min_cloud_cover} to {max_cloud_cover}")
            cloud_cover_filters = (
                Q(vendor_name="planet", cloud_cover__gte=min_cloud_cover / 100, cloud_cover__lte=max_cloud_cover / 100) |
                Q(~Q(vendor_name="planet"), cloud_cover__gte=min_cloud_cover, cloud_cover__lte=max_cloud_cover)
            )
            # Include null values only if either min or max is 0
            if min_cloud_cover == 0 or max_cloud_cover == 0:
                logger.debug("Including null cloud cover values")
                cloud_cover_filters |= Q(cloud_cover__isnull=True)

            filters &= cloud_cover_filters

        if min_off_nadir_angle is not None and max_off_nadir_angle is not None:
            logger.debug(f"Sun elevation filters: {min_off_nadir_angle} to {max_off_nadir_angle}")
            sun_elevation_filters = Q(sun_elevation__gte=min_off_nadir_angle, sun_elevation__lte=max_off_nadir_angle)
            filters &= sun_elevation_filters

        if min_gsd is not None and max_gsd is not None:
            logger.debug(f"GSD filters: {min_gsd} to {max_gsd}")
            gsd_filters = Q(gsd__gte=min_gsd, gsd__lte=max_gsd)
            filters &= gsd_filters

        zoomed_captures = []
        if zoomed_wkt:
            try:
                zoomed_geom = GEOSGeometry(zoomed_wkt)
                zoomed_filters = filters & Q(location_polygon__intersects=zoomed_geom) & Q(location_polygon__within=wkt_polygon_geom)
                zoomed_captures = captures.filter(zoomed_filters)

                if sort_by and sort_order:
                    zoomed_captures = (
                        zoomed_captures.order_by(sort_by)
                        if sort_order == "asc"
                        else zoomed_captures.order_by(f"-{sort_by}")
                    )

                logger.debug(f"Zoomed WKT matches {len(zoomed_captures)} records")
            except Exception as e:
                logger.error(f"Error processing zoomed WKT: {str(e)}")
                return {"data": str(e), "status_code": 400}

        captures = captures.filter(filters).exclude(id__in=[record.id for record in zoomed_captures])
        if sort_by and sort_order:
            captures = (
                        captures.order_by(sort_by)
                        if sort_order == "asc"
                        else captures.order_by(f"-{sort_by}")
                    )

        if zoomed_captures:
            combined_captures = list(chain(zoomed_captures, captures))
        else:
            combined_captures = list(captures)

        if source == "home" and not vendor_id:
            if not wkt_polygon or (latitude and longitude and distance):
                return {"data": "Please provide a valid polygon or latitude, longitude, and distance", "status_code": 400}

            total_records = len(combined_captures)
            final_response = combined_captures
        else:
            paginator = Paginator(combined_captures, page_size)
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

            skfyi_ids = [
                record["id"] for record in missing_images if "skyfi" in record["vendor_name"]
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

            if skfyi_ids:
                response = get_skyfi_record_thumbnails_by_ids(skfyi_ids)
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

        if vendor_id and len(final_response) == 1:
            first_record = final_response[0]
            latitude = first_record.location_polygon.centroid.y
            longitude = first_record.location_polygon.centroid.x
            address_response = get_address_from_lat_long_via_google_maps(latitude, longitude)
            if address_response["status_code"] == 200:
                first_record.address = address_response["data"]

            location_polygon = first_record.location_polygon
            nearest_site = Site.objects.annotate(distance=Distance("location_polygon", location_polygon)).order_by("distance").first()
            if nearest_site:
                nearest_site_details = {
                    "name": nearest_site.name,
                    "distance": nearest_site.distance.km
                }
                first_record.nearest_site = nearest_site_details

        # Success response
        logger.info("Satellite records fetched successfully")
        return {
            "zoomed_captures_count": len(zoomed_captures),
            "data": final_response,
            "polygon_area_km2": polygon_area,
            "time_taken": str(datetime.now() - start_time),
            "total_records": total_records,
            "page_number": page_number if source != "home" else None,
            "page_size": page_size if source != "home" else None,
            "status_code": 200,
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"Error fetching satellite records: {str(e)}")
        return {"data": str(e), "status_code": 400, "error": f"Error: {str(e)}"}


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
        return {"data": f"{str(e)}", "status_code": 400, "error": f"Error: {str(e)}"}


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
        return {"data": f"{str(e)}", "status_code": 500, "error": f"Error: {str(e)}"}

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
        durations = [1, 4, 7, 30, 90]
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


        # Count for each vendor in the selected area airbus, maxar, planet, blacksky, capella, skyfi-umbra

        vendor_counts = SatelliteCaptureCatalog.objects.filter(
            location_polygon__intersects=buffered_polygon
        ).values('vendor_name').annotate(count=Count('id'))

        vendor_count = {vendor['vendor_name']: vendor['count'] for vendor in vendor_counts}

        total_vendor_count = sum(vendor_count.values())

        analytics = {
            "vendor_count": vendor_count,
            "total_count": total_vendor_count,
            "average_per_day": total_vendor_count /
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
        return {"data": f"{str(e)}", "status_code": 500, "error": f"Error: {str(e)}"}


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

        try:
            area_response = get_area_from_polygon_wkt(polygon_wkt)
            area = area_response["data"] if area_response["status_code"] == 200 else 0
        except Exception as e:
            logger.error(f"Error calculating area from WKT: {str(e)}")
            area = 0
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
        durations = [1, 4, 7, 30, 90 ]
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


        analytics = {
            "total_count": sum(result["current_count"] for result in results.values()),
            "average_per_day": sum(result["current_count"] for result in results.values()) / (now() - longest_period_start).days,
            "oldest_date": longest_period_start,
            "newest_info": NewestInfoSerializer(newest_record_instance).data if newest_record_instance else None,
            "newest_clear_cloud_cover_info": NewestInfoSerializer(newest_clear_cloud_cover_instance).data if newest_clear_cloud_cover_instance else None,
            "address": address_response["data"],
            "percentages": results,
            "area": area
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
        return {"data": f"{str(e)}", "status_code": 500, "error": f"Error: {str(e)}"}

def get_polygon_selection_acquisition_calender_days_frequency(polygon_wkt, start_date, end_date):
    """
    Retrieve the frequency of image captures for each calendar day in the selected area.

    Args:
        polygon_wkt (str): WKT representation of the selected area polygon.
        start_date (datetime): Start date for the acquisition range.
        end_date (datetime): End date for the acquisition range.

    Returns:
        dict: A dictionary containing the frequency of image captures for each calendar day.
    """
    try:
        logger.info("Starting frequency calculation for polygon selection.")

        # Start time tracking
        func_start_time = now()

        # Convert WKT string to a Polygon object
        polygon = fromstr(polygon_wkt)
        logger.debug(f"Polygon WKT: {polygon_wkt}")

        # Fetch frequency data directly from the database
        frequency_data = (
            SatelliteCaptureCatalog.objects.filter(
                location_polygon__intersects=polygon,
                acquisition_datetime__gte=start_date,
                acquisition_datetime__lte=end_date
            )
            .annotate(date=TruncDate('acquisition_datetime'))  # Extract the date part
            .values('date')  # Group by the date
            .annotate(count=Count('id'))  # Count captures for each date
            .order_by('date')  # Sort by date
        )

        # Convert QuerySet to dictionary
        frequency_dict = {entry['date'].strftime("%Y-%m-%d"): entry['count'] for entry in frequency_data}

        # Calculate time taken
        func_end_time = now()
        net_time = func_end_time - func_start_time
        logger.info(f"Frequency calculation completed in {net_time} seconds.")

        return {
            "data": frequency_dict,
            "time_taken": str(net_time),
            "status_code": 200,
        }

    except Exception as e:
        logger.error(f"Error in frequency calculation: {str(e)}", exc_info=True)
        return {"data": None, "status_code": 500, "error": f"Error: {str(e)}"}


def get_pin_selection_acquisition_calender_days_frequency(latitude, longitude, distance, start_date, end_date):
    """
    Retrieve the frequency of image captures for each calendar day around the selected pin.

    Args:
        latitude (float): Latitude of the selected pin.
        longitude (float): Longitude of the selected pin.
        distance (float): Radius in kilometers around the selected pin.
        start_date (datetime): Start date for the acquisition range.
        end_date (datetime): End date for the acquisition range.

    Returns:
        dict: A dictionary containing the frequency of image captures for each calendar day.
    """
    try:
        logger.info("Starting frequency calculation for pin selection.")

        # Start time tracking
        func_start_time = now()

        # Create a point and buffer it
        point = Point(longitude, latitude, srid=4326)
        buffered_polygon = point.buffer(distance / 111.32)  # Approximation for 1 degree = ~111.32 km

        logger.debug(f"Pin Location: Latitude={latitude}, Longitude={longitude}, Distance={distance} km")

        # Fetch frequency data directly from the database
        frequency_data = (
            SatelliteCaptureCatalog.objects.filter(
                location_polygon__intersects=buffered_polygon,
                acquisition_datetime__gte=start_date,
                acquisition_datetime__lte=end_date
            )
            .annotate(date=TruncDate('acquisition_datetime'))  # Extract the date part
            .values('date')  # Group by the date
            .annotate(count=Count('id'))  # Count captures for each date
            .order_by('date')  # Sort by date
        )

        # Convert QuerySet to dictionary
        frequency_dict = {entry['date'].strftime("%Y-%m-%d"): entry['count'] for entry in frequency_data}

        # Calculate time taken
        func_end_time = now()
        net_time = func_end_time - func_start_time
        logger.info(f"Frequency calculation completed in {net_time} seconds.")

        return {
            "data": frequency_dict,
            "time_taken": str(net_time),
            "status_code": 200,
        }

    except Exception as e:
        logger.error(f"Error in frequency calculation: {str(e)}", exc_info=True)
        return {"data": None, "status_code": 500, "error": f"Error: {str(e)}"}


def generate_circle_polygon_geojson(latitude, longitude, distance_km, num_points=36):
    """
    Generates a GeoJSON polygon representing a circle centered at a given latitude and longitude.

    Args:
        latitude (float): Latitude of the center in decimal degrees.
        longitude (float): Longitude of the center in decimal degrees.
        distance_km (float): Radius of the circle in kilometers.
        num_points (int): Number of points to approximate the circle.

    Returns:
        dict: GeoJSON dictionary representing the circle as a polygon.
    """
    EARTH_RADIUS_KM = 6371.0
    radius_radians = distance_km / EARTH_RADIUS_KM

    points = []
    for i in range(num_points + 1):  # +1 to close the polygon
        angle = 2 * math.pi * i / num_points  # Angle in radians
        lat = math.asin(math.sin(math.radians(latitude)) * math.cos(radius_radians) +
                        math.cos(math.radians(latitude)) * math.sin(radius_radians) * math.cos(angle))
        lon = math.radians(longitude) + math.atan2(math.sin(angle) * math.sin(radius_radians) * math.cos(math.radians(latitude)),
                                                   math.cos(radius_radians) - math.sin(math.radians(latitude)) * math.sin(lat))
        points.append([math.degrees(lon), math.degrees(lat)])  # GeoJSON uses [lon, lat]

    geojson_polygon = {
        "type": "Point",
        "coordinates": [points]
    }
    return geojson_polygon


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the Haversine distance between two points in kilometers.

    Args:
        lat1, lon1: Latitude and longitude of the first point in degrees.
        lat2, lon2: Latitude and longitude of the second point in degrees.

    Returns:
        float: Distance in kilometers.
    """
    EARTH_RADIUS_KM = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return EARTH_RADIUS_KM * c


def get_circle_parameters_from_geojson(geojson_polygon):
    """
    Extract the center (latitude, longitude) and radius (distance) from a GeoJSON polygon.

    Args:
        geojson_polygon (dict): GeoJSON dictionary representing a circle as a polygon.

    Returns:
        tuple: (latitude, longitude, radius_km)
    """
    coordinates = geojson_polygon["coordinates"][0]  # Outer ring of the polygon

    # Calculate the center (approximation by averaging points)
    lon_sum = sum(point[0] for point in coordinates)
    lat_sum = sum(point[1] for point in coordinates)
    num_points = len(coordinates)
    center_lon = lon_sum / num_points
    center_lat = lat_sum / num_points

    # Calculate the radius using the first point
    first_point = coordinates[0]
    radius_km = haversine_distance(center_lat, center_lon, first_point[1], first_point[0])

    return center_lat, center_lon, radius_km
