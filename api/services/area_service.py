from django.contrib.gis.geos import GEOSGeometry
from core.models import CollectionCatalog, time_ranges
from shapely.geometry import shape
from logging_module import logger
from django.contrib.gis.geos import Point
from django.contrib.gis.measure import D
from django.core.paginator import Paginator
from django.db.models import Q
from core.utils import s3, bucket_name
from typing import List
from datetime import datetime, timedelta, time
from api.serializers.area_serializer import NewestInfoSerializer, OldestInfoSerializer
from decouple import config
import requests
from django.utils.timezone import now
from concurrent.futures import ThreadPoolExecutor
from django.contrib.gis.geos import fromstr
import shapely.wkt
from pyproj import Geod
from api.services.vendor_service import *
from api.models import Site, GroupSite
import math
from django.contrib.gis.db.models.functions import Distance
from django.db.models import Count
from django.db.models.functions import TruncDate
from itertools import chain
import pytz

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
    
def get_utc_time_range(time_period, user_timezone):
    # Get user's timezone object
    user_tz = pytz.timezone(user_timezone)

    # Get current date in the user's timezone
    local_now = datetime.now(user_tz).date()

    # Define start time in local timezone
    start_hour, end_hour = time_ranges[time_period]
    start_time = user_tz.localize(datetime.combine(local_now, time(start_hour, 0)))

    # Handle overnight case (crosses midnight)
    if time_period == "overnight":
        end_time = user_tz.localize(datetime.combine(local_now + timedelta(days=1), time(end_hour, 0)))
    else:
        end_time = user_tz.localize(datetime.combine(local_now, time(end_hour, 0)))

    # Convert to UTC
    start_time_utc = start_time.astimezone(pytz.utc).hour
    end_time_utc = end_time.astimezone(pytz.utc).hour

    return start_time_utc, end_time_utc


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
    focused_records_ids: str = None,
    user_timezone: str = None,
    user_duration_type: str = None,
    min_azimuth_angle: float = None,
    max_azimuth_angle: float = None,
    min_illumination_azimuth_angle: float = None,
    max_illumination_azimuth_angle: float = None,
    min_illumination_elevation_angle: float = None,
    max_illumination_elevation_angle: float = None,
    min_holdback_seconds: int = None,
    max_holdback_seconds: int = None,
):
    logger.info("Inside get satellite records service")
    start_time = datetime.now()

    try:
        captures = CollectionCatalog.objects.all()
        filters = Q()

        if sort_by and sort_by == "cloud_cover":
            sort_by = "cloud_cover_percent"

        polygon_area = None

        if start_date:
            filters &= Q(acquisition_datetime__gte=start_date)
        if end_date:
            filters &= Q(acquisition_datetime__lte=end_date)


        if latitude and longitude and distance:
            latitude, longitude, distance = float(latitude), float(longitude), float(distance)
            filters &= Q(
                location_polygon__distance_lte=(
                    Point(longitude, latitude, srid=4326),
                    D(km=distance),
                )
            )

        if min_azimuth_angle is not None and max_azimuth_angle is not None:
            min_azimuth_angle, max_azimuth_angle = float(min_azimuth_angle), float(max_azimuth_angle) 
            logger.debug(f"Azimuth angle filters: {min_azimuth_angle} to {max_azimuth_angle}")
            azimuth_angle_filters = Q(azimuth_angle__gte=min_azimuth_angle, azimuth_angle__lte=max_azimuth_angle)
            filters &= azimuth_angle_filters

            if min_azimuth_angle == -1:
                filters |= Q(azimuth_angle__isnull=True)

        if min_illumination_azimuth_angle is not None and max_illumination_azimuth_angle is not None:
            min_illumination_azimuth_angle, max_illumination_azimuth_angle = float(min_illumination_azimuth_angle), float(max_illumination_azimuth_angle)
            logger.debug(f"Illumination azimuth angle filters: {min_illumination_azimuth_angle} to {max_illumination_azimuth_angle}")
            illumination_azimuth_angle_filters = Q(illumination_azimuth_angle__gte=min_illumination_azimuth_angle, illumination_azimuth_angle__lte=max_illumination_azimuth_angle)
            filters &= illumination_azimuth_angle_filters

            if min_illumination_azimuth_angle == -1:
                filters |= Q(illumination_azimuth_angle__isnull=True)

        if min_illumination_elevation_angle is not None and max_illumination_elevation_angle is not None:
            min_illumination_elevation_angle, max_illumination_elevation_angle = float(min_illumination_elevation_angle), float(max_illumination_elevation_angle)
            logger.debug(f"Illumination elevation angle filters: {min_illumination_elevation_angle} to {max_illumination_elevation_angle}")
            illumination_elevation_angle_filters = Q(illumination_elevation_angle__gte=min_illumination_elevation_angle, illumination_elevation_angle__lte=max_illumination_elevation_angle)
            filters &= illumination_elevation_angle_filters

            if min_illumination_elevation_angle == -1:
                filters |= Q(illumination_elevation_angle__isnull=True)

        if min_holdback_seconds is not None and max_holdback_seconds is not None:
            min_holdback_seconds, max_holdback_seconds = int(min_holdback_seconds), int(max_holdback_seconds)
            logger.debug(f"Holdback seconds filters: {min_holdback_seconds} to {max_holdback_seconds}")
            holdback_seconds_filters = Q(holdback_seconds__gte=min_holdback_seconds, holdback_seconds__lte=max_holdback_seconds)
            filters &= holdback_seconds_filters

            if min_holdback_seconds == -1:
                filters |= Q(holdback_seconds__isnull=True)

        if user_timezone and user_duration_type:
            selected_durations = [d.strip() for d in user_duration_type.split(",") if d.strip()]
            time_filters = Q()
            for duration in selected_durations:
                start_hour_utc, end_hour_utc = get_utc_time_range(duration, user_timezone)
                logger.debug(f"User Timezone: {user_timezone}, User Duration Type: {user_duration_type}, Start Hour: {start_hour_utc}, End Hour: {end_hour_utc}")
                if start_hour_utc < end_hour_utc:
                    time_filters |= Q(acquisition_datetime__time__gte=time(start_hour_utc, 0)) & Q(acquisition_datetime__time__lt=time(end_hour_utc, 0))
                else:
                    # Overnight case (crosses midnight)
                    time_filters |= Q(acquisition_datetime__time__gte=time(start_hour_utc, 0)) | Q(acquisition_datetime__time__lt=time(end_hour_utc, 0))

            filters &= time_filters

        if vendor_name and "," in vendor_name:
            vendor_names = vendor_name.split(",")
            filters &= Q(vendor_name__in=vendor_names)
        elif vendor_name:
            filters &= Q(vendor_name=vendor_name)

        if wkt_polygon:
            # try:
            #     area_response = get_area_from_polygon_wkt(wkt_polygon)
            #     if area_response["status_code"] == 200:
            #         polygon_area = area_response["data"]
            #         if polygon_area > 1000000000:
            #             logger.warning("Area is too large for processing")
            #             return {"data": "Area is too large for processing", "status_code": 400}
            #     else:
            #         logger.warning(f"Failed to calculate area: {area_response['data']}")
            # except Exception as e:
            #     logger.error(f"Error calculating polygon area: {str(e)}")
            logger.debug("Polygon WKT provided")
            wkt_polygon_geom = GEOSGeometry(wkt_polygon)
            filters &= Q(location_polygon__intersects=wkt_polygon_geom)

                    
        if min_cloud_cover is not None and max_cloud_cover is not None:
            min_cloud_cover, max_cloud_cover = float(min_cloud_cover), float(max_cloud_cover)
            logger.debug(f"Cloud cover filters: {type(min_cloud_cover)} to {type(max_cloud_cover)}")
            cloud_cover_filters = (
                Q(~Q(vendor_name__in=[ 'capella', 'skyfi-umbra']), cloud_cover_percent__gte=min_cloud_cover, cloud_cover_percent__lte=max_cloud_cover)
            )

            if min_cloud_cover == -1:
                cloud_cover_filters |= Q(vendor_name__in=["capella", "skyfi-umbra"])

            filters &= cloud_cover_filters

        if min_off_nadir_angle is not None and max_off_nadir_angle is not None:
            logger.debug(f"Sun elevation filters: {min_off_nadir_angle} to {max_off_nadir_angle}")
            min_off_nadir_angle, max_off_nadir_angle = float(min_off_nadir_angle), float(max_off_nadir_angle)
            sun_elevation_filters = Q(sun_elevation__gte=min_off_nadir_angle, sun_elevation__lte=max_off_nadir_angle)
            filters &= sun_elevation_filters

        if min_gsd is not None and max_gsd is not None:
            logger.debug(f"GSD filters: {min_gsd} to {max_gsd}")
            min_gsd, max_gsd = float(min_gsd), float(max_gsd)
            gsd_filters = Q(gsd__gte=min_gsd, gsd__lte=max_gsd)
            filters &= gsd_filters

        if vendor_id:
            filters &= Q(vendor_id=vendor_id)

        # Prioritized records first
        focused_captures = []
        if focused_records_ids:
            try:
                focused_ids = [int(id.strip()) for id in focused_records_ids.split(",")]
                focused_captures = captures.filter(id__in=focused_ids)

                focused_captures = (
                        focused_captures.order_by(sort_by)
                        if sort_order == "asc"
                        else focused_captures.order_by(f"-{sort_by}")
                    )

                logger.debug(f"Focused Records IDs: {focused_ids}, Found {len(focused_captures)} records")
            except Exception as e:
                logger.error(f"Error processing focused record IDs: {str(e)}")
                return {"data": str(e), "status_code": 400}

        zoomed_captures = []
        if zoomed_wkt:
            try:
                zoomed_geom = GEOSGeometry(zoomed_wkt)
                zoomed_filters = filters & Q(location_polygon__intersects=zoomed_geom)
                zoomed_captures = captures.filter(zoomed_filters).exclude(id__in=[record.id for record in focused_captures])

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

        excluded_ids = [record.id for record in zoomed_captures] + [record.id for record in focused_captures]
        print(filters)
        captures = captures.filter(filters).exclude(id__in=excluded_ids)
        if sort_by and sort_order:
            captures = (
                        captures.order_by(sort_by)
                        if sort_order == "asc"
                        else captures.order_by(f"-{sort_by}")
                    )

        regular_captures_count = captures.count()                
        
        if focused_captures:
            combined_captures = list(chain(focused_captures, zoomed_captures, captures))
        elif zoomed_captures:
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
            "focused_captures_count": len(focused_captures),
            "regular_captures_count": regular_captures_count,
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
    current_count = CollectionCatalog.objects.filter(
        location_polygon__intersects=buffered_polygon,
        acquisition_datetime__gte=start_time
    ).count()

    previous_duration_start = start_time - timedelta(days=days)
    previous_count = CollectionCatalog.objects.filter(
        location_polygon__intersects=buffered_polygon,
        acquisition_datetime__gte=previous_duration_start,
        acquisition_datetime__lt=start_time
    ).count()
    percentage_change = calculate_percentage_change(current_count, previous_count)
    return days, current_count, previous_count,  percentage_change

def get_site_and_group_name_by_site_id(site_id:int):
    data = {
        "site_name": "",
        "site_id": "",
        "group_name": "",
        "group_id": "",
    }
    try:
        group_site = GroupSite.objects.filter(site_id=int(site_id), is_deleted=False).first()
        if not group_site:
            return data
        return {
            "site_name": group_site.site.name,
            "site_id": group_site.site.id,
            "group_name": group_site.group.name,
            "group_id": group_site.group.id,
        }
    except Exception as e:
        logger.error(f"Error fetching site and group name: {str(e)}")
        return data

def get_pin_selection_analytics_and_location(latitude, longitude, distance, site_id=None):
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
        oldest_record_instance = CollectionCatalog.objects.filter(
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

        newest_record_instance = CollectionCatalog.objects.filter(
            location_polygon__intersects=buffered_polygon
        ).order_by("acquisition_datetime").last()

        # newest clear cloud cover info means cloud cover is 0 also there are null values in cloud cover

        newest_clear_cloud_cover_instance = CollectionCatalog.objects.filter(
            location_polygon__intersects=buffered_polygon,
            cloud_cover_percent=0
        ).order_by("-acquisition_datetime").first()


        # Count for each vendor in the selected area airbus, maxar, planet, blacksky, capella, skyfi-umbra

        vendor_counts = CollectionCatalog.objects.filter(
            location_polygon__intersects=buffered_polygon
        ).values('vendor_name').annotate(count=Count('id'))

        vendor_count = {vendor['vendor_name']: vendor['count'] for vendor in vendor_counts}

        total_count = CollectionCatalog.objects.filter(
            location_polygon__intersects=buffered_polygon,
            acquisition_datetime__gte=now() - timedelta(days=90)
        ).count()

        avg_count = total_count / 90

        analytics = {
            "vendor_count": vendor_count,
            "total_count": total_count,
            "average_per_day": avg_count,
            "oldest_date": longest_period_start,
            "oldest_info": OldestInfoSerializer(oldest_record_instance).data if oldest_record_instance else None,
            "newest_info": NewestInfoSerializer(newest_record_instance).data if newest_record_instance else None,
            "newest_clear_cloud_cover_info": NewestInfoSerializer(newest_clear_cloud_cover_instance).data if newest_clear_cloud_cover_instance else None,
            "address": address_response["data"],
            "percentages": results
        }

        site_details = get_site_and_group_name_by_site_id(site_id)
        analytics["site_details"] = site_details

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
        oldest_record_instance = CollectionCatalog.objects.filter(
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

        total_count = CollectionCatalog.objects.filter(
            location_polygon__intersects=polygon,
            acquisition_datetime__gte=now() - timedelta(days=90)
        ).count()
        avg_count = total_count / 90

        # Get the newest image record (preferably clear cloud cover if available)
        newest_record_instance = CollectionCatalog.objects.filter(
            location_polygon__intersects=polygon
        ).order_by("acquisition_datetime").last()

        # Clear cloud cover info if available
        newest_clear_cloud_cover_instance = CollectionCatalog.objects.filter(
            location_polygon__intersects=polygon,
            cloud_cover_percent=0
        ).order_by("-acquisition_datetime").first()

        analytics = {
            "total_count": total_count,
            "average_per_day": avg_count,
            "oldest_date": longest_period_start,
            "oldest_info": OldestInfoSerializer(oldest_record_instance).data if oldest_record_instance else None,
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


def get_polygon_selection_acquisition_calender_days_frequency(
    polygon_wkt: str,
    start_date: str,
    end_date: str,
    vendor_id: str = None,
    vendor_name: str = None,
    min_cloud_cover: float = None,
    max_cloud_cover: float = None,
    min_off_nadir_angle: float = None,
    max_off_nadir_angle: float = None,
    min_gsd: float = None,
    max_gsd: float = None,
    user_timezone: str = None,
    user_duration_type: str = None,
    min_azimuth_angle: float = None,
    max_azimuth_angle: float = None,
    min_illumination_azimuth_angle: float = None,
    max_illumination_azimuth_angle: float = None,
    min_illumination_elevation_angle: float = None,
    max_illumination_elevation_angle: float = None,
    min_holdback_seconds: int = None,
    max_holdback_seconds: int = None,

):
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
        print(start_date, end_date)
        # Start time tracking
        func_start_time = now()

        # Convert WKT string to a Polygon object
        polygon = fromstr(polygon_wkt)
        logger.debug(f"Polygon WKT: {polygon_wkt}")

        filters = Q()

        if start_date:
            filters &= Q(acquisition_datetime__gte=start_date)
        if end_date:
            filters &= Q(acquisition_datetime__lte=end_date)

        if vendor_id:
            filters &= Q(vendor_id=vendor_id)

        if vendor_name and "," in vendor_name:
            vendor_names = vendor_name.split(",")
            filters &= Q(vendor_name__in=vendor_names)
        elif vendor_name:
            filters &= Q(vendor_name=vendor_name)

        if user_timezone and user_duration_type:
            selected_durations = [d.strip() for d in user_duration_type.split(",") if d.strip()]
            time_filters = Q()
            for duration in selected_durations:
                start_hour_utc, end_hour_utc = get_utc_time_range(duration, user_timezone)
                logger.debug(f"User Timezone: {user_timezone}, User Duration Type: {user_duration_type}, Start Hour: {start_hour_utc}, End Hour: {end_hour_utc}")
                if start_hour_utc < end_hour_utc:
                    time_filters |= Q(acquisition_datetime__time__gte=time(start_hour_utc, 0)) & Q(acquisition_datetime__time__lt=time(end_hour_utc, 0))
                else:
                    # Overnight case (crosses midnight)
                    time_filters |= Q(acquisition_datetime__time__gte=time(start_hour_utc, 0)) | Q(acquisition_datetime__time__lt=time(end_hour_utc, 0))

            filters &= time_filters

        if min_cloud_cover is not None and max_cloud_cover is not None:
            logger.debug(f"Cloud cover filters: {min_cloud_cover} to {max_cloud_cover}")
            min_cloud_cover, max_cloud_cover = float(min_cloud_cover), float(max_cloud_cover)
            cloud_cover_filters = (
                Q(~Q(vendor_name__in=['capella', 'skyfi-umbra']), cloud_cover_percent__gte=min_cloud_cover, cloud_cover_percent__lte=max_cloud_cover)
            )

            if min_cloud_cover == -1:
                cloud_cover_filters |= Q(vendor_name__in=["capella", "skyfi-umbra"])

            filters &= cloud_cover_filters

        if min_off_nadir_angle is not None and max_off_nadir_angle is not None:
            logger.debug(f"Sun elevation filters: {min_off_nadir_angle} to {max_off_nadir_angle}")
            min_off_nadir_angle, max_off_nadir_angle = float(min_off_nadir_angle), float(max_off_nadir_angle)
            sun_elevation_filters = Q(sun_elevation__gte=min_off_nadir_angle, sun_elevation__lte=max_off_nadir_angle)
            filters &= sun_elevation_filters

        if min_gsd is not None and max_gsd is not None:
            logger.debug(f"GSD filters: {min_gsd} to {max_gsd}")
            min_gsd, max_gsd = float(min_gsd), float(max_gsd)
            gsd_filters = Q(gsd__gte=min_gsd, gsd__lte=max_gsd)
            filters &= gsd_filters
        
        if min_azimuth_angle is not None and max_azimuth_angle is not None:
            logger.debug(f"Azimuth angle filters: {min_azimuth_angle} to {max_azimuth_angle}")
            min_azimuth_angle, max_azimuth_angle = float(min_azimuth_angle), float(max_azimuth_angle)
            azimuth_angle_filters = Q(azimuth_angle__gte=min_azimuth_angle, azimuth_angle__lte=max_azimuth_angle)
            filters &= azimuth_angle_filters

            if min_azimuth_angle == -1:
                filters |= Q(azimuth_angle__isnull=True)

        if min_illumination_azimuth_angle is not None and max_illumination_azimuth_angle is not None:
            logger.debug(f"Illumination azimuth angle filters: {min_illumination_azimuth_angle} to {max_illumination_azimuth_angle}")
            min_illumination_azimuth_angle, max_illumination_azimuth_angle = float(min_illumination_azimuth_angle), float(max_illumination_azimuth_angle)
            illumination_azimuth_angle_filters = Q(illumination_azimuth_angle__gte=min_illumination_azimuth_angle, illumination_azimuth_angle__lte=max_illumination_azimuth_angle)
            filters &= illumination_azimuth_angle_filters

            if min_illumination_azimuth_angle == -1:
                filters |= Q(illumination_azimuth_angle__isnull=True)

        if min_illumination_elevation_angle is not None and max_illumination_elevation_angle is not None:
            logger.debug(f"Illumination elevation angle filters: {min_illumination_elevation_angle} to {max_illumination_elevation_angle}")
            min_illumination_elevation_angle, max_illumination_elevation_angle = float(min_illumination_elevation_angle), float(max_illumination_elevation_angle)
            illumination_elevation_angle_filters = Q(illumination_elevation_angle__gte=min_illumination_elevation_angle, illumination_elevation_angle__lte=max_illumination_elevation_angle)
            filters &= illumination_elevation_angle_filters

            if min_illumination_elevation_angle == -1:
                filters |= Q(illumination_elevation_angle__isnull=True)

        if min_holdback_seconds is not None and max_holdback_seconds is not None:
            logger.debug(f"Holdback seconds filters: {min_holdback_seconds} to {max_holdback_seconds}")
            min_holdback_seconds, max_holdback_seconds = int(min_holdback_seconds), int(max_holdback_seconds)
            holdback_seconds_filters = Q(holdback_seconds__gte=min_holdback_seconds, holdback_seconds__lte=max_holdback_seconds)
            filters &= holdback_seconds_filters

            if min_holdback_seconds == -1:
                filters |= Q(holdback_seconds__isnull=True)

        filters &= Q(location_polygon__intersects=polygon)

        # Fetch frequency data directly from the database
        frequency_data = (
            CollectionCatalog.objects.filter(
                filters
            )
            .annotate(date=TruncDate('acquisition_datetime'))  # Extract the date part
            .values('date')  # Group by the date
            .annotate(count=Count('id'))  # Count captures for each date
            .order_by('date')  # Sort by date
        )
        if "T" in start_date:
            start_date = start_date.split("T")[0]
        if "T" in end_date:
            end_date = end_date.split("T")[0]
        # Convert QuerySet to dictionary
        frequency_dict = {entry['date'].strftime("%Y-%m-%d"): entry['count'] for entry in frequency_data}
        full_date_range = {}
        current_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

        while current_date <= end_date:
            full_date_range[current_date.strftime("%Y-%m-%d")] = frequency_dict.get(current_date.strftime("%Y-%m-%d"), 0)
            current_date += timedelta(days=1)

        # Calculate time taken
        func_end_time = now()
        net_time = func_end_time - func_start_time
        logger.info(f"Frequency calculation completed in {net_time} seconds.")

        return {
            "data": full_date_range,
            "time_taken": str(net_time),
            "status_code": 200,
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
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
            CollectionCatalog.objects.filter(
                location_polygon__intersects=buffered_polygon,
                acquisition_datetime__gte=start_date,
                acquisition_datetime__lte=end_date
            )
            .annotate(date=TruncDate('acquisition_datetime'))  # Extract the date part
            .values('date')  # Group by the date
            .annotate(count=Count('id'))  # Count captures for each date
            .order_by('date')  # Sort by date
        )
        if "T" in start_date:
            start_date = start_date.split("T")[0]
        if "T" in end_date:
            end_date = end_date.split("T")[0]

        # Convert QuerySet to dictionary

        frequency_dict = {entry['date'].strftime("%Y-%m-%d"): entry['count'] for entry in frequency_data}
        full_date_range = {}
        current_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

        while current_date <= end_date:
            full_date_range[current_date.strftime("%Y-%m-%d")] = frequency_dict.get(current_date.strftime("%Y-%m-%d"), 0)
            current_date += timedelta(days=1)

        # Calculate time taken
        func_end_time = now()
        net_time = func_end_time - func_start_time
        logger.info(f"Frequency calculation completed in {net_time} seconds.")

        return {
            "data": full_date_range,
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


def get_weather_details_from_tommorrow_third_party():
    """
    Retrieve weather details for a specific date.

    Args:
        date (str): Date in the format "YYYY-MM-DD".

    Returns:
        dict: A dictionary containing weather details.
    """
    try:
        logger.info("Inside get weather details service")
        weather_api_key = config("TOMMORROW_WEATHER_API_KEY")        
        url = f"https://api.tomorrow.io/v4/historical?apikey={weather_api_key}"
        payload = {
            "location": "42.3478, -71.0466",
            "fields": ["temperature"],
            "timesteps": ["1d"],
            "startTime": "2025-02-01",
            "endTime": "2025-02-02",
            "units": "metric"
        }
        headers = {
            "accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "content-type": "application/json"
        }
        response = requests.post(url, json=payload, headers=headers)
        response_data = response.json()
        if response.status_code == 403:
            return {"data": response_data["message"], "status_code": 403, "error": response_data["message"]}
        if response.status_code != 200:
            return {"data": response_data, "status_code": response.status_code, "error": response_data}


        logger.info("Weather details fetched successfully")
        return {"data": response_data, "status_code": 200}
    except Exception as e:
        logger.error(f"Error fetching weather details: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 500, "error": f"Error: {str(e)}"}