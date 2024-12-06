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
from core.services.utils import calculate_area_from_geojson
from api.serializers.area_serializer import NewestInfoSerializer
from decouple import config
import requests


def convert_geojson_to_wkt(geometry):
    logger.info("Inside convert GeoJSON to WKT service")
    try:
        try:
            polygon = shape(geometry)
        except Exception as e:
            return {"data": f"Invalid GeoJSON: {str(e)}", "status_code": 400}
        
        try:
            area = calculate_area_from_geojson(geometry, "geojson_to_wkt")
        except Exception as e:
            return {"data": f"Error calculating area from GeoJSON: {str(e)}", "status_code": 400}

        logger.info("GeoJSON converted to WKT successfully")
        return {"data": polygon.wkt, "area": area, "status_code": 200}
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
):
    logger.info("Inside get satellite records service")
    
    try:
        captures = SatelliteCaptureCatalog.objects.all()

        filters = Q()
        
        if wkt_polygon:
            polygon = GEOSGeometry(wkt_polygon)
            filters &= Q(location_polygon__intersects=polygon)
        
        if latitude and longitude and distance:
            point = Point(longitude, latitude, srid=4326)
            filters &= Q(location_polygon__distance_lte=(point, D(km=distance)))

        if start_date:
            filters &= Q(acquisition_datetime__gte=start_date)
        
        if end_date:
            filters &= Q(acquisition_datetime__lte=end_date)

        captures = captures.filter(filters).order_by('-acquisition_datetime')

        paginator = Paginator(captures, page_size)
        page = paginator.get_page(page_number)

        logger.info("Satellite records fetched successfully")
        return {
            "data": list(page),
            "total_records": paginator.count,
            "page_number": page_number,
            "page_size": page_size,
            "status_code": 200
        }
    
    except Exception as e:
        logger.error(f"Error fetching satellite records: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 400}
    
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



def get_pin_selection_analytics_and_location(latitude: float, longitude: float, distance: float, duration: int = 1):
    """
    Get the analytics and location of the selected pin.

    Args:
        latitude (float): Latitude of the selected pin.
        longitude (float): Longitude of the selected pin.
        distance (float): Distance in kilometers.

    Returns:
        dict: A dictionary containing analytics and location data.
    """
    try:
        # Required oldest date, new date, Total count of images, Average per day
        logger.info("Inside get pin selection analytics and location service")
        func_start_time = datetime.now()

        point = Point(longitude, latitude, srid=4326)
        buffered_polygon = point.buffer(distance / 111.32)  


        logger.info(f"Latitude: {latitude}, Longitude: {longitude}, Distance: {distance}, Duration: {duration}")

        # Get address from latitude and longitude
        address_response = get_address_from_lat_long_via_google_maps(latitude, longitude)
        if address_response["status_code"] != 200:
            return address_response

        start_time = datetime.now() - timedelta(days=duration)
        percentage_change_calcuation_time = start_time - timedelta(days=duration)

        captures = SatelliteCaptureCatalog.objects.filter(
                location_polygon__intersects=buffered_polygon,
                acquisition_datetime__gte=start_time
            )
        
        # Calculate percentage change
        percentage_change_captures = SatelliteCaptureCatalog.objects.filter(
            location_polygon__intersects=buffered_polygon,
            acquisition_datetime__gte=percentage_change_calcuation_time
        )


        percentage = 0

        net_count = captures.count()
        analytics = {
            "count": net_count,
            "average_per_day": net_count / duration,
            "oldest_date": None,
            "newest_info": None,
            "address": address_response["data"]
        }

        percentage_change_count = percentage_change_captures.count()
        if percentage_change_count > 0:
            percentage = ((net_count - percentage_change_count) / percentage_change_count) * 100

        analytics["percentage_change"] = percentage

        oldest_record_instance = captures.order_by("acquisition_datetime").first()
        if oldest_record_instance:
            analytics["oldest_date"] = oldest_record_instance.acquisition_datetime
        

        newest_record_instance = captures.order_by("acquisition_datetime").last()
        if newest_record_instance:
            serializer = NewestInfoSerializer(newest_record_instance)
            analytics["newest_info"] = serializer.data


        func_end_time = datetime.now()
        net_time = func_end_time - func_start_time
        logger.info(f"Time taken to fetch pin selection analytics and location: {net_time}")

        logger.info("Pin selection analytics and location fetched successfully")
        return {
            "data": {"analytics": analytics, "time_taken": str(net_time)},
            "status_code": 200,
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"Error fetching pin selection analytics and location: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 500}
    