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



def convert_geojson_to_wkt(geometry):
    logger.info("Inside convert GeoJSON to WKT service")
    try:
        try:
            polygon = shape(geometry)
        except Exception as e:
            return {"data": f"Invalid GeoJSON: {str(e)}", "status_code": 400}

        logger.info("GeoJSON converted to WKT successfully")
        return {"data": polygon.wkt, "status_code": 200}
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

        captures = captures.filter(filters).order_by('acquisition_datetime')

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

        # Create a geospatial point object from the provided latitude and longitude
        point = Point(longitude, latitude, srid=4326)
        buffered_polygon = point.buffer(distance / 111.32)  # 1 degree ≈ 111.32 km

        # Get the start time based on the selected duration
        start_time = datetime.now() - timedelta(days=duration)

        logger.info(f"Latitude: {latitude}, Longitude: {longitude}, Distance: {distance}, Duration: {duration}")

        # Construct the query filtering by location and the selected time range
        captures = SatelliteCaptureCatalog.objects.filter(
                location_polygon__intersects=buffered_polygon ,   # Contained polygons
                acquisition_datetime__gte=start_time              # Time range filter
            )

        # Return the analytics (count of captures)
        analytics = {
            "count": captures.count(),
            "average_per_day": captures.count() / duration,
            "oldest_date": captures.order_by("acquisition_datetime").first(),
            "newest_date": captures.order_by("acquisition_datetime").last(),
        }

        if analytics["oldest_date"]:
            analytics["oldest_date"] = analytics["oldest_date"].acquisition_datetime
        if analytics["newest_date"]:
            analytics["newest_date"] = analytics["newest_date"].acquisition_datetime

        func_end_time = datetime.now()
        logger.info(f"Time taken to fetch pin selection analytics and location: {func_end_time - func_start_time}")

        logger.info("Pin selection analytics and location fetched successfully")
        return {
            "data": {"analytics": analytics},
            "status_code": 200,
        }

    except Exception as e:
        logger.error(f"Error fetching pin selection analytics and location: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 400}
    
