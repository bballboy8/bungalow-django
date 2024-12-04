from django.contrib.gis.geos import GEOSGeometry
from core.models import SatelliteCaptureCatalog
from shapely.geometry import shape
from logging_module import logger
from django.contrib.gis.geos import Point
from django.contrib.gis.measure import D
from django.core.paginator import Paginator
from django.db.models import Q


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

        captures = captures.filter(filters)

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