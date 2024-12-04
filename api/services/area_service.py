from django.contrib.gis.geos import GEOSGeometry
from core.models import SatelliteCaptureCatalog
from shapely.geometry import shape
from logging_module import logger


def filter_by_polygon(wkt_polygon):
    logger.info("Inside filter records by polygon service")
    try:
        try:
            polygon = GEOSGeometry(wkt_polygon)
        except Exception as e:
            return {"data": f"Invalid WKT polygon: {str(e)}", "status_code": 400}

        captures = SatelliteCaptureCatalog.objects.filter(
            location_polygon__intersects=polygon
        )

        if not captures:
            return {"data": [], "status_code": 200}

        logger.info("Records filtered successfully")
        return {"data": captures, "status_code": 200}
    except Exception as e:
        logger.error(f"Error filtering records by polygon: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 400}


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
