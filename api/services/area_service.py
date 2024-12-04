from django.contrib.gis.geos import GEOSGeometry
from core.models import SatelliteCaptureCatalog


def filter_by_polygon(wkt_polygon):
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

        return {"data": captures, "status_code": 200}
    except Exception as e:
        return {"data": f"{str(e)}", "status_code": 400}
