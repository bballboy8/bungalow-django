from rest_framework import serializers
from core.models import SatelliteCaptureCatalog
from shapely.geometry import shape
from pyproj import Geod
import shapely.wkt

def get_area(geometry):
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
        return { "area": area, "status_code": 200}
    except Exception as e:
        return {"data": f"{str(e)}", "status_code": 400}

class SatelliteCatalogFilterSerializer(serializers.Serializer):
    wkt_polygon = serializers.CharField(default="")


class GeoJSONToWKTSerializer(serializers.Serializer):
    geometry = serializers.JSONField()


class SatelliteCaptureCatalogListSerializer(serializers.ModelSerializer):
    presigned_url = serializers.CharField(required=False, allow_blank=True)
    address = serializers.CharField(required=False, allow_blank=True)
    class Meta:
        model = SatelliteCaptureCatalog
        exclude = ["location_polygon", "created_at", "updated_at"]

    def to_representation(self, instance):
        data = super().to_representation(instance)
        if 'cloud_cover' in data and data['cloud_cover'] <= 1:
            data['cloud_cover'] *= 100   
        if "coordinates_record" in data:
            response = get_area(data["coordinates_record"])
            if response["status_code"] == 200:
                data["area"] = response["area"]
        return data

class SatelliteCaptureImageByIdAndVendorSerializer(serializers.Serializer):
    record = serializers.JSONField(default=[{"id": "", "vendor": ""}])


class PinSelectionAnalyticsAndLocationSerializer(serializers.Serializer):
    latitude = serializers.FloatField()
    longitude = serializers.FloatField()
    distance = serializers.FloatField()

class NewestInfoSerializer(serializers.Serializer):
    # id, vendor, vendor_id, acquisition_datetime, cloud_coverage
    id = serializers.CharField()
    vendor_name = serializers.CharField()
    vendor_id = serializers.CharField()
    acquisition_datetime = serializers.DateTimeField()
    cloud_cover = serializers.FloatField()

class PolygonSelectionAnalyticsAndLocationSerializer(serializers.Serializer):
    polygon_wkt = serializers.CharField()


class AreaFromPolygonWktSerializer(serializers.Serializer):
    polygon_wkt = serializers.CharField()


class GenerateCirclePolygonSerializer(serializers.Serializer):
    latitude = serializers.FloatField(help_text="Center latitude of the circle.")
    longitude = serializers.FloatField(help_text="Center longitude of the circle.")
    distance_km = serializers.FloatField(help_text="Radius of the circle in kilometers.")


class ExtractCircleParametersSerializer(serializers.Serializer):
    geojson_polygon = serializers.JSONField(help_text="GeoJSON Polygon representing a circle.")