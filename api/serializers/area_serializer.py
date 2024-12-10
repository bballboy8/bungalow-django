from rest_framework import serializers
from core.models import SatelliteCaptureCatalog

class SatelliteCatalogFilterSerializer(serializers.Serializer):
    wkt_polygon = serializers.CharField(default="")


class GeoJSONToWKTSerializer(serializers.Serializer):
    geometry = serializers.JSONField()


class SatelliteCaptureCatalogSerializer(serializers.ModelSerializer):
    presigned_url = serializers.CharField(required=False, allow_blank=True)
    class Meta:
        model = SatelliteCaptureCatalog
        exclude = ["location_polygon", "created_at", "updated_at"]


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