from rest_framework import serializers
from core.models import SatelliteCaptureCatalog

class SatelliteCatalogFilterSerializer(serializers.Serializer):
    wkt_polygon = serializers.CharField(default="")


class GeoJSONToWKTSerializer(serializers.Serializer):
    geometry = serializers.JSONField()


class SatelliteCaptureCatalogSerializer(serializers.ModelSerializer):
    class Meta:
        model = SatelliteCaptureCatalog
        exclude = ["location_polygon" , "coordinates_record"]