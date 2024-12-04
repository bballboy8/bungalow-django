from rest_framework import serializers


class SatelliteCatalogFilterSerializer(serializers.Serializer):
    wkt_polygon = serializers.CharField()


class GeoJSONToWKTSerializer(serializers.Serializer):
    geometry = serializers.JSONField()