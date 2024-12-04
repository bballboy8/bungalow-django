from rest_framework import serializers


class SatelliteCatalogFilterSerializer(serializers.Serializer):
    wkt_polygon = serializers.CharField()
