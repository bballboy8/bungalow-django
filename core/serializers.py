# serializers.py

from rest_framework import serializers
from .models import SatelliteCaptureCatalog, SatelliteDateRetrievalPipelineHistory
from django.contrib.gis.geos import Polygon

class SatelliteCaptureCatalogSerializer(serializers.ModelSerializer):
    type_of_day = serializers.ReadOnlyField()  # Expose the type_of_day property

    class Meta:
        model = SatelliteCaptureCatalog
        fields = '__all__'
    
    def validate_location_polygon(self, value):
        if isinstance(value, dict) and value.get("type") == "Polygon":
            try:
                return Polygon(value["coordinates"][0])
            except (TypeError, ValueError):
                raise serializers.ValidationError("Invalid coordinates for Polygon.")
        raise serializers.ValidationError("location_polygon must be a valid GeoJSON Polygon.")

    def create(self, validated_data):
        location_polygon = validated_data.get("location_polygon")
        if isinstance(location_polygon, dict):
            validated_data["location_polygon"] = self.validate_location_polygon(location_polygon)
        return super().create(validated_data)

    # def validate_vendor_id(self, value):
    #     if SatelliteCaptureCatalog.objects.filter(vendor_id=value).exists():
    #         raise serializers.ValidationError("A record with this vendor_id already exists.")
    #     return value
    

class SatelliteDateRetrievalPipelineHistorySerializer(serializers.ModelSerializer):
    class Meta:
        model = SatelliteDateRetrievalPipelineHistory
        fields = '__all__'