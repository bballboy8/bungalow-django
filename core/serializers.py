# serializers.py

from rest_framework import serializers
from .models import CollectionCatalog, SatelliteDateRetrievalPipelineHistory, SatelliteCaptureCatalogMetadata, CollectionCatalog
from django.contrib.gis.geos import Polygon
import hashlib
import json

class SatelliteCaptureCatalogSerializer(serializers.ModelSerializer):
    class Meta:
        model = CollectionCatalog
        fields = '__all__'

    def validate_location_polygon(self, value):
        if isinstance(value, dict) and value.get("type") == "Polygon":
            try:
                return Polygon(value["coordinates"][0])
            except (TypeError, ValueError):
                raise serializers.ValidationError("Invalid coordinates for Polygon.")
        raise serializers.ValidationError("location_polygon must be a valid GeoJSON Polygon.")

    def create(self, validated_data):
        coordinates_record = validated_data.get("coordinates_record")
        if isinstance(coordinates_record, dict):
            validated_data["location_polygon"] = self.validate_location_polygon(coordinates_record)
            centroid = validated_data["location_polygon"].centroid
            validated_data["geometryCentroid_lat"] = centroid.y
            validated_data["geometryCentroid_lon"] = centroid.x

        # Generate MD5 hash of coordinates_record
        coordinates_record_md5 = hashlib.md5(json.dumps(coordinates_record, sort_keys=True).encode()).hexdigest()
        validated_data["coordinates_record_md5"] = coordinates_record_md5

        # Check for duplicate record before saving
        existing_record = CollectionCatalog.objects.filter(
            acquisition_datetime=validated_data["acquisition_datetime"],
            coordinates_record_md5=coordinates_record_md5
        ).first()

        if existing_record:
            raise serializers.ValidationError(f"A record with this acquisition_datetime and coordinates_record already exists. ID: {existing_record.id} {validated_data.get('vendor_id')}")
        
        # check for duplicate vendor_id
        vendor_id = validated_data.get("vendor_id")
        if vendor_id and CollectionCatalog.objects.filter(vendor_id=vendor_id).exists():
            raise serializers.ValidationError(f"A record with this vendor_id already exists. {vendor_id}")
            
        return super().create(validated_data)

    def validate_vendor_id(self, value):
        if CollectionCatalog.objects.filter(vendor_id=value).exists():
            raise serializers.ValidationError(f"A record with this vendor_id already exists. {value}")
        return value

class SatelliteDateRetrievalPipelineHistorySerializer(serializers.ModelSerializer):
    class Meta:
        model = SatelliteDateRetrievalPipelineHistory
        fields = '__all__'

class SatelliteCaptureCatalogMetadataSerializer(serializers.ModelSerializer):
    class Meta:
        model = SatelliteCaptureCatalogMetadata
        fields = '__all__'

class CollectionCatalogSerializer(serializers.ModelSerializer):
    class Meta:
        model = CollectionCatalog
        fields = '__all__'

    def validate_location_polygon(self, value):
        if isinstance(value, dict) and value.get("type") == "Polygon":
            try:
                return Polygon(value["coordinates"][0])
            except (TypeError, ValueError):
                raise serializers.ValidationError("Invalid coordinates for Polygon.")
        raise serializers.ValidationError("location_polygon must be a valid GeoJSON Polygon.")

    def create(self, validated_data):
        coordinates_record = validated_data.get("coordinates_record")
        if isinstance(coordinates_record, dict):
            validated_data["location_polygon"] = self.validate_location_polygon(coordinates_record)
            centroid = validated_data["location_polygon"].centroid
            validated_data["geometryCentroid_lat"] = centroid.y
            validated_data["geometryCentroid_lon"] = centroid.x

        # Generate MD5 hash of coordinates_record
        coordinates_record_md5 = hashlib.md5(json.dumps(coordinates_record, sort_keys=True).encode()).hexdigest()
        validated_data["coordinates_record_md5"] = coordinates_record_md5

        # Check for duplicate record before saving
        existing_record = CollectionCatalog.objects.filter(
            acquisition_datetime=validated_data["acquisition_datetime"],
            coordinates_record_md5=coordinates_record_md5
        ).first()

        if existing_record:
            raise serializers.ValidationError(f"A record with this acquisition_datetime and coordinates_record already exists. ID: {existing_record.id} {validated_data.get('vendor_id')}")
        
        # check for duplicate vendor_id
        vendor_id = validated_data.get("vendor_id")
        if vendor_id and CollectionCatalog.objects.filter(vendor_id=vendor_id).exists():
            raise serializers.ValidationError(f"A record with this vendor_id already exists. {vendor_id}")
            
        return super().create(validated_data)

    def validate_vendor_id(self, value):
        if CollectionCatalog.objects.filter(vendor_id=value).exists():
            raise serializers.ValidationError(f"A record with this vendor_id already exists. {value}")
        return value