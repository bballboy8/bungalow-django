from rest_framework import serializers
from api.models import Group, Site, GroupSite
from django.contrib.gis.geos import Polygon
from api.services.group_and_sites_service import get_area_from_geojson

class SiteSerializer(serializers.ModelSerializer):
    class Meta:
        model = Site
        fields = [
            "id",
            "name",
            "created_at",
            "updated_at",
            "site_type",
            "coordinates_record",
        ]

    def validate_location_polygon(self, value):
        if isinstance(value, dict) and value.get("type") == "Polygon":
            try:
                return Polygon(value["coordinates"][0])
            except (TypeError, ValueError):
                raise serializers.ValidationError("Invalid coordinates for Polygon.")
        raise serializers.ValidationError(
            "location_polygon must be a valid GeoJSON Polygon."
        )

    def create(self, validated_data):
        location_polygon = validated_data.get("coordinates_record")
        if isinstance(location_polygon, dict):
            validated_data["location_polygon"] = self.validate_location_polygon(
                location_polygon
            )

        if validated_data.get("coordinates_record"):
            validated_data["site_area"] = get_area_from_geojson(
                validated_data["coordinates_record"]
            )["area"]
        
        return super().create(validated_data)


class GroupSerializer(serializers.ModelSerializer):
    subgroups = serializers.SerializerMethodField()

    class Meta:
        model = Group
        fields = [
            "id",
            "name",
            "parent",
            "created_at",
            "updated_at",
            "subgroups",
        ]

    def get_subgroups(self, obj):
        """
        Retrieve subgroups for n-level nesting.
        """
        subgroups = obj.subgroups.all()
        return GroupSerializer(subgroups, many=True).data
    
class ParentGroupSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    name = serializers.CharField()
    created_at = serializers.DateTimeField()
    surface_area = serializers.FloatField()
    total_objects = serializers.IntegerField()


class GroupSiteSerializer(serializers.ModelSerializer):
    group = GroupSerializer()
    site = SiteSerializer()

    class Meta:
        model = GroupSite
        fields = ["id", "group", "site", "assigned_at"]


class AddSiteSerializer(serializers.Serializer):
    name = serializers.CharField()
    coordinates_record = serializers.JSONField(
        default={"type": "Polygon", "coordinates": []}
    )
    site_type = serializers.CharField(default="Polygon")


class AddGroupSiteSerializer(serializers.Serializer):
    group_id = serializers.IntegerField()
    site_id = serializers.IntegerField()


class AddGroupSerializer(serializers.Serializer):
    name = serializers.CharField()
    parent = serializers.IntegerField(required=False)

class AreaFromGeoJsonSerializer(serializers.Serializer):
    coordinates_record = serializers.JSONField(
        default={"type": "Polygon", "coordinates": []}
    )