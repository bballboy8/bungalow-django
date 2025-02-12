from rest_framework import serializers
from api.models import Group, Site, GroupSite
from django.contrib.gis.geos import Polygon
from django.contrib.auth.models import User
from logging_module import logger
from shapely.geometry import shape
from pyproj import Geod
import shapely.wkt

def convert_geojson_to_wkt(geometry):
    logger.info("Inside convert GeoJSON to WKT service")
    try:
        try:
            polygon = shape(geometry)
            wkt = polygon.wkt
        except Exception as e:
            return {"data": [], "status_code": 400, "error": f"Invalid GeoJSON: {str(e)}"}
        
        try:
            geod = Geod(ellps="WGS84")
            polygon = shapely.wkt.loads(wkt)
            area = round(abs(geod.geometry_area_perimeter(polygon)[0]) / 1000000.0, 2)
        except Exception as e:
            return {"data": [], "status_code": 400, "error": f"Error calculating area from GeoJSON: {str(e)}"}

        logger.info("GeoJSON converted to WKT successfully")
        return {"data": wkt, "area": area, "status_code": 200}
    except Exception as e:
        logger.error(f"Error converting GeoJSON to WKT: {str(e)}")
        return {"data": [], "status_code": 400, "error": f"Error: {str(e)}"}


def get_area_from_geojson(geometry):
    """
    Calculate the area of a site from its GeoJSON coordinates record.
    """
    try:
        response = convert_geojson_to_wkt(geometry)
        geod = Geod(ellps="WGS84")
        polygon = shapely.wkt.loads(response["data"])
        area = round(abs(geod.geometry_area_perimeter(polygon)[0]) / 1000000.0, 2)
        return {"area": area, "status_code": 200}
    except Exception as e:
        logger.error(f"Error calculating area from GeoJSON: {str(e)}")
        return {"area": 0, "status_code": 500, "error": f"Error calculating area from GeoJSON: {str(e)}"}

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
            "notification",
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

        # Access user_id from context
        user_id = self.context.get("user_id")
        user_id = User.objects.get(id=user_id)
        validated_data["user"] = user_id
        
        return super().create(validated_data)
    
class GetSiteSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    name = serializers.CharField()
    acquisition_count = serializers.IntegerField()
    most_recent = serializers.DateTimeField()
    frequency = serializers.FloatField()
    gap = serializers.FloatField()
    most_recent_clear = serializers.DateTimeField()
    heatmap = serializers.JSONField()
    site_type = serializers.CharField()
    notification = serializers.BooleanField()

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
            "notification",
        ]

    def get_subgroups(self, obj):
        """
        Retrieve subgroups for n-level nesting.
        """
        subgroups = obj.subgroups.all()
        return GroupSerializer(subgroups, many=True).data
    
    def create(self, validated_data):
        user_id = self.context.get("user_id")  # Access user_id from context
        user = User.objects.get(id=user_id)
        group = Group.objects.create(user=user, **validated_data)
        return group
    
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
    notification = serializers.BooleanField(default=False)

class UpdateSiteSerializer(serializers.Serializer):
    site_id = serializers.IntegerField()
    name = serializers.CharField()
    notification = serializers.BooleanField(default=False)
    is_deleted = serializers.BooleanField(default=False)

    def update(self, instance, validated_data):
        for attr, value in validated_data.items():
            setattr(instance, attr, value)
        instance.save()
        return instance


class AddGroupSiteSerializer(serializers.Serializer):
    group_id = serializers.IntegerField()
    site_id = serializers.IntegerField()

class RemoveGroupSiteSerializer(serializers.Serializer):
    group_site_id = serializers.IntegerField()


class AddGroupSerializer(serializers.Serializer):
    name = serializers.CharField()
    parent = serializers.IntegerField(required=False)
    notification = serializers.BooleanField(default=False)

class UpdateGroupSerializer(serializers.Serializer):
    group_id = serializers.IntegerField()
    name = serializers.CharField()
    notification = serializers.BooleanField(default=False)
    is_deleted = serializers.BooleanField(default=False)

    def update(self, instance, validated_data):
        for attr, value in validated_data.items():
            setattr(instance, attr, value)
        instance.save()
        return instance

class AreaFromGeoJsonSerializer(serializers.Serializer):
    coordinates_record = serializers.JSONField(
        default={"type": "Polygon", "coordinates": []}
    )
class UploadFileSerializer(serializers.Serializer):
    file = serializers.FileField()

class UploadCSVResponseSerializer(serializers.Serializer):
    row_number = serializers.IntegerField()
    row_name = serializers.CharField()
    status = serializers.CharField()
    reason = serializers.CharField(required=False)