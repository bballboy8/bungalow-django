from rest_framework import serializers
from api.models import Group, Site, GroupSite


class SiteSerializer(serializers.ModelSerializer):
    class Meta:
        model = Site
        fields = ["id", "name", "location_polygon", "created_at", "updated_at"]


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


class GroupSiteSerializer(serializers.ModelSerializer):
    group = GroupSerializer()
    site = SiteSerializer()

    class Meta:
        model = GroupSite
        fields = ["id", "group", "site", "assigned_at"]
