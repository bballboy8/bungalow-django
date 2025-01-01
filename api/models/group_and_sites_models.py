from django.contrib.gis.db import models
from django.db import models as plane_models
from django.utils.timezone import now
from django.contrib.auth.models import User


SITE_TYPE_CHOICES = (
    ("Point", "Point"),
    ("Rectangle", "Rectangle"),
    ("Polygon", "Polygon"),
)

class Site(models.Model):
    name = models.CharField(max_length=255, unique=True)
    location_polygon = models.PolygonField()
    coordinates_record = models.JSONField(null=True, blank=True)
    site_area = models.FloatField(null=True, blank=True)
    site_type = models.CharField(
        max_length=10, choices=SITE_TYPE_CHOICES, default="Polygon"
    )
    created_at = models.DateTimeField(default=now, editable=False)
    updated_at = models.DateTimeField(default=now)
    user = models.ForeignKey(User, on_delete=models.CASCADE, default=1)
    is_deleted = models.BooleanField(default=False)
    notification = models.BooleanField(default=False)


    def __str__(self):
        return self.name


class Group(plane_models.Model):
    name = plane_models.CharField(max_length=255, unique=True)
    parent = plane_models.ForeignKey(
        "self",
        null=True,
        blank=True,
        related_name="subgroups",
        on_delete=plane_models.CASCADE,
    )
    description = plane_models.TextField(blank=True, null=True)
    created_at = plane_models.DateTimeField(default=now, editable=False)
    updated_at = plane_models.DateTimeField(default=now)
    user = plane_models.ForeignKey(User, on_delete=plane_models.CASCADE, default=1)
    is_deleted = plane_models.BooleanField(default=False)
    notification = plane_models.BooleanField(default=False)

    def __str__(self):
        return self.name


class GroupSite(plane_models.Model):
    group = plane_models.ForeignKey(
        Group, on_delete=plane_models.CASCADE, related_name="group_sites"
    )
    site = plane_models.ForeignKey(
        Site, on_delete=plane_models.CASCADE, related_name="site_groups"
    )
    assigned_at = models.DateTimeField(default=now)
    site_area = models.FloatField(null=True, blank=True)
    user = plane_models.ForeignKey(User, on_delete=plane_models.CASCADE, default=1)
    is_deleted = plane_models.BooleanField(default=False)

    class Meta:
        unique_together = ("group", "site")

    def __str__(self):
        return f"{self.group.name} - {self.site.name}"
