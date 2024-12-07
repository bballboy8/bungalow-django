from django.contrib.gis.db import models
from django.db import models as plane_models
from django.utils.timezone import now


SITE_TYPE_CHOICES = (
    ("Point", "Point"),
    ("Rectangle", "Rectangle"),
    ("Polygon", "Polygon"),
)

class Site(models.Model):
    name = models.CharField(max_length=255, unique=True)
    location_polygon = models.PolygonField()
    coordinates_record = models.JSONField(null=True, blank=True)
    site_type = models.CharField(
        max_length=10, choices=SITE_TYPE_CHOICES, default="Polygon"
    )
    created_at = models.DateTimeField(default=now, editable=False)
    updated_at = models.DateTimeField(default=now)

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

    class Meta:
        unique_together = ("group", "site")

    def __str__(self):
        return f"{self.group.name} - {self.site.name}"
