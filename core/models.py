# Create your models here.

from django.contrib.gis.db import models
from django.db import models as plane_models
from django.utils import timezone

class DistinctSatelliteCaptureManager(models.Manager):
    def get_queryset(self):
        # Return the queryset with distinct fields applied
        return super().get_queryset().distinct(
            'acquisition_datetime',
            'vendor_name',
            'sun_elevation',
            'sensor',
            'cloud_cover'
        )

# Constants for choices
VENDOR_CHOICES = [
    ('airbus', 'airbus'),
    ('blacksky', 'blacksky'),
    ('planet', 'planet'),
    ('maxar', 'maxar'),
    ('capella', 'capella'),
    ('skyfi-umbra', 'skyfi-umbra'),
]

# Time ranges for different times of the day
time_ranges = {
    "morning": (5, 11),
    "midday": (11, 16),
    "evening": (16, 21),
    "overnight": (21, 5),
}

class SatelliteCaptureCatalog(models.Model):
    TYPE_CHOICES = [
        ('Day', 'Day'),
        ('Night', 'Night'),
    ]

    acquisition_datetime = models.DateTimeField(null=True, blank=True)
    cloud_cover = models.FloatField(null=True, blank=True)
    vendor_id = models.CharField(max_length=255, null=True, blank=True)
    vendor_name = models.CharField(max_length=50, choices=VENDOR_CHOICES)
    sensor = models.TextField(null=True, blank=True)
    area = models.FloatField(null=True, blank=True)
    type = models.CharField(max_length=8, choices=TYPE_CHOICES, null=True, blank=True)
    sun_elevation = models.FloatField(null=True, blank=True)
    resolution = models.CharField(max_length=50, null=True, blank=True)
    georeferenced = models.BooleanField(null=True, blank=True, default=False)
    location_polygon = models.PolygonField(null=True, blank=True)
    coordinates_record = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    image_uploaded = models.BooleanField(default=False, null=True, blank=True) 
    gsd = models.FloatField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=['acquisition_datetime']),
            models.Index(fields=['vendor_name']),
            models.Index(fields=["vendor_id"]),
        ]

    def __str__(self):
        return f"Acquisition {self.vendor_name} - {self.acquisition_datetime}"

    @property
    def type_of_day(self):
        """Determine Day or Night based on datetime"""
        if self.acquisition_datetime:
            return 'Day' if 6 <= self.acquisition_datetime.hour <= 18 else 'Night'
        return None


class SatelliteDateRetrievalPipelineHistory(plane_models.Model):
    start_datetime = models.DateTimeField()
    end_datetime = models.DateTimeField()
    vendor_name = models.CharField(max_length=50, choices=VENDOR_CHOICES)
    message = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['start_datetime']),
            models.Index(fields=['end_datetime']),
        ]

    def __str__(self):
        return f"{self.id} - {self.end_datetime}"
    
    @property
    def duration(self):
        """Calculate duration of pipeline"""
        if self.start_datetime and self.end_datetime:
            return self.end_datetime - self.start_datetime
        return None

class SatelliteCaptureCatalogMetadata(plane_models.Model):
    vendor_name = plane_models.CharField(max_length=50, choices=VENDOR_CHOICES)
    vendor_id = plane_models.CharField(max_length=255, null=True, blank=True)
    acquisition_datetime = plane_models.DateTimeField(null=True, blank=True)
    metadata = plane_models.JSONField(null=True, blank=True)
    created_at = plane_models.DateTimeField(auto_now_add=True)
    updated_at = plane_models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['vendor_name']),
            models.Index(fields=['vendor_id']),
            models.Index(fields=['acquisition_datetime']),
        ]

    def __str__(self):
        return f"Metadata for {self.vendor_name} - {self.acquisition_datetime}"
