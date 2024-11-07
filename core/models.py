
# Create your models here.

from django.contrib.gis.db import models

class SatelliteCaptureCatalog(models.Model):
    # Constants for choices
    VENDOR_CHOICES = [
        ('airbus', 'airbus'),
        ('blacksky', 'blacksky'),
        ('planet', 'planet'),
        ('maxar', 'maxar'),
        ('capella', 'capella'),
        ('skyfi', 'skyfi'),
    ]

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
    georeferenced = models.BooleanField(null=True, blank=True)
    location_polygon = models.PolygonField(null=True, blank=True) 
    
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
