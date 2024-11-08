from django.shortcuts import render

# Create your views here.


from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from core.utils import save_image_in_s3_and_get_url
from rest_framework import viewsets
from .models import SatelliteCaptureCatalog
from .serializers import SatelliteCaptureCatalogSerializer
from django.contrib.gis.geos import Point
from django.contrib.gis.measure import D

class UploadImageView(APIView):
    def post(self, request):
        image = request.FILES.get("image")
        image_id = request.data.get("id")
        extension = request.data.get("extension")

        if not image or not image_id:
            return Response({"error": "Image and ID are required."}, status=status.HTTP_400_BAD_REQUEST)        

        url = save_image_in_s3_and_get_url(image, image_id, extension)
        if url == "AWS credentials not available.":
            return Response({"error": "AWS credentials not available."}, status=status.HTTP_403_FORBIDDEN)

        return Response({"url": url}, status=status.HTTP_201_CREATED)
    
class SatelliteCaptureCatalogViewSet(viewsets.ModelViewSet):
    queryset = SatelliteCaptureCatalog.objects.all()
    serializer_class = SatelliteCaptureCatalogSerializer

    def get_queryset(self):
        queryset = super().get_queryset()
        
        latitude = self.request.query_params.get('latitude')
        longitude = self.request.query_params.get('longitude')
        distance = self.request.query_params.get('distance')
        
        if latitude and longitude and distance:
            try:
                point = Point(float(longitude), float(latitude), srid=4326)
                distance_km = float(distance)
                
                queryset = queryset.filter(location_polygon__distance_lte=(point, D(km=distance_km)))
            except (ValueError, TypeError):
                return queryset.none()
        
        return queryset
    