# urls.py

from django.urls import path, include
from core.views import UploadImageView, SatelliteCaptureCatalogViewSet
from rest_framework.routers import DefaultRouter

router = DefaultRouter()
router.register(r'satellite-captures', SatelliteCaptureCatalogViewSet)

urlpatterns = [
    path("upload-image/", UploadImageView.as_view(), name="upload-image"),
    path('catalogs/', include(router.urls)),

]
