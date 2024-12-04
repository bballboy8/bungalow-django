from django.urls import path
from .views import *

urlpatterns = [
    path('search/', SearchView.as_view(), name='search'),
    path('geojson-to-wkt/', GeoJSONToWKTView.as_view(), name='geojson-to-wkt'),
    path('satellite-catalog/', SatelliteCaptureCatalogView.as_view(), name='satellite-capture-list'),

]
