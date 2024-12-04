from django.urls import path
from .views import *

urlpatterns = [
    path('search/', SearchView.as_view(), name='search'),
    path('satellite-catalog/', SatelliteCaptureCatalogFilterView.as_view(), name='satellite-capture-filter-list'),

]
