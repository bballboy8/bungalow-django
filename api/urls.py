from django.urls import path
from .views import *

urlpatterns = [
    path('geojson-to-wkt', GeoJSONToWKTView.as_view(), name='geojson-to-wkt'),
    path('satellite-catalog', SatelliteCaptureCatalogView.as_view(), name='satellite-capture-list'),
    path('get-satellite-captured-images', GetSatelliteCapturedImageByIdAndVendorView.as_view(), name='get-satellite-captured-images'),
    path("airbus/add-images", AirbusVendorView.as_view(), name="airbus-vendor"),
    path("maxar/add-images", MaxarVendorView.as_view(), name="maxar-vendor"),
    path("planet/add-images", PlanetVendorView.as_view(), name="planet-vendor"),
    path("blacksky/add-images", BlackskyVendorView.as_view(), name="blacksky-vendor"),
    path("capella/add-images", CapellaVendorView.as_view(), name="capella-vendor"),
    path("get-pin-selection-analytics", GetPinSelectionAnalyticsAndLocation.as_view(), name="get-selection-pin-analytics"),
    path("get-polygon-selection-acquisition-calender-days-frequency", GetPolygonSelectionAcquisitionCalenderDaysFrequencyView.as_view(), name="get-polygon-selection-acquisition-calender-days-frequency"),
    path("get-pin-selection-acquisition-calender-days-frequency", GetPinSelectionAcquisitionCalenderDaysFrequencyView.as_view(), name="get-pin-selection-acquisition-calender-days-frequency"),
    path('get-groups/', GroupView.as_view(), name='group_list'),
    path('get-nested-groups/<int:group_id>/', GroupView.as_view(), name='group_hierarchy'),
    path('get-sites/', SiteView.as_view(), name='site_list'),
    path('get-group-sites/', GroupSiteView.as_view(), name='assign_site_to_group'),
    path('get-nested-group-sites/<int:group_id>/', GroupSiteView.as_view(), name='get_sites_in_group'),


]
