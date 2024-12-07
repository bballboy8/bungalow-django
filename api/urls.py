from django.urls import path
from .views import *

urlpatterns = [
    path("geojson-to-wkt", GeoJSONToWKTView.as_view(), name="geojson-to-wkt"),
    path(
        "satellite-catalog",
        SatelliteCaptureCatalogView.as_view(),
        name="satellite-capture-list",
    ),
    path(
        "get-satellite-captured-images",
        GetSatelliteCapturedImageByIdAndVendorView.as_view(),
        name="get-satellite-captured-images",
    ),
    path("airbus/add-images", AirbusVendorView.as_view(), name="airbus-vendor"),
    path("maxar/add-images", MaxarVendorView.as_view(), name="maxar-vendor"),
    path("planet/add-images", PlanetVendorView.as_view(), name="planet-vendor"),
    path("blacksky/add-images", BlackskyVendorView.as_view(), name="blacksky-vendor"),
    path("capella/add-images", CapellaVendorView.as_view(), name="capella-vendor"),
    path(
        "get-pin-selection-analytics",
        GetPinSelectionAnalyticsAndLocation.as_view(),
        name="get-selection-pin-analytics",
    ),
    path(
        "get-polygon-selection-acquisition-calender-days-frequency",
        GetPolygonSelectionAcquisitionCalenderDaysFrequencyView.as_view(),
        name="get-polygon-selection-acquisition-calender-days-frequency",
    ),
    path(
        "get-pin-selection-acquisition-calender-days-frequency",
        GetPinSelectionAcquisitionCalenderDaysFrequencyView.as_view(),
        name="get-pin-selection-acquisition-calender-days-frequency",
    ),
    path("add-group", AddGroupView.as_view(), name="add_group"),
    path("get-groups", GetGroupsView.as_view(), name="group_list"),
    path("add-site", AddSiteView.as_view(), name="add_site"),
    path("get-sites", GetSiteView.as_view(), name="site_list"),
    path("add-group-site", AddGroupSiteView.as_view(), name="add_group_site"),
]
