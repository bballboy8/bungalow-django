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
    path("add-group", AddGroupView.as_view(), name="add-group"),
    path("get-groups-for-assignment-and-searching", GetGroupsForAssignmentAndSearchingView.as_view(), name="groups-for-assignment-and-searching"),
    path("add-site", AddSiteView.as_view(), name="add-site"),
    path("get-sites", GetSiteView.as_view(), name="site-list"),
    path("add-group-site", AddGroupSiteView.as_view(), name="add-group-site"),
    path("get-parent-groups-with-details", GetParentGroupsListwithDetailsView.as_view(), name="get-parent-groups-with-details"),
    path("get-area-from-geojson", GetAreaFromGeoJsonView.as_view(), name="get-area-from-geojson"),
    path("proxy-image/", ProxyImageAPIView.as_view(), name="proxy_image"),
    path("get-area-from-polygon-wkt", GetAreaFromPolygonWkt.as_view(), name="get-area-from-polygon-wkt"),

]
