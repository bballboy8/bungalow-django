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
    path(
        "get-collection-history",
        GetCollectionHistoryView.as_view(),
        name="get-collection-history",
    ),
    path("airbus/add-images", AirbusVendorView.as_view(), name="airbus-vendor"),
    path("maxar/add-images", MaxarVendorView.as_view(), name="maxar-vendor"),
    path("planet/add-images", PlanetVendorView.as_view(), name="planet-vendor"),
    path("blacksky/add-images", BlackskyVendorView.as_view(), name="blacksky-vendor"),
    path("capella/add-images", CapellaVendorView.as_view(), name="capella-vendor"),
    path("skyfi/add-images", SkyfiVendorView.as_view(), name="skyfire-vendor"),
    path(
        "get-pin-selection-analytics",
        GetPinSelectionAnalyticsAndLocation.as_view(),
        name="get-selection-pin-analytics",
    ),
    path(
        "get-polygon-selection-analytics",
        GetPolygonSelectionAnalyticsAndLocation.as_view(),
        name="get-selection-polygon-analytics",
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
    path("update-group", UpdateGroupView.as_view(), name="update-group"),
    path("get-groups-for-assignment-and-searching", GetGroupsForAssignmentAndSearchingView.as_view(), name="groups-for-assignment-and-searching"),
    path("add-site", AddSiteView.as_view(), name="add-site"),
    path("update-site", UpdateSiteView.as_view(), name="update-site"),
    path("get-sites", GetSiteView.as_view(), name="site-list"),
    path("add-group-site", AddGroupSiteView.as_view(), name="add-group-site"),
    path("remove-group-site", RemoveGroupSiteView.as_view(), name="remove-group-site"),
    path("get-parent-groups-with-details", GetParentGroupsListwithDetailsView.as_view(), name="get-parent-groups-with-details"),
    path("get-area-from-geojson", GetAreaFromGeoJsonView.as_view(), name="get-area-from-geojson"),
    path("proxy-image/", ProxyImageAPIView.as_view(), name="proxy_image"),
    path("get-area-from-polygon-wkt", GetAreaFromPolygonWkt.as_view(), name="get-area-from-polygon-wkt"),
    path('generate-circle-polygon/', GenerateCirclePolygonAPIView.as_view(), name='generate-circle-polygon'),
    path('extract-circle-parameters/', ExtractCircleParametersAPIView.as_view(), name='extract-circle-parameters'),
    path("get-nested-group-and-sites-by-group-id", GetGroupSiteByGroupIdView.as_view(), name="get-group-sites-by-group-id"),

]
