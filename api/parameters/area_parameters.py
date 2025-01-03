from drf_spectacular.utils import OpenApiParameter

satellite_capture_catalog_params = [
    OpenApiParameter(
        name="source",
        type=str,
        default="home",
        location=OpenApiParameter.QUERY,
        description="Source of the request. Default is home.",
    ),
    OpenApiParameter(
        name="page_number",
        type=int,
        default=1,
        location=OpenApiParameter.QUERY,
        description="Page number for paginated response",
    ),
    OpenApiParameter(
        name="page_size",
        type=int,
        default=10,
        location=OpenApiParameter.QUERY,
        description="Number of records per page",
    ),
    OpenApiParameter(
        name="vendor_id",
        type=str,
        location=OpenApiParameter.QUERY,
        description="Filter records by vendor id",
    ),
    OpenApiParameter(
        name="start_date",
        type=str,
        location=OpenApiParameter.QUERY,
        description="Filter records with acquisition date greater than or equal to this date. Format: 2024-11-12T06:16:18.126580Z",
    ),
    OpenApiParameter(
        name="end_date",
        type=str,
        location=OpenApiParameter.QUERY,
        description="Filter records with acquisition date less than or equal to this date. Format: 2024-11-12T06:16:18.126580Z",
    ),
    # add latitude, longitude, and distance parameters
    OpenApiParameter(
        name="latitude",
        type=float,
        location=OpenApiParameter.QUERY,
        description="Latitude of the location",
    ),
    OpenApiParameter(
        name="longitude",
        type=float,
        location=OpenApiParameter.QUERY,
        description="Longitude of the location",
    ),
    OpenApiParameter(
        name="distance",
        type=float,
        location=OpenApiParameter.QUERY,
        description="Distance in kilometers from the location",
    ),
    OpenApiParameter(
        name="sort_by",
        type=str,
        location=OpenApiParameter.QUERY,
        default="acquisition_datetime",
        description="Sort records by field. Default is acquisition_datetime, vendor_name, sensor",
    ),
    OpenApiParameter(
        name="sort_order",
        type=str,
        location=OpenApiParameter.QUERY,
        default="desc",
        description="Sort order. Default is desc",
    ),
    OpenApiParameter(
        name="zoomed_wkt",
        type=str,
        location=OpenApiParameter.QUERY,
        description="Zoomed WKT polygon",
    ),
]
