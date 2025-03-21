from drf_spectacular.utils import OpenApiParameter
from datetime import datetime

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
        default="2024-11-12T06:16:18.126580Z",
        location=OpenApiParameter.QUERY,
        description="Filter records with acquisition date greater than or equal to this date. Format: 2024-11-12T06:16:18.126580Z",
    ),
    OpenApiParameter(
        name="end_date",
        type=str,
        default="2024-11-12T06:16:18.126580Z",
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
        description="Zoomed WKT polygon :POLYGON ((90 10, 135 10, 135 50, 90 50, 90 10))",
    ),
    OpenApiParameter(
        name="vendor_name",
        type=str,
        location=OpenApiParameter.QUERY,
        description="Filter records by vendor name maxar, airbus, planet, blacksky, capella, skyfi-umbra or maxar,airbus,planet,blacksky,capella,skyfi-umbra",
    ),
    OpenApiParameter(
        name="min_cloud_cover",
        type=float,
        location=OpenApiParameter.QUERY,
        description="Filter records by minimum cloud cover",
    ),
    OpenApiParameter(
        name="max_cloud_cover",
        type=float,
        location
        =OpenApiParameter.QUERY,
        description="Filter records by maximum cloud cover",
    ),
    OpenApiParameter(
        name="min_off_nadir_angle",
        type=float,
        location
        =OpenApiParameter.QUERY,
        description="Filter records by minimum off nadir angle",
    ),
    OpenApiParameter(
        name="max_off_nadir_angle",
        type=float,
        location
        =OpenApiParameter.QUERY,
        description="Filter records by maximum off nadir angle",
    ),
    OpenApiParameter(
        name="min_gsd",
        type=float,
        location=OpenApiParameter.QUERY,
        description="Filter records by minimum ground sample distance",
    ),
    OpenApiParameter(
        name="max_gsd",
        type=float,
        location=OpenApiParameter.QUERY,
        description="Filter records by maximum ground sample distance",
    ),
    OpenApiParameter(
        name="focused_records_ids",
        type=str,
        location=OpenApiParameter.QUERY,
        description="Ids of the overlapped records"
    ),
    OpenApiParameter(
        name="user_timezone",
        type=str,
        location=OpenApiParameter.QUERY,
        description="User timezone Example: Asia/Kolkata, America/New_York"
    ),
    OpenApiParameter(
        name="user_duration_type",
        type=str,
        location=OpenApiParameter.QUERY,
        description="User duration type Example: morning, midday, evening, overnight"
    ),
    OpenApiParameter(
        name="min_azimuth_angle",
        type=float,
        location=OpenApiParameter.QUERY,
        description="Filter records by minimum azimuth angle",
    ),
    OpenApiParameter(
        name="max_azimuth_angle",
        type=float,
        location=OpenApiParameter.QUERY,
        description="Filter records by maximum azimuth angle",
    ),
    OpenApiParameter(
        name="min_illumination_azimuth_angle",
        type=float,
        location=OpenApiParameter.QUERY,
        description="Filter records by minimum illumination azimuth angle",
    ),
    OpenApiParameter(
        name="max_illumination_azimuth_angle",
        type=float,
        location
        =OpenApiParameter.QUERY,
        description="Filter records by maximum illumination azimuth angle",
    ),
    OpenApiParameter(
        name="min_illumination_elevation_angle",
        type=float,
        location=OpenApiParameter.QUERY,
        description="Filter records by minimum illumination elevation angle",
    ),
    OpenApiParameter(
        name="max_illumination_elevation_angle",
        type=float,
        location
        =OpenApiParameter.QUERY,
        description="Filter records by maximum illumination elevation angle",
    ),
    OpenApiParameter(
        name="min_holdback_seconds",
        type=int,
        location=OpenApiParameter.QUERY,
        description="Filter records by minimum holdback seconds",
    ),
    OpenApiParameter(
        name="max_holdback_seconds",
        type=int,
        location=OpenApiParameter.QUERY,
        description="Filter records by maximum holdback seconds",
    ),
    OpenApiParameter(
        name="is_purchased",
        type=bool,
        location=OpenApiParameter.QUERY,
        description="Filter records by is purchased"
    )
]



calendar_params = [
    OpenApiParameter(
        name="vendor_id",
        type=str,
        location=OpenApiParameter.QUERY,
        description="Filter records by vendor id",
    ),
    OpenApiParameter(
        name="vendor_name",
        type=str,
        location=OpenApiParameter.QUERY,
        description="Filter records by vendor name maxar, airbus, planet, blacksky, capella, skyfi-umbra or maxar,airbus,planet,blacksky,capella,skyfi-umbra",
    ),
    OpenApiParameter(
        name="min_cloud_cover",
        type=float,
        location=OpenApiParameter.QUERY,
        description="Filter records by minimum cloud cover",
    ),
    OpenApiParameter(
        name="max_cloud_cover",
        type=float,
        location
        =OpenApiParameter.QUERY,
        description="Filter records by maximum cloud cover",
    ),
    OpenApiParameter(
        name="min_off_nadir_angle",
        type=float,
        location
        =OpenApiParameter.QUERY,
        description="Filter records by minimum off nadir angle",
    ),
    OpenApiParameter(
        name="max_off_nadir_angle",
        type=float,
        location
        =OpenApiParameter.QUERY,
        description="Filter records by maximum off nadir angle",
    ),
    OpenApiParameter(
        name="min_gsd",
        type=float,
        location=OpenApiParameter.QUERY,
        description="Filter records by minimum ground sample distance",
    ),
    OpenApiParameter(
        name="max_gsd",
        type=float,
        location=OpenApiParameter.QUERY,
        description="Filter records by maximum ground sample distance",
    ),
    OpenApiParameter(
        name="user_timezone",
        type=str,
        location=OpenApiParameter.QUERY,
        description="User timezone Example: Asia/Kolkata, America/New_York"
    ),
    OpenApiParameter(
        name="user_duration_type",
        type=str,
        location=OpenApiParameter.QUERY,
        description="User duration type Example: morning, midday, evening, overnight"
    ),
    OpenApiParameter(
        name="min_azimuth_angle",
        type=float,
        location=OpenApiParameter.QUERY,
        description="Filter records by minimum azimuth angle",
    ),
    OpenApiParameter(
        name="max_azimuth_angle",
        type=float,
        location=OpenApiParameter.QUERY,
        description="Filter records by maximum azimuth angle",
    ),
    OpenApiParameter(
        name="min_illumination_azimuth_angle",
        type=float,
        location=OpenApiParameter.QUERY,
        description="Filter records by minimum illumination azimuth angle",
    ),
    OpenApiParameter(
        name="max_illumination_azimuth_angle",
        type=float,
        location
        =OpenApiParameter.QUERY,
        description="Filter records by maximum illumination azimuth angle",
    ),
    OpenApiParameter(
        name="min_illumination_elevation_angle",
        type=float,
        location=OpenApiParameter.QUERY,
        description="Filter records by minimum illumination elevation angle",
    ),
    OpenApiParameter(
        name="max_illumination_elevation_angle",
        type=float,
        location
        =OpenApiParameter.QUERY,
        description="Filter records by maximum illumination elevation angle",
    ),
    OpenApiParameter(
        name="min_holdback_seconds",
        type=int,
        location=OpenApiParameter.QUERY,
        description="Filter records by minimum holdback seconds",
    ),
    OpenApiParameter(
        name="max_holdback_seconds",
        type=int,
        location=OpenApiParameter.QUERY,
        description="Filter records by maximum holdback seconds",
    ),
    OpenApiParameter(
        name="is_purchased",
        type=bool,
        location=OpenApiParameter.QUERY,
        description="Filter records by is purchased"
    )
]

collection_history_params = [
    OpenApiParameter(
        name="vendor_name",
        type=str,
        location=OpenApiParameter.QUERY,
        description="Filter records by vendor name maxar, airbus, planet, blacksky, capella, skyfi-umbra or maxar,airbus,planet,blacksky,capella,skyfi-umbra",
    ),
    OpenApiParameter(
        name="start_date",
        type=str,
        default=(datetime.now().isoformat()),
        location=OpenApiParameter.QUERY,
        description="Filter records with acquisition date greater than or equal to this date. Format: 2024-11-12T06:16:18.126580Z",
    ),
    OpenApiParameter(
        name="end_date",
        type=str,
        default=(datetime.now().isoformat()),
        location
        =OpenApiParameter.QUERY,
        description="Filter records with acquisition date less than or equal to this date. Format: 2024-11-12T06:16:18.126580Z",
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
]