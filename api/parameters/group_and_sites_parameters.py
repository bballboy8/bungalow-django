from drf_spectacular.utils import extend_schema, OpenApiResponse, OpenApiParameter


group_search_for_assignment_parameters = [
    OpenApiParameter(
        name="group_id",
        type=int,
        default=None,
        location=OpenApiParameter.QUERY,
        description="ID of the group to retrieve the full hierarchy or empty to retrieve all groups.",
    ),
    OpenApiParameter(
        name="group_name",
        type=str,
        default=None,
        location=OpenApiParameter.QUERY,
        description="Name of the group to retrieve the full hierarchy or empty to retrieve all groups.",
    ),
]

site_search_parameters = [
    OpenApiParameter(
        name="name",
        type=str,
        default=None,
        location=OpenApiParameter.QUERY,
        description="Name of the site to retrieve or empty to retrieve all sites.",
    ),
    # page_number, per_page
    OpenApiParameter(
        name="page_number",
        type=int,
        default=1,
        location=OpenApiParameter.QUERY,
        description="Page number to retrieve sites.",
    ),
    OpenApiParameter(
        name="per_page",
        type=int,
        default=10,
        location=OpenApiParameter.QUERY,
        description="Number of sites per page.",
    ),
]