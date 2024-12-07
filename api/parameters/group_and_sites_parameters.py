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
