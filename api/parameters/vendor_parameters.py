from drf_spectacular.utils import OpenApiParameter

get_proxy_images_parameters = [
    OpenApiParameter(
        name="vendor_id",
        type=str,
        location
        =OpenApiParameter.QUERY,
        description="Vendor ID",
    ),
    OpenApiParameter(
        name="vendor_name",
        type=str,
        location=OpenApiParameter.QUERY,
        description="Vendor Name",
    ),

]