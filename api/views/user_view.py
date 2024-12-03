from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiResponse
from rest_framework.permissions import AllowAny, IsAuthenticated

@extend_schema(
    description="Search for items with pagination and filtering by search term.",
    parameters=[
        OpenApiParameter('page', type=int, description="Page number", required=False, default=1),
        OpenApiParameter('per_page', type=int, description="Number of items per page", required=False, default=10),
        OpenApiParameter('search', type=str, description="Search term for filtering results", required=False),
    ],
    responses={
        200: OpenApiResponse(description="A list of search results with pagination"),
    },
    tags=["Search"]
)
class SearchView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        page = int(request.query_params.get('page', 1))
        per_page = int(request.query_params.get('per_page', 10))
        search = request.query_params.get('search', '')

        # Mock response
        results = {
            'page': page,
            'per_page': per_page,
            'search': search,
            'items': [{"id": i, "name": f"Item {i}"} for i in range((page-1)*per_page, page*per_page)],
        }

        return Response(results, status=status.HTTP_200_OK)
