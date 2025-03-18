from rest_framework.views import APIView
from rest_framework.response import Response
from drf_spectacular.utils import extend_schema, OpenApiResponse
from api.services.vendor_service import *
from api.serializers.vendor_serializer import *
from rest_framework.permissions import IsAuthenticated
from logging_module import logger
import time
from django.http import StreamingHttpResponse, HttpResponse
from decouple import config
from api.parameters.vendor_parameters import *


class AirbusVendorView(APIView):

    @extend_schema(
        description="""Save Airbus record images by ids. ["id1", "id2", "id3"] """,
        request=AirbusVendorImagesSerializer,
        responses={
            200: OpenApiResponse(
                description="Airbus record images successfully retrieved.",
            ),
            400: OpenApiResponse(description="Bad Request - Invalid ids."),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Vendors"],
    )
    def put(self, request, *args, **kwargs):
        logger.info("Inside Post method of Airbus Vendor View")

        try:
            ids = request.data.get("ids", None)
            if not ids:
                return Response(
                    {
                        "data": "ids is required",
                        "status_code": 400,
                    },
                    status=400,
                )

            service_response = get_airbus_record_images_by_ids(ids)

            if service_response["status_code"] != 200:
                return Response(
                    service_response, status=service_response["status_code"]
                )

            logger.info("Airbus Vendor View response")

            return Response({"data": service_response["data"], "status_code": 200})

        except Exception as e:
            logger.error(f"Error in Airbus Vendor View: {str(e)}")
            return Response({"data": f"{str(e)}", "status_code": 500, "error": f"{str(e)}"}, status=500)


class MaxarVendorView(APIView):

    @extend_schema(
        description="Save Maxar record images by ids.",
        request=MaxarVendorImagesSerializer,
        responses={
            200: OpenApiResponse(
                description="Maxar record images successfully retrieved.",
            ),
            400: OpenApiResponse(description="Bad Request - Invalid ids."),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Vendors"],
    )
    def put(self, request, *args, **kwargs):
        logger.info("Inside Post method of Maxar Vendor View")

        try:
            ids = request.data.get("ids", None)
            if not ids:
                return Response(
                    {
                        "data": "ids is required",
                        "status_code": 400,
                    },
                    status=400,
                )

            service_response = get_maxar_record_images_by_ids(ids)

            if service_response["status_code"] != 200:
                return Response(
                    service_response, status=service_response["status_code"]
                )

            logger.info("Maxar Vendor View response")

            return Response({"data": service_response["data"], "status_code": 200})

        except Exception as e:
            logger.error(f"Error in Maxar Vendor View: {str(e)}")
            return Response({"data": f"{str(e)}", "status_code": 500, "error": f"{str(e)}"}, status=500)


class BlackskyVendorView(APIView):

    @extend_schema(
        description="Save Blacksky record images by ids.",
        request=BlackskyVendorImagesSerializer,
        responses={
            200: OpenApiResponse(
                description="Blacksky record images successfully retrieved.",
            ),
            400: OpenApiResponse(description="Bad Request - Invalid ids."),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Vendors"],
    )
    def put(self, request, *args, **kwargs):
        logger.info("Inside Post method of Blacksky Vendor View")

        try:
            ids = request.data.get("ids", None)
            if not ids:
                return Response(
                    {
                        "data": "ids is required",
                        "status_code": 400,
                    },
                    status=400,
                )

            service_response = get_blacksky_record_images_by_ids(ids)

            if service_response["status_code"] != 200:
                return Response(
                    service_response, status=service_response["status_code"]
                )

            logger.info("Blacksky Vendor View response")

            return Response({"data": service_response["data"], "status_code": 200})

        except Exception as e:
            logger.error(f"Error in Blacksky Vendor View: {str(e)}")
            return Response({"data": f"{str(e)}", "status_code": 500, "error": f"{str(e)}"}, status=500)


class PlanetVendorView(APIView):

    @extend_schema(
        description="Save Planet record images by ids.",
        request=PlanetVendorImagesSerializer,
        responses={
            200: OpenApiResponse(
                description="Planet record images successfully retrieved.",
            ),
            400: OpenApiResponse(description="Bad Request - Invalid ids."),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Vendors"],
    )
    def put(self, request, *args, **kwargs):
        logger.info("Inside Post method of Planet Vendor View")

        try:
            ids = request.data.get("ids", None)
            if not ids:
                return Response(
                    {
                        "data": "ids is required",
                        "status_code": 400,
                    },
                    status=400,
                )

            service_response = get_planet_record_images_by_ids(ids)

            if service_response["status_code"] != 200:
                return Response(
                    service_response, status=service_response["status_code"]
                )

            logger.info("Planet Vendor View response")

            return Response({"data": service_response["data"], "status_code": 200})

        except Exception as e:
            logger.error(f"Error in Planet Vendor View: {str(e)}")
            return Response({"data": f"{str(e)}", "status_code": 500, "error": f"{str(e)}"}, status=500)


class CapellaVendorView(APIView):

    @extend_schema(
        description="Save Capella record images by ids.",
        request=CapellaVendorImagesSerializer,
        responses={
            200: OpenApiResponse(
                description="Capella record images successfully retrieved.",
            ),
            400: OpenApiResponse(description="Bad Request - Invalid ids."),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Vendors"],
    )
    def put(self, request, *args, **kwargs):
        logger.info("Inside Post method of Capella Vendor View")

        try:
            ids = request.data.get("ids", None)
            if not ids:
                return Response(
                    {
                        "data": "ids is required",
                        "status_code": 400,
                    },
                    status=400,
                )

            service_response = get_capella_record_images_by_ids(ids)

            if service_response["status_code"] != 200:
                return Response(
                    service_response, status=service_response["status_code"]
                )

            logger.info("Capella Vendor View response")

            return Response({"data": service_response["data"], "status_code": 200})

        except Exception as e:
            logger.error(f"Error in Capella Vendor View: {str(e)}")
            return Response({"data": f"{str(e)}", "status_code": 500, "error": f"{str(e)}"}, status=500)


class ProxyImageAPIView(APIView):
    @extend_schema(
        description="Get Proxy images by vendor name and id.",
        parameters=get_proxy_images_parameters,
        responses={
            200: OpenApiResponse(
                description="Proxy image successfully retrieved.",
            ),
            400: OpenApiResponse(description="Bad Request - Invalid ids."),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Vendors"],
    )
    def get(self, request, *args, **kwargs):
        vendor_name = request.GET.get("vendor_name")
        vendor_id = request.GET.get("vendor_id")

        if not vendor_name or not vendor_id:
            return HttpResponse("Missing vendor_name or vendor_id", status=400)

        if vendor_name == "maxar":
            vendor_id = vendor_id.split("-")[0]
            image_url = f"https://api.maxar.com/browse-archive/v1/browse/show?image_id={vendor_id}"
            headers = {"Accept": "application/json", "MAXAR-API-KEY": AUTH_TOKEN}
        elif vendor_name == "planet":
            headers = {
                "Content-Type": "application/json",
                "Authorization": "api-key " + config("PLANET_API_KEY"),
            }
            image_url = f"https://tiles.planet.com/data/v1/item-types/SkySatCollect/items/{vendor_id}/thumb"
        elif vendor_name == "blacksky":
            headers = {"Authorization": BLACKSKY_AUTH_TOKEN}
            image_url = f"{BLACKSKY_BASE_URL}/v1/browse/{vendor_id}"
        elif vendor_name == "airbus":
            access_token = get_acces_token()
            headers = {"Authorization": "Bearer " + access_token}
            image_url = f"https://access.foundation.api.oneatlas.airbus.com/api/v1/items/{vendor_id}/thumbnail?width=2000"
        else:
            return HttpResponse("Unsupported vendor", status=400)

        # Fetch the image
        response = requests.get(image_url, headers=headers, stream=True)
        if response.status_code == 200:

            def generate():
                for chunk in response.iter_content(chunk_size=128):
                    yield chunk

            content_type = response.headers.get(
                "Content-Type", "application/octet-stream"
            )
            return StreamingHttpResponse(generate(), content_type=content_type)

        return HttpResponse(
            f"Failed to fetch image: {response.status_code}",
            status=response.status_code,
        )


class SkyfiVendorView(APIView):

    @extend_schema(
        description="Save Skyfi record images by ids.",
        request=SkyfiVendorImagesSerializer,
        responses={
            200: OpenApiResponse(
                description="Skyfi record images successfully retrieved.",
            ),
            400: OpenApiResponse(description="Bad Request - Invalid ids."),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Vendors"],
    )
    def put(self, request, *args, **kwargs):
        logger.info("Inside Post method of Skyfi Vendor View")

        try:
            ids = request.data.get("ids", None)
            if not ids:
                return Response(
                    {
                        "data": "ids is required",
                        "status_code": 400,
                    },
                    status=400,
                )

            service_response = get_skyfi_record_images_by_ids(ids)

            if service_response["status_code"] != 200:
                return Response(
                    service_response, status=service_response["status_code"]
                )

            logger.info("Skyfi Vendor View response")

            return Response({"data": service_response["data"], "status_code": 200})

        except Exception as e:
            logger.error(f"Error in Skyfi Vendor View: {str(e)}")
            return Response({"data": f"{str(e)}", "status_code": 500, "error": f"{str(e)}"}, status=500)