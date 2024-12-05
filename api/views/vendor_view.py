from rest_framework.views import APIView
from rest_framework.response import Response
from drf_spectacular.utils import extend_schema, OpenApiResponse
from api.services.vendor_service import *
from api.serializers.vendor_serializer import *
from rest_framework.permissions import IsAuthenticated
from logging_module import logger
import time


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
            return Response({"data": f"{str(e)}", "status_code": 500}, status=500)


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
            print(service_response)

            if service_response["status_code"] != 200:
                return Response(
                    service_response, status=service_response["status_code"]
                )

            logger.info("Maxar Vendor View response")

            return Response({"data": service_response["data"], "status_code": 200})

        except Exception as e:
            logger.error(f"Error in Maxar Vendor View: {str(e)}")
            return Response({"data": f"{str(e)}", "status_code": 500}, status=500)


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
            return Response({"data": f"{str(e)}", "status_code": 500}, status=500)


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
            return Response({"data": f"{str(e)}", "status_code": 500}, status=500)


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
            return Response({"data": f"{str(e)}", "status_code": 500}, status=500)
