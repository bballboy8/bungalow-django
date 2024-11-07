from django.shortcuts import render

# Create your views here.


from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from core.utils import save_image_in_s3_and_get_url

class UploadImageView(APIView):
    def post(self, request):
        image = request.FILES.get("image")
        image_id = request.data.get("id")
        extension = request.data.get("extension")

        if not image or not image_id:
            return Response({"error": "Image and ID are required."}, status=status.HTTP_400_BAD_REQUEST)        

        url = save_image_in_s3_and_get_url(image, image_id, extension)
        if url == "AWS credentials not available.":
            return Response({"error": "AWS credentials not available."}, status=status.HTTP_403_FORBIDDEN)

        return Response({"url": url}, status=status.HTTP_201_CREATED)