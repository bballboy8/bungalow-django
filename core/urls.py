# urls.py

from django.urls import path
from core.views import UploadImageView

urlpatterns = [
    path("upload-image/", UploadImageView.as_view(), name="upload-image"),
]
