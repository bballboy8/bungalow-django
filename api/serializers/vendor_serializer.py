from rest_framework import serializers

# request serializer : ids = ["id1", "id2", "id3"]

class AirbusVendorImagesSerializer(serializers.Serializer):
    ids = serializers.ListField(child=serializers.CharField())

class MaxarVendorImagesSerializer(serializers.Serializer):
    ids = serializers.ListField(child=serializers.CharField())

class BlackskyVendorImagesSerializer(serializers.Serializer):
    ids = serializers.ListField(child=serializers.CharField())

class PlanetVendorImagesSerializer(serializers.Serializer):
    ids = serializers.ListField(child=serializers.CharField())

class CapellaVendorImagesSerializer(serializers.Serializer):
    ids = serializers.ListField(child=serializers.CharField())

class SkyfiVendorImagesSerializer(serializers.Serializer):
    ids = serializers.ListField(child=serializers.CharField())