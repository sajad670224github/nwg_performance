from rest_framework import serializers

class UserLoginSerializer(serializers.Serializer):
    token = serializers.CharField()
    user = serializers.CharField()
    session = serializers.CharField()
