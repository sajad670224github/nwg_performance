from rest_framework import serializers

class SlaParameterSerializer(serializers.Serializer):
    start_date = serializers.CharField()
    end_date = serializers.CharField()
    network = serializers.CharField(required=False, allow_null=True)
    layer = serializers.CharField(required=False, allow_null=True)
    technology = serializers.CharField(required=False, allow_null=True)
    type = serializers.CharField(required=False, allow_null=True)
class SlaKpiSerializer(serializers.Serializer):
    start_date = serializers.CharField()
    end_date = serializers.CharField()
    type = serializers.CharField()
    technology = serializers.CharField()

class SlaElementSerializer(SlaKpiSerializer):
    layer = serializers.CharField()

class SlaKpiDataSerializer(SlaElementSerializer):
    elements = serializers.CharField()
    output = serializers.CharField()
    network = serializers.CharField()
    kpis = serializers.CharField()