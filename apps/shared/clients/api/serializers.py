from rest_framework import serializers


class TenantDomainResponseSerializer(serializers.Serializer):
    domain = serializers.CharField()
    name = serializers.CharField()
