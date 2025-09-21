from django.shortcuts import render
import uuid
import os
from drf_yasg.utils import swagger_auto_schema
from django.http import HttpResponse, JsonResponse
from rest_framework import (
    status,
    views,
    permissions,
    response,
    serializers,
    exceptions,
    generics
)

from django.views import View
from sla.utils import get_clickhouse_column_information, get_clickhouse_kpi_information, get_user_elements, get_sla_kpi, serializing_chart_output
from sla.serializers import SlaParameterSerializer, SlaKpiSerializer, SlaElementSerializer, SlaKpiDataSerializer




def dashboard_view(request):
    return render(request, 'sla_template.html')


class SlaParameterViews(views.APIView):
    permission_classes = (permissions.IsAuthenticated,)

    @swagger_auto_schema(
        request_body=SlaParameterSerializer,
    )
    def post(self, request, *args, **kwargs):
        print(request.data)
        serializer = SlaParameterSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.data
        type_ = kwargs.get("type")
        print(data)
        list_data = get_clickhouse_column_information(type_, data)
        data_out = {}
        for i in zip(list(range(len(list_data))), list_data):
            data_out[i[0]] = i[1]
        return JsonResponse(data_out)

class SlaKpiViews(views.APIView):
    permission_classes = (permissions.IsAuthenticated,)

    @swagger_auto_schema(
        request_body=SlaKpiSerializer,
    )
    def post(self, request, *args, **kwargs):
        serializer = SlaKpiSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.data
        time_ = [data['start_date'], data['end_date']]
        type_ = data['type']
        technology = data['technology']
        user = request.user
        return response.Response(get_clickhouse_kpi_information(type_, time_, technology), status.HTTP_200_OK)



class SlaElementViews(views.APIView):
    permission_classes = (permissions.IsAuthenticated,)

    @swagger_auto_schema(
        request_body=SlaElementSerializer,
    )
    def post(self, request, *args, **kwargs):
        serializer = SlaElementSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.data
        time_ = [data['start_date'], data['end_date']]
        type_ = data['type']
        technology = data['technology']
        layer = data['layer']
        return response.Response(get_clickhouse_column_information('element', time_, {'technology': technology, 'type': type_, 'layer':layer}), status.HTTP_200_OK)


class SlaKpiDataViews(views.APIView):
    permission_classes = (permissions.IsAuthenticated,)

    @swagger_auto_schema(
        request_body=SlaKpiDataSerializer,
    )
    def post(self, request, *args, **kwargs):
        serializer = SlaKpiDataSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.data
        date_ = [data['start_date'], data['end_date']]
        network = data['network']
        type_ = data['type']
        technology = data['technology']
        kpis = data['kpis']
        layer = data['layer']
        output = data['output']
        elements = data['elements']
        # To Do-- filter user elements by user group
        df = get_sla_kpi(date_, technology, network, layer, kpis, elements, type_)
        if output == 'csv':
            report_name = f"sla_output_{date_[0]}_{str(uuid.uuid4()).replace('-', '_')}.csv"
            df.to_csv(report_name, index=False)
            fname = os.path.basename(report_name)
            with open(report_name, 'r') as fd:
                response = HttpResponse(fd.read(), content_type='text/csv')
                response['Content-Disposition'] = f'attachment; filename="{fname}"'
                return response
        return JsonResponse(serializing_chart_output(df), safe=False)
