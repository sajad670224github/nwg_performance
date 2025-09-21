from django.urls import path

from sla.views import SlaParameterViews, SlaKpiViews, SlaElementViews, SlaKpiDataViews, dashboard_view

urlpatterns = [
    path('sla_params/<type>/', SlaParameterViews.as_view()),
    path('sla_kpis/', SlaKpiViews.as_view()),
    path('sla_elements/', SlaElementViews.as_view()),
    path('sla_kpi_data/', SlaKpiDataViews.as_view()),
    path('sla_tempalte/', dashboard_view, name='dashboard')
    ]
