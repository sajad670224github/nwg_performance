from django.urls import path

from sla.views import SlaParameterViews, SlaKpiViews, SlaElementViews, SlaKpiDataViews, dashboard_view

urlpatterns = [
    path('sla_params/<type>/', SlaParameterViews.as_view()),
    path('sla_kpis/', SlaKpiViews.as_view()),
    path('sla_elements/', SlaElementViews.as_view()),
    path('sla_kpi_data/', SlaKpiDataViews.as_view()),
    path('sla_tempalte/', dashboard_view, name='dashboard')
    ]

variables = dict(
technology = ['LMBB'],
type1 = ['score'],
layers = ['region'],
network = ['Irancell']
)
layer_list = ["network", "layers", "technology", "type1"]
payload = {'startedate': '2025-01-21', 'enddate': '2025-05-05'}
type_ = "layers"
for item in layer_list:
    if item == type_:
        break
    payload[item] = variables[item]

print(payload)