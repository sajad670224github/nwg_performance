
from django.contrib import admin
from django.urls import path, include, include
from django.views.generic.base import TemplateView
from .views import CustomLoginView, HomeView, SlaKpiView
from django.urls import path, include, re_path
from rest_framework import permissions
from drf_yasg.views import get_schema_view
from drf_yasg import openapi
schema_view = get_schema_view(
    openapi.Info(
        title="NWG Performance",
        default_version='v1',
        description="",
        terms_of_service="https://www.google.com/policies/terms/",
        contact=openapi.Contact(email=""),
        license=openapi.License(name="BSD License"),
    ),
    public=True,
    permission_classes=(permissions.AllowAny,),
)

from sla.views import SlaParameterViews

urlpatterns = [
    path('admin/', admin.site.urls),
    path('home/', HomeView.as_view()),
    path('sla_analytics/', SlaKpiView.as_view()),
    path('accounts/login/', CustomLoginView.as_view(), name='login'),
    path('accounts/', include('django.contrib.auth.urls')),  # Auth URLs
#    path('', TemplateView.as_view(template_name='home.html'), name='home'),
    ### swagger
    path('swagger<format>/', schema_view.without_ui(cache_timeout=0), name='schema-json'),
    path('swagger/', schema_view.with_ui('swagger', cache_timeout=0), name='schema-swagger-ui'),
    path('redoc/', schema_view.with_ui('redoc', cache_timeout=0), name='schema-redoc'),
    path('sla/', include('sla.urls')),
]
