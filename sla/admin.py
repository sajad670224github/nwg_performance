from django.contrib import admin

from sla.models import *

class SlaKpiModelsAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "kpi_id",
        "name",
        "technology",
        "bh_kpi_id",
        "bh_kpi_name",
        "special_kpi"
    )

@admin.register(SlaPmModels)
class SlaPmModelsAdmin(admin.ModelAdmin):
    show_full_result_count = True
    list_display = (
       "id",
       "technology",
       "layer",
       "granularity",
       "network",
        "formatted_kpis"
    )
    filter_horizontal = ('kpis',)  # Adds a nice widget for many-to-many selection

    def formatted_kpis(self, obj):
        return ", ".join([str(kpi) for kpi in obj.kpis.all()])
    formatted_kpis.short_description = 'KPIs'
    def get_queryset(self, request):
        return super().get_queryset(request).prefetch_related('kpis')


admin.site.register(SlaKpiModels, SlaKpiModelsAdmin)
