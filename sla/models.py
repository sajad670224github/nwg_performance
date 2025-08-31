from django.db import models

# Create your models here.

class SlaKpiModels(models.Model):
    id = models.AutoField(primary_key=True)
    kpi_id = models.CharField(max_length=20)
    name = models.CharField(max_length=100)
    technology = models.CharField(max_length=100)
    bh_kpi_id = models.CharField(max_length=20, blank=True, null=True)
    bh_kpi_name = models.CharField(max_length=100, blank=True, null=True)
    special_kpi = models.BooleanField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True, null=True)

    def __str__(self):
        return f"{self.id}--{self.kpi_id}--{self.name}"

class SlaPmModels(models.Model):
    id = models.AutoField(primary_key=True)
    technology = models.CharField(max_length=50)
    layer = models.CharField(max_length=50)
    kpis = models.ManyToManyField(SlaKpiModels,
                                  related_name="pm_kpi",
                                  blank=True,
                                  help_text="List of KPIs and their respective bh information")
    granularity = models.CharField(max_length=2)
    network = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True, null=True)

class Target(models.Model):
    id = models.AutoField(primary_key=True)
    level = models.CharField(max_length=50)
    year = models.IntegerField(max_length=4, default=2023)
    quarter = models.IntegerField(max_length=1, default=1)
    technology = models.CharField(max_length=50)
    kpi = models.CharField(max_length=50)
    region = models.CharField(max_length=50)
    weight = models.FloatField(blank=True, null=True)
    # targets
    min_val = models.FloatField(blank=True, null=True, verbose_name="Min")
    worst2 = models.FloatField(blank=True, null=True)
    worst1 = models.FloatField(blank=True, null=True)
    worst = models.FloatField(blank=True, null=True)
    best = models.FloatField(blank=True, null=True)
    best1 = models.FloatField(blank=True, null=True)
    best2 = models.FloatField(blank=True, null=True)
    max_val = models.FloatField(blank=True, null=True, verbose_name="Max")
    # generic
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True, null=True)


class Baseline(models.Model):
    id = models.AutoField(primary_key=True)
    year = models.IntegerField(max_length=4, default=2023)
    quarter = models.IntegerField(max_length=1, default=1)
    level = models.CharField(max_length=50)
    technology = models.CharField(max_length=10)
    element = models.CharField(max_length=32)
    loss_zone_3 = models.FloatField()
    loss_zone_2 = models.FloatField()
    loss_zone_1 = models.FloatField()
    base = models.FloatField()
    tier0 = models.FloatField()
    tier1 = models.FloatField()
    tier2 = models.FloatField()
    tier3 = models.FloatField()
    # generic
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True, null=True)


class TirInformation(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=50)
    score = models.FloatField()
    # generic
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True, null=True)


class ULImprovementLevel(models.Model):
    id = models.AutoField(primary_key=True)
    year = models.IntegerField(max_length=4, default=2023)
    quarter =  models.IntegerField(default=1)
    technology =  models.CharField(max_length=10)
    level =  models.CharField(max_length=10, help_text="it is a choice from li(load index) and ui(user index)")
    kpi =  models.CharField(max_length=10)
    step = models.IntegerField(max_length=2)
    # generic
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        verbose_name = "User/Load Index Improvement Level"


class ReportHistory(models.Model):
    id = models.AutoField(primary_key=True)
    level = models.CharField(max_length=10, blank=True, null=True)
    technology = models.CharField(max_length=10, blank=True, null=True)
    type = models.CharField(max_length=10, blank=True, null=True)
    day = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)


class TechnologyWeight(models.Model):
    id = models.AutoField(primary_key=True)
    technology = models.CharField(max_length=10, blank=True, null=True)
    hi_weight = models.FloatField()
    li_weight = models.FloatField()
    ui_weight = models.FloatField()
    created_at = models.DateTimeField(auto_now_add=True)