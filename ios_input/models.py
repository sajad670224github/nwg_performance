from django.db import models


class AtollData(models.Model):
    id = models.AutoField(primary_key=True)
    cell = models.CharField(max_length=256)
    sector = models.CharField(max_length=256)
    site = models.CharField(max_length=256)
    city = models.CharField(max_length=256)
    province = models.CharField(max_length=256)
    subregion = models.CharField(max_length=256)
    region = models.CharField(max_length=256)
    network = models.CharField(max_length=256)
    vendor = models.CharField(max_length=256)
    technology = models.CharField(max_length=256)
    sectornotech = models.CharField(max_length=256)
    ne = models.CharField(max_length=128, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True, null=True)
    hash_data = models.CharField(max_length=256)