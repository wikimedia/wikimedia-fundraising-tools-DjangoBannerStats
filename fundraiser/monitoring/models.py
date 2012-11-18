
from django.db import models

class GangliaSource(models.Model):
    name = models.CharField(max_length=255)
    cluster = models.CharField(max_length=255)
    host = models.CharField(max_length=255)
    metric = models.CharField(max_length=255)
    range = models.CharField(max_length=255)

class GangliaData(models.Model):
    source = models.ForeignKey(GangliaSource)
    timestamp = models.DateTimeField(auto_now_add=False)
    value = models.IntegerField()

