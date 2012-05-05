from django.db import models


class SquidHost(models.Model):
    "Lookup table for squid hostnames"
    id = models.IntegerField(primary_key=True)
    hostname = models.CharField(max_length=128)

    class Meta:
        db_table = 'squidhost'
        managed = False

    def __unicode__(self):
        return self.hostname

class SquidRecord(models.Model):
    ""
    id = models.IntegerField(primary_key=True)
    squid = models.ForeignKey(SquidHost)
    sequence = models.IntegerField()
    timestamp = models.DateTimeField(auto_now_add=False)

    class Meta:
        db_table = 'squidrecord'
        managed = False

class Project(models.Model):
    "Lookup table for project names"
    id = models.IntegerField(primary_key=True)
    project = models.CharField(max_length=255)

    class Meta:
        db_table = 'project'
        managed = False

class Language(models.Model):
    "Lookup table for languages"
    id = models.IntegerField(primary_key=True)
    language = models.CharField(max_length=255)
    iso_code = models.CharField(max_length=12)

    class Meta:
        db_table = 'language'
        managed = False

class Country(models.Model):
    "Lookup table for countries"
    id = models.IntegerField(primary_key=True)
    country = models.CharField(max_length=255)
    iso_code = models.CharField(max_length=12)

    class Meta:
        db_table = 'country'
        managed = False


class BannerImpression(models.Model):
    ""
    id = models.IntegerField(primary_key=True)
    timestamp = models.DateTimeField(auto_now_add=False)
#    squidrecord = models.ForeignKey(SquidRecord, null=True)
    banner = models.CharField(max_length=255)
    campaign = models.CharField(max_length=255)
    project = models.ForeignKey(Project, null=True)
    language = models.ForeignKey(Language, null=True)
    country = models.ForeignKey(Country, null=True)

    class Meta:
        db_table = 'bannerimpression_raw'
        managed = False

