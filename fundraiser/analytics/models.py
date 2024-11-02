from django.db import models


class SquidLog(models.Model):
    "Return information about a log file"
    id = models.IntegerField(primary_key=True)
    filename = models.CharField(max_length=128)
    impressiontype = models.CharField(max_length=128)
    timestamp = models.DateTimeField(auto_now_add=False)

    class Meta:
        db_table = "squidlog"
        managed = False

    def __unicode__(self):
        return self.filename

    def filename2timestamp(self):
        import datetime
        import re

        regex = re.compile(
            r"""
                (beaconImpressions|landingpages)
                (-sampled[0-9]+)?
                (\.tab|\.tsv)?
                [-.]
                (?P<timestamp>[0-9-AMP]+)
                .log.gz
            """,
            re.IGNORECASE | re.VERBOSE
        )

        parts = regex.match(self.filename)

        if not parts:
            raise ValueError(
                f"Filename does not match existing patterns for squid logs: {self.filename}"
            )

        try:
            timestamp = datetime.datetime.strptime(parts.group("timestamp"), "%Y%m%d-%H%M%S")
        except ValueError:
            # Don't catch this one
            timestamp = datetime.datetime.strptime(parts.group("timestamp"), "%Y-%m-%d-%I%p--%M")
        return timestamp


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
    squid = models.ForeignKey(SquidHost, on_delete=models.CASCADE)
    sequence = models.IntegerField()
    timestamp = models.DateTimeField(auto_now_add=False)

    class Meta:
        db_table = 'squidrecord'
        managed = False

    def __unicode__(self):
        return "%s - %s" % (self.squid, self.sequence)


class Project(models.Model):
    "Lookup table for project names"
    id = models.IntegerField(primary_key=True)
    project = models.CharField(max_length=255)

    class Meta:
        db_table = 'project'
        managed = False

    def __unicode__(self):
        return self.project


class Language(models.Model):
    "Lookup table for languages"
    id = models.IntegerField(primary_key=True)
    language = models.CharField(max_length=255)
    iso_code = models.CharField(max_length=12)

    class Meta:
        db_table = 'language'
        managed = False

    def __unicode__(self):
        return "%s (%s)" % (self.iso_code, self.language)


class Country(models.Model):
    "Lookup table for countries"
    id = models.IntegerField(primary_key=True)
    country = models.CharField(max_length=255)
    iso_code = models.CharField(max_length=12)

    class Meta:
        db_table = 'country'
        managed = False

    def __unicode__(self):
        return self.iso_code


class BannerImpression(models.Model):
    ""
    id = models.IntegerField(primary_key=True)
    timestamp = models.DateTimeField(auto_now_add=False)
#    squidrecord = models.ForeignKey(SquidRecord, null=True)
    banner = models.CharField(max_length=255)
    campaign = models.CharField(max_length=255)
    project = models.ForeignKey(Project, null=True, on_delete=models.CASCADE)
    language = models.ForeignKey(Language, null=True, on_delete=models.CASCADE)
    country = models.ForeignKey(Country, null=True, on_delete=models.CASCADE)

    class Meta:
        db_table = 'bannerimpression_raw'
        managed = False


class LandingPageImpression(models.Model):
    ""
    id = models.IntegerField(primary_key=True)
    timestamp = models.DateTimeField(auto_now_add=False)
    utm_source = models.CharField(max_length=255)
    utm_campaign = models.CharField(max_length=255)
    utm_key = models.CharField(max_length=128)
    utm_medium = models.CharField(max_length=255)
    landing_page = models.CharField(max_length=255)
    project = models.ForeignKey(Project, null=True, on_delete=models.CASCADE)
    language = models.ForeignKey(Language, null=True, on_delete=models.CASCADE)
    country = models.ForeignKey(Country, null=True, on_delete=models.CASCADE)

    class Meta:
        db_table = 'landingpageimpression_raw'
        managed = False


class LandingPageImpressions(models.Model):
    ""
    id = models.IntegerField(primary_key=True)
    timestamp = models.DateTimeField(auto_now_add=False)
    utm_source = models.CharField(max_length=255)
    utm_campaign = models.CharField(max_length=255)
    utm_medium = models.CharField(max_length=255)
    landing_page = models.CharField(max_length=255)
    project = models.ForeignKey(Project, null=True, on_delete=models.CASCADE)
    language = models.ForeignKey(Language, null=True, on_delete=models.CASCADE)
    country = models.ForeignKey(Country, null=True, on_delete=models.CASCADE)
    count = models.IntegerField()

    class Meta:
        db_table = 'landingpageimpressions'
        managed = False


class RecentHit(models.Model):
    "Used to keep track of number of hits per IP in the past day"
    id = models.IntegerField(primary_key=True)
    timestamp = models.DateTimeField(auto_now_add=False)
    address = models.CharField(max_length=255)

    class Meta:
        db_table = 'recenthits'
        managed = False
