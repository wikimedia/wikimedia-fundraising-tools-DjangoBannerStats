from fundraiser.analytics.cache import *
from fundraiser.analytics.models import *


@cache
def lookup_country(country=None):
    try:
        if country is None:
            pass # TODO: an exception should probably be thrown

        # do we want to get or create?  additional error handling would be required
        # if we set a static list and require that a country be in there, but this
        # also means that anomalous log entries are caught
        country, created = Country.objects.get_or_create(iso_code=country)
        return country
    except Exception:
        print "INVALID COUNTRY: %s" % country

@cache
def lookup_language(language=None):
    try:
        if language is None:
            pass # TODO: an exception should probably be thrown

        # do we want to get or create?  additional error handling would be required
        # if we set a static list and require that a language be in there, but this
        # also means that anomalous log entries are caught
        language, created = Language.objects.get_or_create(iso_code=language)
        return language
    except Exception:
        print "INVALID LANGUAGE: %s" % language

@cache
def lookup_project(project=None):
    try:
        if project is None:
            pass # TODO: an exception should probably be thrown

        # do we want to get or create?  additional error handling would be required
        # if we set a static list and require that a project be in there, but this
        # also means that anomalous log entries are caught
        project, created = Project.objects.get_or_create(project=project)
        return project
    except Exception as e:
        print "INVALID PROJECT: %s" % project

@cache
def lookup_squidhost(hostname=None, create=True, verbose=False):
    "Gets the FK of the SquidHost"
    if not hostname:
        return False

    try:
        squid = SquidHost.objects.get(hostname=hostname)
        return squid
    except SquidHost.DoesNotExist:
        if create:
            # doing this in two steps as unmanaged models seem to have an issue immediately grabbing the pk
            SquidHost.objects.create(hostname=hostname)
            squid = SquidHost.objects.get(hostname=hostname)
            if verbose and squid:
                print "** CREATED SQUID HOST FOR: %s (id:%d)" % (squid.hostname, squid.pk)
            return squid
        else:
            return False
