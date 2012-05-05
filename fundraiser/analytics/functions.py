from analytics.cache import *
from analytics.models import *


@cache
def lookup_country(country=None):
    if country is None:
        pass # TODO: an exception should probably be thrown

    # do we want to get or create?  additional error handling would be required
    # if we set a static list and require that a country be in there, but this
    # also means that anomalous log entries are caught
    country, created = Country.objects.get_or_create(iso_code=country)
    return country


@cache
def lookup_language(language=None):
    if language is None:
        pass # TODO: an exception should probably be thrown

    # do we want to get or create?  additional error handling would be required
    # if we set a static list and require that a language be in there, but this
    # also means that anomalous log entries are caught
    language, created = Language.objects.get_or_create(iso_code=language)
    return language

@cache
def lookup_project(project=None):
    if project is None:
        pass # TODO: an exception should probably be thrown

    # do we want to get or create?  additional error handling would be required
    # if we set a static list and require that a project be in there, but this
    # also means that anomalous log entries are caught
    project, created = Project.objects.get_or_create(project=project)
    return project


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
