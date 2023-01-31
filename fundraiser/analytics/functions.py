'''Various functions.'''
import re
from datetime import timedelta
from django.core.cache import caches
from fundraiser.analytics.models import Country, Language, Project, SquidHost


def lookup_country(country_string=None, logger=None):
    """
    Looks up the Country object for a given string. If no matching country is
    found, a default non-country "XX" is used.

    country_string -- a string representing the country from which the request was made
    logger -- logging handle

    Returns: a Country object or None on error
    """
    default = "XX"
    if country_string and re.match(r"[a-z][a-z]$", country_string, re.IGNORECASE):
        country_string = country_string.upper()
    else:
        logger.info("** INVALID COUNTRY (%s), returning default (%s)", country_string, default)
        country_string = default
    country = caches["country"].get(country_string)
    if not country:
        logger.info("** LOOKUP COUNTRY %s", country_string)
        try:
            country = Country.objects.get(iso_code=country_string)
        except Country.DoesNotExist:
            logger.info("** UNLISTED COUNTRY (%s), returning default (%s)", country_string, default)
            country_string = default
            try:
                country = Country.objects.get(iso_code=country_string)
            except Country.DoesNotExist:
                logger.warning("** DEFAULT COUNTRY '%s' MISSING FROM DATABASE", default)
        except Exception as unhandled_error:
            logger.warning("** UNHANDLED ERROR: %s", unhandled_error)
        caches["country"].set(country_string, country)
    return country


def lookup_language(language_string=None, logger=None):
    """
    Looks up the Language object for a given string. If no matching country is
    found, a default non-country "other" is used.

    language_string -- a string representing the language from which the request was made
    logger -- logging handle

    Returns: a Language object or None on error
    """
    default = "other"
    if language_string and re.match(r"[a-z\-]{2,12}$", language_string, re.IGNORECASE):
        language_string = language_string.lower()
    else:
        logger.info("** INVALID LANGUAGE (%s), returning default (%s)", language_string, default)
        language = default
    language = caches["language"].get(language_string)
    if not language:
        logger.info("** LOOKUP LANGUAGE %s", language_string)
        try:
            language = Language.objects.get(iso_code=language_string)
        except Language.DoesNotExist:
            logger.info("** UNLISTED LANGUAGE (%s), returning default (%s)", language_string, default)
            language_string = default
            try:
                language = Language.objects.get(iso_code=language_string)
            except Language.DoesNotExist:
                logger.warning("** DEFAULT COUNTRY '%s' MISSING FROM DATABASE", default)
        except Exception as unhandled_error:
            logger.warning("** UNHANDLED ERROR: %s", unhandled_error)
        caches["language"].set(language_string, language)
    return language


def lookup_project(project_string=None, logger=None):
    """
    Looks up the Project object for a given string. A new Project object will
    be created if it does not already exist.

    Keyword arguments:
    project_string -- a string representing the project from which the request was made
    logger -- logging handle

    Returns: a Project object or None on error
    """
    default = "donatewiki"
    if not project_string or not re.match(r"[\w]{,128}$", project_string):
        logger.info("** INVALID PROJECT (%s), returning default (%s)", project_string, default)
        project_string = default
    project = caches["project"].get(project_string)
    if not project:
        logger.info("** LOOKUP PROJECT %s", project_string)
        try:
            project = Project.objects.get(project=project_string)
        except Project.DoesNotExist:
            Project.objects.create(project=project_string)
            project = Project.objects.get(project=project_string)
            if project:
                logger.info("** CREATED PROJECT FOR: %s (id:%d)", project.project, project.id)
        except Warning:
            pass
        caches["project"].set(project_string, project)
    return project


def lookup_squidhost(hostname=None, logger=None):
    """
    Looks up the SquidHost object for a given hostname. A SquidHost object will
    be created if it does not already exist.

    Keyword arguments:
    hostname -- the hostname of the squid serving the request
    logger -- logging handle

    Returns: a SquidHost object or None on error
    """
    default = "unknown"
    if not hostname:
        logger.info("** INVALID SQUID HOSTNAME, returning default (%s)", default)
        hostname = default
    elif len(hostname) > 128:
        logger.info("** TRUNCATING SQUID HOSTNAME %s to %s", hostname, hostname[:128])
        hostname = hostname[:128]
    squid = caches["squid"].get(hostname)
    if not squid:
        logger.info("** LOOKUP SQUID %s", hostname)
        try:
            squid = SquidHost.objects.get(hostname=hostname)
        except SquidHost.DoesNotExist:
            SquidHost.objects.create(hostname=hostname)
            squid = SquidHost.objects.get(hostname=hostname)
            if squid:
                logger.info("** CREATED SQUID HOST FOR: %s (id:%d)", squid.hostname, squid.id)
        except Warning:
            pass
        caches["squid"].set(hostname, squid)
    return squid


def roundtime(time, minutes=1, midpoint=True):
    """
    NOTE: minutes should be less than 60
    """
    time += timedelta(minutes=-(time.minute % minutes), seconds=-time.second)

    if midpoint:
        time += timedelta(seconds=minutes * 60 / 2)

    return time
