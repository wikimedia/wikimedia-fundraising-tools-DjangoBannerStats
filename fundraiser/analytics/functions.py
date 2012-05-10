from fundraiser.analytics.cache import cache
from fundraiser.analytics.models import Country, Language, Project, SquidHost

@cache
def lookup_country(country=None, default="XX", create=True, verbose=False):
    """
    Looks up the Country object for a given string.  If create=True, a Country
    object will be created if it does not already exist.

    Keyword arguments:
    country -- a string representing the country from which the request was made
    default -- the default country if the country is blank or unspecified
    create -- if True, creates a new Country object if one does not already exist
    verbose -- provides verbose logging output NOTE: due to the cached nature of this
        function, the verbose output will only be printed on the first instance of
        a country during each run

    Returns: a Country object or None on error

    TODO: is there any (sane) way to fix the verbose logging with caching enabled?
    """
    # check for bad input values and try to return a default
    if not country or country is None:
        if not default is False:
            if verbose:
                print "** INVALID COUNTRY, returning default (%s)" % default
            return lookup_country(default, False, create, verbose)
        else:
            return None
    if len(country) > 8:
        if verbose:
            print "** TRUNCATING COUNTRY ISO_CODE %s to %s" % (country, country[:8])
        language = country[:8]
    try:
        country = Country.objects.get(iso_code=country)
        return country
    except Country.DoesNotExist:
        if create:
            # doing this in two steps as unmanaged models seem to have an
            # issue immediately grabbing the primary key (id)
            Country.objects.create(iso_code=country)
            country = Country.objects.get(iso_code=country)
            if verbose and country:
                print "** CREATED COUNTRY FOR: %s (id:%d)" % (country.iso_code, country.id)
            return country
        else:
            return None

@cache
def lookup_language(language=None, default="en", create=True, verbose=False):
    """
    Looks up the Language object for a given string.  If create=True, a Language
    object will be created if it does not already exist.

    Keyword arguments:
    language -- a string representing the language from which the request was made
    default -- the default language if the language is blank or unspecified
    create -- if True, creates a new Language object if one does not already exist
    verbose -- provides verbose logging output NOTE: due to the cached nature of this
        function, the verbose output will only be printed on the first instance of
        a language during each run

    Returns: a Language object or None on error

    TODO: is there any (sane) way to fix the verbose logging with caching enabled?
    """
    # check for bad input values and try to return a default
    if not language or language is None:
        if not default is False:
            if verbose:
                print "** INVALID LANGUAGE, returning default (%s)" % default
            return lookup_language(default, False, create, verbose)
        else:
            return None
    if len(language) > 24:
        if verbose:
            print "** TRUNCATING LANGUAGE ISO_CODE %s to %s" % (language, language[:24])
        language = language[:24]
    try:
        language = Language.objects.get(iso_code=language)
        return language
    except Language.DoesNotExist:
        if create:
            # doing this in two steps as unmanaged models seem to have an
            # issue immediately grabbing the primary key (id)
            Language.objects.create(iso_code=language)
            language = Language.objects.get(iso_code=language)
            if verbose and language:
                print "** CREATED LANGUAGE FOR: %s (id:%d)" % (language.iso_code, language.id)
            return language
        else:
            return None

@cache
def lookup_project(project=None, default="donatewiki", create=True, verbose=False):
    """
    Looks up the Project object for a given string.  If create=True, a Project
    object will be created if it does not already exist.

    Keyword arguments:
    project -- a string representing the project from which the request was made
    default -- the default project if the project is blank or unspecified
    create -- if True, creates a new Project object if one does not already exist
    verbose -- provides verbose logging output NOTE: due to the cached nature of this
        function, the verbose output will only be printed on the first instance of
        a project during each run

    Returns: a Project object or None on error

    TODO: is there any (sane) way to fix the verbose logging with caching enabled?
    """
    # check for bad input values and try to return a default
    if not project or project is None:
        if not default is False:
            if verbose:
                print "** INVALID PROJECT, returning default (%s)" % default
            return lookup_project(default, False, create, verbose)
        else:
            return None
    if len(project) > 128:
        if verbose:
            print "** TRUNCATING PROJECT %s to %s" % (project, project[:128])
        project = project[:128]
    try:
        project = Project.objects.get(project=project)
        return project
    except Project.DoesNotExist:
        if create:
            # doing this in two steps as unmanaged models seem to have an
            # issue immediately grabbing the primary key (id)
            Project.objects.create(project=project)
            project = Project.objects.get(project=project)
            if verbose and project:
                print "** CREATED PROJECT FOR: %s (id:%d)" % (project.project, project.id)
            return project
        else:
            return None

@cache
def lookup_squidhost(hostname=None, default="unknown", create=True, verbose=False):
    """
    Looks up the SquidHost object for a given hostname.  If create=True, a SquidHost
    object will be created if it does not already exist.

    Keyword arguments:
    hostname -- the hostname of the squid serving the request
    default -- the default hostname if the hostname is blank or unspecified
    create -- if True, creates a new SquidHost object if one does not already exist
    verbose -- provides verbose logging output NOTE: due to the cached nature of this
        function, the verbose output will only be printed on the first instance of
        the hostname during each run

    Returns: a SquidHost object or None on error

    TODO: is there any (sane) way to fix the verbose logging with caching enabled?
    """
    # check for bad input values and try to return a default
    if not hostname or hostname is None:
        if not default is False:
            if verbose:
                print "** INVALID SQUID HOSTNAME, returning default (%s)" % default
            return lookup_squidhost(default, False, create, verbose)
        else:
            return None
    if len(hostname) > 128:
        if verbose:
            print "** TRUNCATING SQUID HOSTNAME %s to %s" % (hostname, hostname[:128])
        hostname = hostname[:128]
    try:
        squid = SquidHost.objects.get(hostname=hostname)
        return squid
    except SquidHost.DoesNotExist:
        if create:
            # doing this in two steps as unmanaged models seem to have an
            # issue immediately grabbing the primary key (id)
            SquidHost.objects.create(hostname=hostname)
            squid = SquidHost.objects.get(hostname=hostname)
            if verbose and squid:
                print "** CREATED SQUID HOST FOR: %s (id:%d)" % (squid.hostname, squid.id)
            return squid
        else:
            return None