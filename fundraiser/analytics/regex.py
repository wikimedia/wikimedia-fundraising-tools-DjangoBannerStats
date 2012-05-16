import re

# Regex based on http://wikitech.wikimedia.org/view/Squid_log_format
squidline = re.compile(
    r"""
        (?P<squid>[\S]+) # Name of the squid server
        (\s[-]*)
        (?P<sequence>[0-9]+) # Sequence ID from the squid server
        (\s)
        (?P<timestamp>[0-9-]+T[0-9:.]+) # Timestamp
        (\s)
        (?P<servicetime>[0-9.]+) # Request service time
        (\s)
        (?P<client>[0-9.]+) # Client IP address
        (\s)
        (?P<squidstatus>[\S]+) # Squid request status and HTTP status code
        (\s)
        (?P<reply>[0-9]+) # Reply size including HTTP headers
        (\s)
        (?P<request>[\S]+) # Request type
        (\s)
        (?P<url>[\S]+) # Request URL
        (\s)
        (?P<squidhierarchy>[\S]+) # Squid hierarchy status, peer IP
        (\s)
        (?P<mime>[\S]+) # MIME content type
        (\s)
        (?P<referrer>[\S]+) # Referer header
        (\s)
        (?P<xff>[\S]+) # X-Forwarded-For header
        (\s)
        (?P<useragent>[\S\s]+) # User-Agent header
    """, re.VERBOSE
)

# Based on urlparse.urlsplit which is really slow but only does:
# <scheme>://<netloc>/<path>?<query>#<fragment>
# This regex does not replicate all functionality, just optimizes
# even further for our purposes
urlparts = re.compile(
    r"""
        (?P<scheme>http|https)
        ://
        (?P<netloc>(?:(?!/|\?|\#)\S)*)
        /?
        (?P<path>(?:(?!\?|\#)\S)*)
        \??
        (?P<query>(?:(?!\#)\S)*)
        \#?
        (?P<fragment>[\S]*)
    """, re.VERBOSE
)

# Splits a querystring into its constituent key/value pairs
queryparts = re.compile(
    r"""
    ((?:(?!=)\S)*)
    =
    ((?:(?!&)\S)*)
    &?
    """, re.VERBOSE
)

# some quick regexes to pare down the parse list and not try our best to parse everything
landingpages_ignore = [
    re.compile(
        r"""
            # ignore calls to Special:LandingCheck. They 302 and then result in a proper call to the landing page
            # also ignore Special:ContributionTracking and the MediaWiki namespace
            (http|https)
            ://wikimediafoundation.org/
            (
                wiki/
              | w/index.php\?title=
            )
            (
                Special:Landingcheck
              | Special:ContributionTracking
              | Special:RecentChanges
              | MediaWiki:
              | File:
              | Talk:
            )
        """, re.VERBOSE | re.IGNORECASE
    ),
    re.compile(
        r"""
            # ignore calls to the api as well as the favicon and the MediaWiki namespace
            (http|https)
            ://wikimediafoundation.org/
            (
                upload/
              | w/
                (
                    skins-
                  | api.php
                  | opensearch_desc.php
                  | index.php\?search=
                  | extensions/
                )
              | favicon.ico
              | tracker/bannerImpression.php
            )
        """, re.VERBOSE | re.IGNORECASE
    ),
    re.compile(
        r"""
            # ignore the ToU for now
            (http|https)
            ://wikimediafoundation.org/
            (
                wiki/
              | w/index.php\?title=
            )
            (
                Terms_of_Use
              | New_Terms_of_use
              | New%20Terms%20of%20use
              | Feedback_privacy_statement
              | Home
              | Main_Page
              | Donate/Benefactor
              | Donate/Stories
              | Donate/Thank_You
              | Donate/Transparency

            )
        """, re.VERBOSE | re.IGNORECASE
    ),
]

landingpages = [
    re.compile(
        r"""
            # match all of the landing page patterns on wmfwiki
            (http|https)
            ://
            (?P<sitename>wikimediafoundation.org)/
            (wiki/|w/index.php\?title=)
            (?P<landingpage>
                (
                    L11                 # landing page naming scheme for 2011
                  | L12                 # landing page naming scheme for 2012
                  | L2011               # potential landing page naming scheme for 2011
                  | L2012               # potential landing page naming scheme for 2012
                  | WMF                 # eg WMFJA085
                  | Donate              # old forms, keeping so that we can possibly redirect them all
                  | Support_Wikipedia   # old forms
                  | Test_120511         # Test from 2012-05-11
                )
                (?:(?!\?|&)[\S])*         # this will give us the landing page up to the next ? or &
            )
        """, re.VERBOSE | re.IGNORECASE
    ),
    re.compile(
        r"""
            (http|https)
            ://
            (?P<sitename>donate.wikimedia.org)/
            (
                wiki/
              | w/index.php\?title=
            )
            (
                Special:FundraiserLandingPage
#              | Special:FundraiserRedirector # these 302 and should result in a valid call to S:FLP
            )
        """, re.VERBOSE | re.IGNORECASE
    )
]