import re

# Regex based on http://wikitech.wikimedia.org/view/Squid_log_format
squidline = re.compile(
    r"""
        ^(?P<squid>[\S]+) # Name of the squid server
        \t[-]*
        (?P<sequence>[0-9]+) # Sequence ID from the squid server
        \t
        (?P<timestamp>[0-9-]+T[0-9:.]+Z?) # Timestamp
        \t
        (?P<servicetime>[0-9.E-]+) # Request service time
        \t
        (?P<client>[^\t]+) # Client IP address
        \t
        (?P<squidstatus>[^\t]+) # Squid request status and HTTP status code
        \t
        (?P<reply>[0-9-]+) # Reply size including HTTP headers
        \t
        (?P<request>[^\t]+) # Request type
        \t
        (?P<url>[^\t]+) # Request URL
        \t
        (?P<squidhierarchy>[^\t]+) # Squid hierarchy status, peer IP
        \t
        (?P<mime>[^\t]+) # MIME content type
        \t
        (?P<referrer>[^\t]+) # Referer header
        \t
        ((?P<xff>[^\t]+) # X-Forwarded-For header
        \t)?
        (?P<useragent>[^\t]+) # User-Agent header
        \t
        (?P<acceptlanguage>[^\t]+) # Accept-Language header
        \t
        (?P<xcarrier>[^\t]+)$ # X-carrier header
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
              | SOPA/Blackoutpage

            )
        """, re.VERBOSE | re.IGNORECASE
    ),
]

# For the benefit of fr-non-tech people creating links to landing pages, please
# keep https://www.mediawiki.org/w/index.php?title=Fundraising_tech/tools up to
# date with any changes you make to the patterns below!
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
            # match all of the landing page patterns on donatewiki
            (http|https)
            ://
            (?P<sitename>donate.wikimedia.org)/
            (wiki/|w/index.php\?title=)
            (?P<landingpage>
                (
                    L11                 # landing page naming scheme for 2011
                  | L12                 # landing page naming scheme for 2012
                  | L2011               # potential landing page naming scheme for 2011
                  | L2012               # potential landing page naming scheme for 2012
                  | WMF                 # eg WMFJA085
                  | WP                  # eg WP-Video-2016
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
                Special:LandingPage
#              | Special:FundraiserLandingPage # These will now 302 redirect to Special:LandingPage
#              | Special:FundraiserRedirector # these 302 and should result in a valid call to S:FLP
            )
        """, re.VERBOSE | re.IGNORECASE
    )
]

ignore_uas = [
    re.compile(r"""frontend_tester/p14"""),
    re.compile(r"""frontend_tester/p14_1"""),
    re.compile(r"""/home/mwaler/frontend_tester/p14"""),
    re.compile(r"""\./p12"""),
    re.compile(r"""\./p13"""),
    re.compile(r"""\./p14"""),
    re.compile(r"""\./p15"""),
    re.compile(r"""/usr/local/frontend_tester/p12"""),
    re.compile(r"""/usr/local/frontend_tester/p14"""),
    re.compile(r"""^bot""", re.IGNORECASE),
]

phantomJS = re.compile(r"phantomJS", re.IGNORECASE)

sampled = re.compile(
    r"""
        beaconImpressions-
        sampled
        (?P<samplerate>[0-9]+)
    """,
    re.VERBOSE | re.IGNORECASE
)
