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
        ([\s]+)
        (?P<referrer>[\S]+) # Referer header
        ([\s]+)
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


landingpages = [
    re.compile(
        r"""
            (http|https)
            ://
            wikimediafoundation.org/w/index.php?title=
            (Special:LandingCheck|index.php)
        """, re.VERBOSE | re.IGNORECASE
    )
]