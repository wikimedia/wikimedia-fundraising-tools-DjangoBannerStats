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

