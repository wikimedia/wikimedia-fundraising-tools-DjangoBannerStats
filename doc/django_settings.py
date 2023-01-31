# Django settings for fundraiser project.

# Proxy log source directory
UDP_LOG_PATH = "/srv/archive/banner_logs"

# Database configuration
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': '<DATABASE_NAME',
        'USER': '<DATABASE_USER>',
        'PASSWORD': '<TOP_SECRET_1>',
        'HOST': '<DATABASE_SERVER>',
        'PORT': '',
    },
}

# Make this unique, and don't share it with anybody.
SECRET_KEY = '<TOP_SECRET_2>'

# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
TIME_ZONE = None  # 'America/Chicago' # Setting to None to keep things in UTC

# Language code for this installation. All choices can be found here:
# http://www.i18nguy.com/unicode/language-identifiers.html
LANGUAGE_CODE = 'en-us'

# Logging level adjustment
DEBUG = True
TEMPLATE_DEBUG = DEBUG

SITE_ID = 1

# If you set this to False, Django will make some optimizations so as not
# to load the internationalization machinery.
USE_I18N = True

# If you set this to False, Django will not format dates, numbers and
# calendars according to the current locale.
USE_L10N = True

# If you set this to False, Django will not use timezone-aware datetimes.
USE_TZ = False

INSTALLED_APPS = [
    'fundraiser.analytics'
]

# A sample logging configuration. The only tangible logging
# performed by this configuration is to send an email to
# the site admins on every HTTP 500 error when DEBUG=False.
# See http://docs.djangoproject.com/en/dev/topics/logging for
# more details on how to customize your logging configuration.
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'filters': {},
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler'
        },
        'mail_admins': {
            'level': 'ERROR',
            'filters': [],
            'class': 'django.utils.log.AdminEmailHandler'
        }
    },
    'loggers': {
        'django.request': {
            'handlers': ['mail_admins'],
            'level': 'ERROR',
            'propagate': True,
        },
        'fundraiser': {
            'handlers': ['console'],
            'level': 'DEBUG'
        }
    }
}

# In-memory object caches to reduce database query load
CACHES = {
    'default': {},
    'country': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'country',
        'TIMEOUT': None,
        'MAX_ENTRIES': 3000,
    },
    'language': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'language',
        'TIMEOUT': None,
        'MAX_ENTRIES': 1000,
    },
    'project': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'project',
        'TIMEOUT': None,
    },
    'squid': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'squid',
        'TIMEOUT': None,
    },
}
