from django.conf.urls import patterns, include, url

# Uncomment the next two lines to enable the admin:
from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('fundraiser.analytics',
    # Examples:
    # url(r'^$', 'fundraiser.views.home', name='home'),

    url(r'^$', 'views.hello_world', name='hello_world'),
    url(u'campaign_ecom/$', 'views.campaign_ecom', name='fundraiser.analytics.campaign_ecom'),

)
