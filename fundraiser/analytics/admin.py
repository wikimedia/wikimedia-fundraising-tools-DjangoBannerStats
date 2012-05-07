from django.contrib import admin

from fundraiser.analytics.models import *

class LanguageAdmin(admin.ModelAdmin):
    list_display = ('iso_code', 'language')

admin.site.register(BannerImpression)
admin.site.register(Language, LanguageAdmin)
admin.site.register(Country)
admin.site.register(Project)
