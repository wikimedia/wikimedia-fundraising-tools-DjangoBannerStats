from django.contrib import admin

from models import *

class LanguageAdmin(admin.ModelAdmin):
    list_display = ('iso_code', 'language')


admin.site.register(Language, LanguageAdmin)
