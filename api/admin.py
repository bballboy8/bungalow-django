from django.contrib import admin
from api.models import *
# Register your models here.

class GroupSiteInline(admin.TabularInline):
    model = GroupSite
    extra = 0

class GroupAdmin(admin.ModelAdmin):
    inlines = [GroupSiteInline]
    list_display = ('name', 'parent', 'description', 'created_at', 'updated_at')
    search_fields = ('name', 'description')
    list_filter = ('created_at', 'updated_at')

class SiteAdmin(admin.ModelAdmin):
    list_display = ('name', 'location_polygon', 'coordinates_record', 'created_at', 'updated_at')
    search_fields = ('name', 'coordinates_record')
    list_filter = ('created_at', 'updated_at')

admin.site.register(Group, GroupAdmin)
admin.site.register(Site, SiteAdmin)
admin.site.register(GroupSite)