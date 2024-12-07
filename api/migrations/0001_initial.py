# Generated by Django 5.1.3 on 2024-12-07 06:49

import django.contrib.gis.db.models.fields
import django.db.models.deletion
import django.utils.timezone
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Site',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=255, unique=True)),
                ('location_polygon', django.contrib.gis.db.models.fields.PolygonField(srid=4326)),
                ('coordinates_record', models.JSONField(blank=True, null=True)),
                ('site_type', models.CharField(choices=[('Point', 'Point'), ('Rectangle', 'Rectangle'), ('Polygon', 'Polygon')], default='Polygon', max_length=10)),
                ('created_at', models.DateTimeField(default=django.utils.timezone.now, editable=False)),
                ('updated_at', models.DateTimeField(default=django.utils.timezone.now)),
            ],
        ),
        migrations.CreateModel(
            name='Group',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=255, unique=True)),
                ('description', models.TextField(blank=True, null=True)),
                ('created_at', models.DateTimeField(default=django.utils.timezone.now, editable=False)),
                ('updated_at', models.DateTimeField(default=django.utils.timezone.now)),
                ('parent', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='subgroups', to='api.group')),
            ],
        ),
        migrations.CreateModel(
            name='GroupSite',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('assigned_at', models.DateTimeField(default=django.utils.timezone.now)),
                ('group', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='group_sites', to='api.group')),
                ('site', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='site_groups', to='api.site')),
            ],
            options={
                'unique_together': {('group', 'site')},
            },
        ),
    ]
