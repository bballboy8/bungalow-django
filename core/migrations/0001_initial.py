# Generated by Django 5.1.3 on 2024-11-07 09:40

import django.contrib.gis.db.models.fields
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='SatelliteCaptureCatalog',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('acquisition_datetime', models.DateTimeField(blank=True, null=True)),
                ('cloud_cover', models.FloatField(blank=True, null=True)),
                ('vendor_id', models.CharField(blank=True, max_length=255, null=True)),
                ('vendor_name', models.CharField(choices=[('airbus', 'airbus'), ('blacksky', 'blacksky'), ('planet', 'planet'), ('maxar', 'maxar'), ('capella', 'capella'), ('skyfi', 'skyfi')], max_length=50)),
                ('sensor', models.TextField(blank=True, null=True)),
                ('area', models.FloatField(blank=True, null=True)),
                ('type', models.CharField(blank=True, choices=[('Day', 'Day'), ('Night', 'Night')], max_length=8, null=True)),
                ('sun_elevation', models.FloatField(blank=True, null=True)),
                ('resolution', models.CharField(blank=True, max_length=50, null=True)),
                ('georeferenced', models.BooleanField(blank=True, null=True)),
                ('location_polygon', django.contrib.gis.db.models.fields.PolygonField(blank=True, null=True, srid=4326)),
            ],
            options={
                'indexes': [models.Index(fields=['acquisition_datetime'], name='core_satell_acquisi_531120_idx'), models.Index(fields=['vendor_name'], name='core_satell_vendor__a1e639_idx'), models.Index(fields=['vendor_id'], name='core_satell_vendor__f4cb0a_idx')],
            },
        ),
    ]
