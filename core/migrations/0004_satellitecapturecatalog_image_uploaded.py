# Generated by Django 5.1.3 on 2024-11-16 06:32

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0003_satellitecapturecatalog_created_at_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='satellitecapturecatalog',
            name='image_uploaded',
            field=models.BooleanField(blank=True, default=False, null=True),
        ),
    ]
