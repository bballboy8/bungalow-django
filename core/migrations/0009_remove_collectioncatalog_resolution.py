# Generated by Django 5.1.3 on 2025-02-08 09:30

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0008_collectioncatalog'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='collectioncatalog',
            name='resolution',
        ),
    ]
