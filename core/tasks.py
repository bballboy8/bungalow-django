# core/tasks.py
from celery import shared_task
from core.services.blacksky_catalog_api import run_blacksky_catalog_api

@shared_task
def run_blacksky_script():
    run_blacksky_catalog_api()
