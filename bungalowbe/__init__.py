# myproject/__init__.py
from bungalowbe.celery import app as celery_app
__all__ = ('celery_app',)
