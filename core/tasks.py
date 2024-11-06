# core/tasks.py
from celery import shared_task

@shared_task
def run_script():
    print("Task run_script started and running.")
