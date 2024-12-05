# api/tasks.py
from celery import shared_task


@shared_task
def run_image_seeder(captures):
    print("Running image seeder")
    print("Image seeder completed")
    return "Image seeder completed"