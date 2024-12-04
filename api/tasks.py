from celery import shared_task



@shared_task
def run_image_seeder(captures):
    print(captures[0])
    print("Image seeder task is running")
    pass