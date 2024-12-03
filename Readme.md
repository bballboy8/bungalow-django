- Bungalow

## Clone Project

1. Clone the Project on Local Machine.
2. Paste .env in Main Directory.

## Runtime

1. Python 3.12
2. Windows/Linux/Mac

## Setting up Virtual Environment

1. python3.12 -m venv venv (just outside project Directory)
2. Activate the Virtual Environment.
3. cd into the project directory.
4. run this command -> pip install -r requirements.txt
5. Start Docker for Redis Server(Django Channels) -> sudo docker run -p 6379:6379 -d redis:5 (Optional)

## Installation

1. run cp .env.sample .env
2. Create a database in Postgres and add relevent DB credentials in .env
3. Run python manage.py migrate.
4. Run daphne bungalowbe.asgi:application.

Project is setup and ready to be used.

## Requirements for Geo Location

1. CREATE EXTENSION postgis; (Run this command in Postgres pgAdmin)

2. Install PostGIS UBUNTU:
   sudo apt-get install gdal-bin libgdal-dev
   sudo apt install postgresql postgresql-contrib postgis

3. In case spatial_ref_sys table is not present in the database, run the following command:
   psql -d <database_name> -c "CREATE EXTENSION postgis;"

4. If spatial sys table is empty:
   INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, proj4text, srtext)
   VALUES (4326, 'EPSG', 4326, '+proj=longlat +datum=WGS84 +no_defs ', 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]');


<!-- Celery pm2 or screen -->
1. Process:  celery -A bungalowbe.celery worker -l info
2. Process:  celery -A bungalowbe beat -l info

