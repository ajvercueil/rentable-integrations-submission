# Not in use ATM - Just a reference file (can't have 1 celery config managing 2 separate tasks)

from celery import Celery

# Celery configuration
app = Celery(
    'tasks',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0'
)

# Optional Celery configurations
app.conf.update(
    result_expires=3600,
)

# Celery Beat Configuration:
'''
# Celery configuration
app = Celery(
    'tasks',
    broker='redis://localhost:6379/1',
    backend='redis://localhost:6379/1'
)

# Celery Beat Schedule (Every 30 seconds)
app.conf.beat_schedule = {
    'update-weather-every-30-seconds': {
        'task': 'weather_updater.update_weather_from_weatherlink',  # Name of the task
        'schedule': 30.0,  # Run every 30 seconds
    },
}

# Celery Beat Schedule (Every Monday morning at 7:30 a.m. UST)
app.conf.beat_schedule = {
    # Executes every Monday morning at 7:30 a.m.
    'update-weather-every-monday-morning': {
        'task': 'update_weather_from_weatherlink',
        'schedule': crontab(hour=7, minute=30, day_of_week=1),
        'args': (16, 16),
    },
}
'''


# Set timezone by uncommenting single line below (optional, adjust as needed)
# app.conf.timezone = 'UTC'

'''
import requests
from lxml import etree
import uuid

unique_run_id = uuid.uuid4()
print('Unique Run ID: ' + str(unique_run_id))
filepath = f'abodo_feed_{unique_run_id}.xml'

response = requests.get('https://aj-s3-test-bucket.s3.us-west-1.amazonaws.com/abodo_feed.xml')
if response.status_code == 200:
    with open(filepath, 'wb') as file:
        file.write(response.content)
    print("File saved successfully (Filepath: " + filepath + ")")

tree = etree.parse(filepath)
root = tree.getroot()
properties = root.findall('.//Property')
'''