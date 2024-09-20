import requests
import boto3
from utilities import convert_floats_to_decimal
from celery import Celery
from botocore.exceptions import ClientError

# Celery configuration
app = Celery(
    'tasks',
    broker='redis://localhost:6379/1',
    backend='redis://localhost:6379/1'
)
# Celery beat schedule
app.conf.beat_schedule = {
    'update-weather-every-30-seconds': {
        'task': 'scheduled_weather_updater.update_weather_from_weatherlink',  # Name of the task
        'schedule': 30.0,  # Run every 30 seconds
    },
}

# Connect to DynamoDB
dynamodb = boto3.resource('dynamodb',
                          endpoint_url='http://localhost:8000',  # Use actual endpoint URL if not local
                          region_name='dummy',
                          aws_access_key_id='dummy',
                          aws_secret_access_key='dummy'
                          )

# Fetch weather data using the forecast URL
def fetch_weather_data_from_url(forecast_url):
    print(f"Fetching weather data from URL: {forecast_url}")
    response = requests.get(forecast_url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch weather data from {forecast_url}, status code: {response.status_code}")
        return None

# Update weather data in the Properties table
def update_weather_data_in_properties(property_id, weather_data, parsed_weather_data, detailed_forecast):
    properties_table = dynamodb.Table('Properties')

    weather_data = convert_floats_to_decimal(weather_data)

    try:
        properties_table.update_item(
            Key={'property_id': property_id},
            UpdateExpression="SET weather_data = :weather_data, next_period_weather_data = :next_period_weather_data, next_period_forecast = :next_period_forecast",
            ExpressionAttributeValues={
                ':weather_data': weather_data,
                ':next_period_weather_data': parsed_weather_data,
                ':next_period_forecast': detailed_forecast
            }
        )
        print(f"Weather data updated for property_id {property_id}")
    except ClientError as e:
        print(f"Error updating weather data for {property_id}: {e}")

# Parse the weather data
def parse_weather_data(weather_data):
    parsed_weather_data = {
        'temperature': weather_data['properties']['periods'][0]['temperature'],
        'temperature_unit': weather_data['properties']['periods'][0]['temperatureUnit'],
        'short_forecast': weather_data['properties']['periods'][0]['shortForecast']
    }
    detailed_forecast = weather_data['properties']['periods'][0]['detailedForecast']
    return parsed_weather_data, detailed_forecast

# Celery beat task to update weather data
@app.task
def update_weather_from_weatherlink():
    weatherlink_table = dynamodb.Table('WeatherLink')

    try:
        response = weatherlink_table.scan()
        items = response.get('Items', [])

        for item in items:
            property_id = item.get('property_id')
            forecast_url = item.get('forecast_url')

            if not forecast_url:
                continue

            # Fetch the latest weather data using the forecast URL
            weather_data = fetch_weather_data_from_url(forecast_url)
            if weather_data:
                parsed_weather_data, detailed_forecast = parse_weather_data(weather_data)

                # Update the weather data in the Properties table
                update_weather_data_in_properties(property_id, weather_data, parsed_weather_data, detailed_forecast)

    except ClientError as e:
        print(f"Error scanning WeatherLink table: {e}")