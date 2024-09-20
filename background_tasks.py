import requests
import boto3
from celery import Celery  # Import Celery app
from utilities import convert_floats_to_decimal
import time
from decimal import Decimal
from botocore.exceptions import ClientError

# Connect to DynamoDB
dynamodb = boto3.resource('dynamodb',
                          endpoint_url='http://localhost:8000',
                          region_name='dummy',
                          aws_access_key_id='dummy',
                          aws_secret_access_key='dummy'
                          )

# Celery configuration
app = Celery(
    'tasks',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0'
)

# Increment atomic counter for API successes
def increment_api_success(run_id):
    statistics_table = dynamodb.Table('RunStatistics')
    try:
        statistics_table.update_item(
            Key={'run_id': run_id},
            UpdateExpression="SET successful_api_calls = if_not_exists(successful_api_calls, :start) + :inc",
            ExpressionAttributeValues={
                ':inc': 1,
                ':start': 0
            }
        )
    except ClientError as e:
        print(f"Error incrementing API success counter: {e}")

# Add an entry to a list field in DynamoDB for background job details
def add_to_list(run_id, field, item):
    statistics_table = dynamodb.Table('RunStatistics')
    try:
        statistics_table.update_item(
            Key={'run_id': run_id},
            UpdateExpression=f"SET {field} = list_append(if_not_exists({field}, :empty_list), :item)",
            ExpressionAttributeValues={
                ':item': [item],
                ':empty_list': []
            }
        )
    except ClientError as e:
        print(f"Error appending to {field} in DynamoDB: {e}")

# Fetch latitude and longitude for unparsed address from OpenStreetMap API
def fetch_lat_lon(parsed_address):
    url = f'https://nominatim.openstreetmap.org/search?q={parsed_address}&format=jsonv2&limit=1'
    print(f"Fetching coordinates for: {url}")  # Test address parsing by logging the URL

    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; MyParser/1.0; +https://example.com/my-parser)'  # Adds a User-Agent header to avoid detection (Bypasses OSM 403 error)
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Will raise an exception if the response status code is not 200
        data = response.json()
        if data:
            lat = data[0].get('lat')
            lon = data[0].get('lon')
            if lat and lon:
                print(f"Fetched coordinates: Latitude = {lat}, Longitude = {lon}")
                return lat, lon
            else:
                print(f"Coordinates not found in the response: {data}")
        else:
            print(f"Empty JSON response for address: {parsed_address}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed for address: {parsed_address} with error: {e}")
        time.sleep(1)  # Add a delay to avoid triggering rate limits
    return None, None

# Fetch weather data using latitude and longitude
def fetch_weather_data(lat, lon):
    weather_url = f'https://api.weather.gov/points/{lat},{lon}'
    print(f"Fetching weather for coordinates: Latitude = {lat}, Longitude = {lon}")
    print(f"Getting gridpoints from URL: {weather_url}")
    
    response = requests.get(weather_url)
    if response.status_code == 200:
        properties = response.json().get('properties')
        if properties and 'forecast' in properties:
            forecast_url = properties['forecast']
            print(f"Forecast URL being used: {forecast_url}")
            forecast_response = requests.get(forecast_url)
            if forecast_response.status_code == 200:
                return forecast_response.json(), forecast_url
    return None

def parse_weather_data(weather_data):
    parsed_weather_data = {
        'temperature': weather_data['properties']['periods'][0]['temperature'],
        'temperature_unit': weather_data['properties']['periods'][0]['temperatureUnit'],
        'short_forecast': weather_data['properties']['periods'][0]['shortForecast']
    }
    detailed_forecast = weather_data['properties']['periods'][0]['detailedForecast']
    print(f"Parsed weather data: {parsed_weather_data}")
    print(f"Detailed forecast: {detailed_forecast}")

    return parsed_weather_data, detailed_forecast

# Save fetched weather data to DynamoDB
def save_weather_data_to_dynamodb(property_id, weather_data, parsed_weather_data, detailed_forecast):
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

# Save forecast URL to WeatherLink table in DynamoDB
def save_forecast_url_to_dynamodb(property_id, forecast_url):
    weatherlink_table = dynamodb.Table('WeatherLink')
    try:
        weatherlink_table.put_item(
            Item={
                'property_id': property_id,
                'forecast_url': forecast_url
            }
        )
        print(f"Saved forecast URL for property_id {property_id}: {forecast_url}")
    except ClientError as e:
        print(f"Error saving forecast URL for {property_id}: {e}")

# Queue background task for fetching and saving weather data
@app.task # Celery task decorator
def queue_weather_job(property_id, parsed_address, run_id):
    task_start_time = time.time()

    # Geocoding API call
    geo_start_time = time.time()
    lat, lon = fetch_lat_lon(parsed_address)
    geo_end_time = time.time()
    geocoding_api_time = geo_end_time - geo_start_time

    if lat and lon:
        # Weather API call
        weather_start_time = time.time()
        weather_data, forecast_url = fetch_weather_data(lat, lon)
        weather_end_time = time.time()
        weather_api_time = weather_end_time - weather_start_time

        if weather_data and forecast_url:
            api_sum_time = geocoding_api_time + weather_api_time
            parsed_weather_data, detailed_forecast = parse_weather_data(weather_data)

            # Save weather data to DynamoDB
            save_weather_data_to_dynamodb(property_id, weather_data, parsed_weather_data, detailed_forecast)
            
            # Save forecast URL to WeatherLink table in DynamoDB
            save_forecast_url_to_dynamodb(property_id, forecast_url)

            # Increment API success counter in DynamoDB
            increment_api_success(run_id)

            # Add background job details to DynamoDB
            add_to_list(run_id, 'background_job_details', {
                'property_id': property_id,
                'geocoding_api_time': str(geocoding_api_time),
                'weather_api_time': str(weather_api_time),
                'api_sum_time': str(api_sum_time)
            })

            # Atomic update of total background time, API times, and counts
            statistics_table = dynamodb.Table('RunStatistics')
            statistics_table.update_item(
                Key={'run_id': run_id},
                UpdateExpression="SET total_background_time = if_not_exists(total_background_time, :start) + :background_time, \
                                  total_api_sum_time = if_not_exists(total_api_sum_time, :start) + :api_sum_time, \
                                  total_geocoding_api_time = if_not_exists(total_geocoding_api_time, :start) + :geo_time, \
                                  total_weather_api_time = if_not_exists(total_weather_api_time, :start) + :weather_time, \
                                  background_api_calls_count = if_not_exists(background_api_calls_count, :start) + :inc",
                ExpressionAttributeValues={
                    ':background_time': Decimal(str(time.time() - task_start_time)),
                    ':api_sum_time': Decimal(str(api_sum_time)),
                    ':geo_time': Decimal(str(geocoding_api_time)),
                    ':weather_time': Decimal(str(weather_api_time)),
                    ':start': Decimal('0'),
                    ':inc': 1
                },
                ReturnValues="UPDATED_NEW"  # Return updated values to use for averages
            )

            # Fetch updated totals after atomic increment
            response = statistics_table.get_item(Key={'run_id': run_id})
            if 'Item' in response:
                total_background_time = Decimal(response['Item'].get('total_background_time', Decimal('0')))
                background_api_calls_count = response['Item'].get('background_api_calls_count', 0)
            else:
                total_background_time = Decimal('0')
                background_api_calls_count = 0

            # Calculate averages
            average_background_time = total_background_time / background_api_calls_count if background_api_calls_count > 0 else Decimal('0')
            average_api_call_time = total_background_time / background_api_calls_count if background_api_calls_count > 0 else Decimal('0')

            # Update the averages in DynamoDB
            statistics_table.update_item(
                Key={'run_id': run_id},
                UpdateExpression="SET average_time_per_property_background = :avg_background_time, \
                                  average_api_call_time = :avg_api_call_time",
                ExpressionAttributeValues={
                    ':avg_background_time': average_background_time,
                    ':avg_api_call_time': average_api_call_time
                }
            )
