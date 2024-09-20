import requests
import boto3
from botocore.exceptions import ClientError

# Connect to DynamoDB
dynamodb = boto3.resource('dynamodb',
                          endpoint_url='http://localhost:8000',
                          region_name='dummy',
                          aws_access_key_id='dummy',
                          aws_secret_access_key='dummy'
                          )

def create_weather_table():
    try:
        dynamodb.create_table(
            TableName='Weather',
            KeySchema=[
                {'AttributeName': 'period_number', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'period_number', 'AttributeType': 'N'}
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        print("DynamoDB Weather table created successfully.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print("DynamoDB Weather table already exists.")
        else:
            print(f"Unexpected error: {e}")

create_weather_table()

weather_table = dynamodb.Table('Weather')

# example_coord_to_grid_forecast_link = 'https://api.weather.gov/points/43.0727274,-89.3879292'

# example_actual_weather_forecast_link = 'https://api.weather.gov/gridpoints/MKX/38,64/forecast'

address_to_coord_link = 'https://nominatim.openstreetmap.org/search?q=316+West+Washington+Ave+Madison+WI+53703&format=jsonv2&limit=1' # parse

headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; MyParser/1.0; +https://example.com/my-parser)'  # Adds a User-Agent header to avoid detection
        }

address_to_coord_response = requests.get(address_to_coord_link, headers=headers)
if address_to_coord_response.status_code == 200:
    address_to_coord_json = address_to_coord_response.json()
    print("Successfully retrieved json")
    lat, long = address_to_coord_json[0]['lat'], address_to_coord_json[0]['lon']
    print(f'Latitude: {lat}, Longitude: {long}')
    
if lat and long:
    coord_to_grid_link = f'https://api.weather.gov/points/{lat},{long}' # parse
    print(coord_to_grid_link)
    coord_to_grid_response = requests.get(coord_to_grid_link, headers=headers)
    if coord_to_grid_response.status_code == 200:
        print("Successfully retrieved coord to grid json")
        forecast_link = coord_to_grid_response.json()['properties']['forecast']
        print(forecast_link)

if forecast_link:
    forecast_response = requests.get(forecast_link, headers=headers)
    if forecast_response.status_code == 200:
        print("Successfully retrieved forecast json")
        forecast_json = forecast_response.json()

for period in forecast_json['properties']['periods']:
    period_number = period['number']
    print(period_number)
    precip_val = period['probabilityOfPrecipitation']['value']
    if not precip_val:
        precip_val = 0
        
    weather_table.put_item(
        Item = {
            'period_number': period_number,
            'period_name': period['name'],
            'period_start': period['startTime'],
            'period_end': period['endTime'],
            'is_daytime': period['isDaytime'],
            'temperature': f"{period['temperature']}{period['temperatureUnit']}",
            'precipitation_probability': f"{precip_val}%",
            'wind': f"{period['windSpeed']} {period['windDirection']}",
        }
    )

print("Successfully added items to DynamoDB Weather table")