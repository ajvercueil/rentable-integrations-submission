import boto3
import time
import uuid
from lxml import etree
from botocore.exceptions import ClientError
from create_tables import create_properties_table, create_weatherlink_table, create_statistics_table
from utilities import parse_address
from background_tasks import queue_weather_job  # Celery task
from decimal import Decimal  # Import Decimal

# Start the overall timer for the parsing run
start_time = time.time()

# Connect to local DynamoDB instance
dynamodb = boto3.resource('dynamodb',
                          endpoint_url='http://localhost:8000',
                          region_name='dummy',
                          aws_access_key_id='dummy',
                          aws_secret_access_key='dummy'
                          )

# Increment atomic counters in DynamoDB
def increment_statistic(run_id, field, increment_by=1):
    """
    Increment a numeric field in the RunStatistics table atomically.

    Args:
        run_id (str): The ID of the run for which we are updating the statistics.
        field (str): The field to increment (e.g., 'successful_api_calls').
        increment_by (int): The amount to increment by.
    """
    statistics_table = dynamodb.Table('RunStatistics')
    try:
        statistics_table.update_item(
            Key={'run_id': run_id},
            UpdateExpression=f"SET {field} = if_not_exists({field}, :start) + :inc",
            ExpressionAttributeValues={
                ':inc': increment_by,
                ':start': 0
            }
        )
    except ClientError as e:
        print(f"Error incrementing {field} in DynamoDB: {e}")

# Add an entry to a list field in DynamoDB
def add_to_list(run_id, field, item):
    """
    Append an item to a list field in the RunStatistics table.
    """
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

# Create DynamoDB tables if necessary
create_statistics_table()
create_properties_table()
create_weatherlink_table()

# Access the tables
property_table = dynamodb.Table('Properties')
run_statistics_table = dynamodb.Table('RunStatistics')


# Parse XML file
print("Parsing XML file 'abodo_feed.xml'...")
tree = etree.parse('abodo_feed.xml')
root = tree.getroot()

properties = []  # List to store extracted property data

properties_elems = root.findall('.//Property')

total_properties = len(properties_elems)

print(f"Found {total_properties} properties in the XML file.")

# Initialize metrics
target_properties_processed = 0
total_target_parsing_time = Decimal('0')
total_background_time = Decimal('0')  # Total time for background tasks
total_background_api_call_time = Decimal('0')  # Total time for individual API calls
background_api_calls_count = 0



# Generate a unique run ID
run_id = str(uuid.uuid4())

# Create initial statistics entry in DynamoDB
run_statistics_table.put_item(Item={
    'run_id': run_id,
    'total_properties_in_xml': total_properties,
    'target_properties_processed': 0,
    'duplicate_targets_skipped': 0,
    'successful_api_calls': 0,
    'total_target_parsing_time': Decimal('0'),
    'average_parse_time_per_target_property': Decimal('0'),
    'average_time_per_property_background': Decimal('0'),
    'average_api_call_time': Decimal('0'),
    'properties_added': 0,
    'property_runtimes': [],
    'background_job_details': [],
    'total_run_time': Decimal('0')  # Add a field to track total run time
})

# Use a set to track unique property IDs
unique_property_ids = set()

total_bedrooms = 0

for property_elem in properties_elems:
    # Track property_id and skip duplicates
    property_id = property_elem.xpath('./PropertyID/Identification/@IDValue')[0]
    if property_id in unique_property_ids:
        print(f"Skipping duplicate property {property_id}")
        # Increment the "duplicate_targets_skipped" counter atomically
        increment_statistic(run_id, 'duplicate_targets_skipped')
        continue
    else:
        unique_property_ids.add(property_id)
    
    # Extract property if in Madison
    city_elem = property_elem.xpath('./PropertyID/Address/City')

    # Calculate number of bedrooms
    bedrooms = sum([int(float(unit.find('./UnitBedrooms').text)) 
                    for ils_unit in property_elem.findall('.//ILS_Unit') 
                    for units in ils_unit.findall('./Units') 
                    for unit in units.findall('./Unit')])
    
    total_bedrooms += bedrooms

    if city_elem and city_elem[0].text == 'Madison':
        # Increment total properties processed counter
        target_properties_processed += 1

        # Track time for each property in the parser
        start_parse_time = time.time()

        print("Processing property in Madison...")
        property_id = property_elem.xpath('./PropertyID/Identification/@IDValue')
        marketing_name = property_elem.xpath('./PropertyID/MarketingName')
        email = property_elem.xpath('./PropertyID/Email')
        unparsed_address = property_elem.xpath('./PropertyID/Address/UnparsedAddress')[0].text
        print(f"Unparsed address: {unparsed_address}")
        parsed_address = parse_address(unparsed_address)
        print(f"Parsed address: {parsed_address}")
        
        property_info = {
            'property_id': property_id[0] if property_id else None,
            'name': marketing_name[0].text if marketing_name else None,
            'email': email[0].text if email else None,
            'bedrooms': bedrooms
        }

        properties.append(property_info)  # Add the dictionary to the properties list

        print(f"Extracted property info: {property_info}")

        # Insert into DynamoDB
        property_table.put_item(Item={
            'property_id': property_info['property_id'],
            'name': property_info['name'],
            'email': property_info['email'],
            'bedrooms': str(property_info['bedrooms'])
        })

        print(f"Property {property_info['property_id']} added to DynamoDB.")

        # Track parsing time for this property and accumulate total time
        end_parse_time = time.time()
        time_taken = Decimal(str(end_parse_time - start_parse_time))
        total_target_parsing_time += time_taken
        
        # Add property and runtime to the list in DynamoDB
        add_to_list(run_id, 'property_runtimes', {'property_id': property_info['property_id'], 'parser_runtime': str(time_taken)})

        # Track background job time and get API result
        print(f"Queueing background job for property {property_info['property_id']}...")
        queue_weather_job.delay(property_info['property_id'], parsed_address, run_id)

        # Increment the properties added counter atomically
        increment_statistic(run_id, 'properties_added')

# Calculate the average time per property in the parser
average_parse_time_per_target_property = total_target_parsing_time / target_properties_processed if target_properties_processed > 0 else Decimal('0')

# Calculate the total run time
total_run_time = Decimal(str(time.time() - start_time))

# Update total and average time in DynamoDB
run_statistics_table.update_item(
    Key={'run_id': run_id},
    UpdateExpression="SET total_target_parsing_time = :total_time, average_parse_time_per_target_property = :avg_time, total_run_time = :run_time, target_properties_processed = :target_count",
    ExpressionAttributeValues={
        ':total_time': total_target_parsing_time,
        ':avg_time': average_parse_time_per_target_property,
        ':run_time': total_run_time,
        ':target_count': target_properties_processed
    }
)

print("Data successfully saved to DynamoDB")

print(f'Unique properties: {len(unique_property_ids)}')
print(f'Total bedrooms (excluding duplicate properties): {total_bedrooms}')

# Print final statistics from DynamoDB
response = run_statistics_table.get_item(Key={'run_id': run_id})
print("Final run statistics:")
print(response['Item'])

# Output the properties
print("Target properties extracted from XML (excluding duplicates):")
for prop in properties:
    print(prop)

# Scan DynamoDB table and print all items
print("Scanning DynamoDB table 'Properties' for all items...")
scanResponse = property_table.scan()
responseItems = scanResponse['Items']
for item in responseItems:
    print(item)
