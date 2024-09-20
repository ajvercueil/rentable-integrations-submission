import boto3
from botocore.exceptions import ClientError

# Connect to DynamoDB
dynamodb = boto3.resource('dynamodb',
                          endpoint_url='http://localhost:8000',
                          region_name='dummy',
                          aws_access_key_id='dummy',
                          aws_secret_access_key='dummy'
                          )

# Create DynamoDB WeatherLink table from scratch
def create_weatherlink_table():
    try:
        print("Creating DynamoDB table 'WeatherLink'...")
        dynamodb.create_table(
            TableName='WeatherLink',
            KeySchema=[{'AttributeName': 'property_id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'property_id', 'AttributeType': 'S'}],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        print("DynamoDB WeatherLink table created successfully.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print("DynamoDB WeatherLink table already exists.")
        else:
            print(f"Unexpected error: {e}")

# Create DynamoDB WeatherLink table from scratch
def create_statistics_table():
    try:
        print("Creating DynamoDB table 'RunStatistics'...")
        dynamodb.create_table(
            TableName='RunStatistics',
            KeySchema=[{'AttributeName': 'run_id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'run_id', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        print("DynamoDB WeatherLink table created successfully.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print("DynamoDB RunStatistics table already exists.")
        else:
            print(f"Unexpected error: {e}")


# Function to create DynamoDB table if it doesn't exist
def create_properties_table():
    try:
        dynamodb.create_table(
            TableName='Properties',
            KeySchema=[
                {'AttributeName': 'property_id', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'property_id', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        print("DynamoDB Properties table created successfully.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print("DynamoDB Properties table already exists.")
        else:
            print(f"Unexpected error: {e}")


# To delete and recreate the RunStatistics table on each run, uncomment the following code and delete the 'create_statistics_table' code above
'''
# Delete and recreate the RunStatistics table
def reset_statistics_table():
    try:
        print("Deleting DynamoDB table 'RunStatistics' if it exists...")
        dynamodb.Table('RunStatistics').delete()
        table_waiter = dynamodb.meta.client.get_waiter('table_not_exists')
        table_waiter.wait(TableName='RunStatistics')
        print("DynamoDB table 'RunStatistics' deleted.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("DynamoDB table 'RunStatistics' does not exist.")
        else:
            print(f"Error deleting DynamoDB table: {e.response['Error']['Message']}")
    create_statistics_table()

# Create DynamoDB RunStatistics table from scratch
def create_statistics_table():
    try:
        print("Creating DynamoDB table 'RunStatistics'...")
        dynamodb.create_table(
            TableName='RunStatistics',
            KeySchema=[{'AttributeName': 'run_id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'run_id', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        table_waiter = dynamodb.meta.client.get_waiter('table_exists')
        table_waiter.wait(TableName='RunStatistics')
        print("DynamoDB table 'RunStatistics' created successfully.")
    except ClientError as e:
        print(f"Error creating DynamoDB table: {e.response['Error']['Message']}")
'''