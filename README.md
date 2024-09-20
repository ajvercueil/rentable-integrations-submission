# Introduction
- This program parses an XML file, gathers and stores property data (including property_id, name, email, and total number of bedrooms in the property) for properties in Madison (excluding duplicates) in a DynamoDB database in a Docker container. In the background, weather data for these properties are gathered, and the Properties database table is updated. In addition, run statistics are generated and dumped into a separate database.
- The background processes use Redis as a queue, and Celery for distributed asynchronous workers. The Celery tasks can be easily observed and monitored using a Celery Flower dashboard.
- In addition, a scheduled Celery Beat job scheduler can be run simultaneously to update weather data for properties in the Properties table (already processed by the xml_parser) at regular increments of time, or at specific scheduled dates/times. These tasks can also be monitored via Celery Flower.

# Installing Dependencies
## Download and install conda and Python3 via Miniconda
https://docs.anaconda.com/miniconda/#quick-command-line-install
- Run the CLI commands provided, replacing the link in the curl command with the link to the file for your device / operating system
- Link to Installers: https://repo.anaconda.com/miniconda/ (right click and "Copy link address" for your specific version)
- After installing, initialize  Miniconda. The following command initializes for zsh shells:
- ```~/miniconda3/bin/conda init zsh```

## Download and Install Docker Desktop
https://www.docker.com/products/docker-desktop/
- Used to containerize and run DynamoDB locally

## Open the Docker Desktop app

## Install redis, celery, boto3, flower, requests, lxml, botocore, docker, awscli libraries via the CLI
```pip install redis celery boto3 flower requests lxml botocore docker awscli```
- lxml: Powerful Pythonic XML processing library (https://lxml.de/tutorial.html)
- Docker: Docker SDK (https://docker-py.readthedocs.io/en/stable/)
- Botocore: Foundation of Boto3 and AWS CLI
- Boto3: AWS SDK for Python (used to interact with local DynamoDB) (https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- AWS CLI: Used to interface with local AWS resources from the command line (https://docs.aws.amazon.com/cli/)
- Redis: In-memory cache used as async task queue and results store for Celery
- Celery: For executing async tasks (Distributed task queue workers. Also supports task scheduling) (https://docs.celeryq.dev/en/stable/getting-started/introduction.html)
- Flower: Used to monitor Celery async tasks via dashboard
- Requests: Allows us to send HTTP/1.1 API requests extremely easily

## Pull DynamoDB image from Docker Hub
```docker pull amazon/dynamodb-local```

## (Optional) Install NoSQL Workbench (Local DynamoDB GUI)
Allows you to easily view, delete, query, scan, and clone the tables, and allows you to easily add GSIs to more easily query the data. \
Download & Install: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/workbench.settingup.html
- Once NoSQL Workbench is installed, select "Operation builder" on the left toolbar
- Once inside the "Operation builder" portal, select "Add connection" at the top of the window
- Select "DynamoDB Local" under "Add a new database connection"
- Name your connection anything you'd like, such as "Interview"
- Keep Hostname as 'localhost'
- Set Port to "8000", or whichever port you set your Local DynamoDB Docker container to run on
- Upon opening the connection, you should see a "Tables" Section on the left side of the operation builder. If needed, click the refresh button to see the tables (which will only display after the program has been run at least once).
- Now you're good to go!

# Starting the Program 
(Open a new terminal window for each command)
## 1. Start DynamoDB local in Docker container (Only needs to be done once - if stopped, delete the docker instance via the CLI or Docker Desktop GUI before restarting)
```docker run  --name dynamodb -p 8000:8000 amazon/dynamodb-local -jar DynamoDBLocal.jar -sharedDb -dbPath .```
## 2. Start Redis Server for Celery
```redis-server```
- This should start correctly and give a multi-line output ending in "Ready to accept connections tcp", but if not:
- To test your connection, open another terminal window and use the command ```redis-cli ping``` (Should receive response 'PONG')
- https://redis.io/docs/latest/operate/oss_and_stack/install/install-redis/
- If on Mac or Linux, and redis is not working, install Homebrew (https://brew.sh/ if not already installed), and use the command ```brew install redis```
- More info regarding Homebrew installation, if needed: https://mac.install.guide/homebrew/3

# Running the Main Parser 
(Open a new terminal window for each command)
## 1. Start Celery Worker (Async Workers)
```celery -A background_tasks worker --loglevel=INFO```
## 2. Start Flower Dashboard for Celery
```celery -A background_tasks flower```
- Open the Celery tasks dashboard at http://0.0.0.0:5555/tasks
## 3. Run the parser
```python3 xml_parser.py```

# Running the Scheduled Weather Updater 
(Only run after the Main Parser has been run at least once, and the DynamoDB tables exist in the Docker container, while the Redis server is running)
(Open a new terminal window for each command)
## 1. Start Celery Worker (Async Workers)
```celery -A scheduled_weather_updater worker --loglevel=INFO```
## 2. Start Celery Beat (Which sends the tasks to Celery on the schedule defined)
```celery -A scheduled_weather_updater beat --loglevel=info```
## 3. Start Flower Dashboard for Celery on a separate port
```celery -A scheduled_weather_updater flower --port=7777```
- Open the Celery tasks dashboard at http://0.0.0.0:7777/tasks

(When everything is running, you should have 8 terminal windows - 7 for background tasks / monitoring / servers / docker, and 1 for executing the main xml_parser, or any other commands)

# Interacting with DynamoDB
## List Tables in Local DynamoDB
```aws dynamodb list-tables --endpoint-url http://localhost:8000```
## Scan Tables in DynamoDB:
- Scan Properties table: ```aws dynamodb scan --table-name Properties --endpoint-url http://localhost:8000```
- Scan RunStatistics table: ```aws dynamodb scan --table-name RunStatistics --endpoint-url http://localhost:8000```
- Scan WeatherLink table: ```aws dynamodb scan --table-name WeatherLink --endpoint-url http://localhost:8000```
## Delete DynamoDB Tables with CLI
- Delete Properties table: ```aws dynamodb delete-table --table-name Properties --endpoint-url http://localhost:8000```
- Delete RunStatistics table: ```aws dynamodb delete-table --table-name RunStatistics --endpoint-url http://localhost:8000```
- Delete WeatherLink table: ```aws dynamodb delete-table --table-name WeatherLink --endpoint-url http://localhost:8000```

# DynamoDB Data Types & Naming Rules
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html

# Stopping and Deleting Docker Containers via the CLI
docker stop <Container_ID>
docker rm <Container_ID>

# Python3 Installation (if for some reason the miniconda installation didn't work)
If Python doesn't install with miniconda, download Python3 via https://www.python.org/downloads/ \
(Or, if you have Homebrew installed on Mac/Linux, use ```brew install python3```)