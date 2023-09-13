# Sending logs to AWS CloudWatch with a sidecar pattern and Python

In a Docker Swarm environment, the sidecar pattern is a common architectural approach used to extend the functionality of a primary container without directly modifying it.

First there's a primary container. This is the main application or service you want to run within a Docker Swarm 
service. It's responsible for the core functionality of your application. In our case, this container will be the 
log generator. It will save logs whitin a json format.

The sidecar container is a secondary container that runs alongside the primary container. It's tightly coupled with 
the primary container and assists it by providing additional services or functionality. In our example, the sidecar 
resposability will be push logs to AWS CloudWatch. The idea is sharing a docker volume between both containers. Whit 
this technique, our primary container will not be affected in network latency generating logs.

The idea is generate something like this:

```yaml
version: '3.6'

services:
  api:
    image: api:latest
    logging:
      options:
        max-size: 10m
    deploy:
      restart_policy:
        condition: any
    volumes:
      - logs_volume:/src/logs
    environment:
      - ENVIRONMENT=production
      - PROCESS_ID=api
    ports:
      - 5000:5000
    command: gunicorn -w 1 app:app -b 0.0.0.0:5000 --timeout 180

  filebeat:
    image: cw:production
    command: bash -c "python cw.py && sleep 1m"
    deploy:
      restart_policy:
        condition: any
    environment:
      - LOG_GROUP=python_logs_example
      - LOG_STREAM_PREFIX=default
    volumes:
      - logs_volume:/src/logs
volumes:
  logs_volume:
```
Let's go. First we need to setup our aws credentials. We can use a profile or a IAM user

```python
if AWS_PROFILE_NAME:
    session = boto3.Session(profile_name=AWS_PROFILE_NAME, region_name=AWS_REGION)
else:
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION)

logs = session.client('logs')
```

then we need to setup the logGroup and logStream in CloudWatch. We can generate them by hand, but I preffer to 
generate them programatically, if they don't exist.


```python
def init_cloudwatch_stream():
    log_stream_name = f"{LOG_STREAM_PREFIX}_{datetime.now().strftime('%Y%m%d')}"

    log_groups = logs.describe_log_groups(logGroupNamePrefix=LOG_GROUP)['logGroups']
    if not any(group['logGroupName'] == LOG_GROUP for group in log_groups):
        logs.create_log_group(logGroupName=LOG_GROUP)

    log_streams = logs.describe_log_streams(
        logGroupName=LOG_GROUP,
        logStreamNamePrefix=log_stream_name
    )['logStreams']

    if not any(stream['logStreamName'] == log_stream_name for stream in log_streams):
        logs.create_log_stream(logGroupName=LOG_GROUP, logStreamName=log_stream_name)

    return log_stream_name
```

Now we need to upload logs to CloudWatch. We need to use put_log_events. To send multiple logs we need to use a 
sequenceToken (not the first time). To do that I use this trick.
```python
function_parameters = dict(
    logGroupName=LOG_GROUP,
    logStreamName=log_stream_name
)

for f in glob(f'{LOG_PATH}/*.{LOG_EXTENSION}'):
    function_parameters['logEvents'] = get_log_events_from_file(f)
    response = logs.put_log_events(**function_parameters)
    function_parameters['sequenceToken'] = response['nextSequenceToken']
```

We also need to read log files and maybe change the fields according to your needs

```python
def get_log_events_from_file(file):
    exclude_fields = ('@timestamp', 'logger')
    return [
        dict(
            timestamp=int(datetime.fromisoformat(d['@timestamp']).timestamp() * 1000),
            message=json.dumps({k: v for k, v in d.items() if k not in exclude_fields})
        ) for d in [json.loads(linea) for linea in open(file, 'r')]]
```

I like to have all the settings of my application in a file called settings.py. It's a pattern that I've copied from 
Django. In this file also I read environment variables from a dotenv file.

```python
import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
ENVIRONMENT = os.getenv('ENVIRONMENT', 'local')

load_dotenv(dotenv_path=Path(BASE_DIR).resolve().joinpath('env', ENVIRONMENT, '.env'))

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

AWS_PROFILE_NAME = os.getenv('AWS_PROFILE_NAME', False)
AWS_REGION = os.getenv('AWS_REGION')

LOG_GROUP = os.getenv('LOG_GROUP', 'python_logs_example')
LOG_STREAM_PREFIX = os.getenv('LOG_STREAM_PREFIX', 'default')

LOG_EXTENSION = 'log'
LOG_PATH = os.getenv('LOG_PATH', Path(BASE_DIR).resolve())
```

And that's all. Your logs in CloudWatch uploaded in a background process decoupled from the main process.
