import json
from datetime import datetime
from glob import glob

import boto3

from settings import (AWS_PROFILE_NAME, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
                      LOG_GROUP, LOG_STREAM_PREFIX, LOG_PATH, LOG_EXTENSION)


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


def get_log_events_from_file(file):
    exclude_fields = ('@timestamp', 'logger',)
    return [
        dict(
            timestamp=int(datetime.fromisoformat(d['@timestamp']).timestamp() * 1000),
            message=json.dumps({k: v for k, v in d.items() if k not in exclude_fields})
        ) for d in [json.loads(linea) for linea in open(file, 'r')]]


def run():
    log_stream_name = init_cloudwatch_stream()

    function_parameters = dict(
        logGroupName=LOG_GROUP,
        logStreamName=log_stream_name
    )

    for f in glob(f'{LOG_PATH}/*.{LOG_EXTENSION}'):
        function_parameters['logEvents'] = get_log_events_from_file(f)
        response = logs.put_log_events(**function_parameters)
        function_parameters['sequenceToken'] = response['nextSequenceToken']


if AWS_PROFILE_NAME:
    session = boto3.Session(profile_name=AWS_PROFILE_NAME, region_name=AWS_REGION)
else:
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION)

logs = session.client('logs')
