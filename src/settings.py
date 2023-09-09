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
