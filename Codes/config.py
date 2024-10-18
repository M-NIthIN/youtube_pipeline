import os
from dotenv import load_dotenv

load_dotenv()

# AWS
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
IAM_GLUE_ROLE = os.getenv('IAM_GLUE_ROLE')

# kaggle
KAGGLE_USERNAME = os.getenv('KAGGLE_USERNAME')
KAGGLE_KEY = os.getenv('KAGGLE_KEY')

# Glue
CRAWLER_NAME = os.getenv('CRAWLER_NAME')
DB_NAME = os.getenv('DB_NAME')