import os
import requests
import zipfile
import io
import boto3
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET_NAME, KAGGLE_USERNAME, KAGGLE_KEY

# S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def stream_kaggle_to_s3(dataset_name):
    # Kaggle API endpoint
    kaggle_url = f"https://www.kaggle.com/api/v1/datasets/download/{dataset_name}"
    auth = (KAGGLE_USERNAME, KAGGLE_KEY)

    try:
        # Stream the dataset from Kaggle
        with requests.get(kaggle_url, auth=auth, stream=True) as response:
            response.raise_for_status()  # Check if the request was successful

            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                for file_info in z.infolist(): # Iterate over the files in the zip
                    if file_info.filename.endswith('/'):
                        continue # Skip directories

                    file_name = os.path.basename(file_info.filename)
                    file_data = z.read(file_info.filename)

                    # Upload to S3
                    if file_name.endswith('.json'):
                        s3_key  = f"youtube/raw_reference_data/{file_name}"
                    elif file_name.endswith('.csv'):
                        region_Code = file_name[:2].lower()
                        s3_key = f"youtube/raw/region={region_Code}/{file_name}"
                    else:
                        continue
                    
                    s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=file_data)
                    print(f"Uploaded {file_name} to s3://{S3_BUCKET_NAME}/{s3_key}")

    except Exception as e:
        print(f"Error streaming and unzipping dataset {dataset_name} to S3: {str(e)}")

if __name__ == "__main__":
    dataset_name = "datasnaek/youtube-new" 
    stream_kaggle_to_s3(dataset_name)

