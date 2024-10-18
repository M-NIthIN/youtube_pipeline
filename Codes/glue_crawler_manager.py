import boto3
from config import IAM_GLUE_ROLE, S3_BUCKET_NAME, CRAWLER_NAME, DB_NAME

glue_client = boto3.client('glue')


def create_glue_crawler(crawler_name, database_name, s3_path, table_prefix):
    """
    Function to create an AWS Glue Crawler.
    
    :param crawler_name: Name of the crawler to be created.
    :param database_name: The Glue Data Catalog database where tables will be created.
    :param s3_path: S3 path that the crawler will scan for data.
    :param table_prefix: Prefix to add to table names created by the crawler.
    :return: None
    """
    try:
        response = glue_client.create_crawler(
            Name=crawler_name,
            Role=IAM_GLUE_ROLE,
            DatabaseName=database_name,
            Targets={
                'S3Targets': [
                    {
                        'Path': s3_path
                    }
                ]
            },
            TablePrefix=table_prefix,
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
            }
        )
        print(f"Crawler '{crawler_name}' created successfully.")
    except Exception as e:
        print(f"Error creating crawler: {str(e)}")


def run_glue_crawler(crawler_name):
    """
    Function to start an AWS Glue Crawler (run the crawler).
    
    :param crawler_name: Name of the crawler to be run.
    :return: None
    """
    try:
        glue_client.start_crawler(Name=crawler_name)
        print(f"Crawler '{crawler_name}' started successfully.")
    except Exception as e:
        print(f"Error starting crawler: {str(e)}")



if __name__ == "__main__":
    s3_path = f's3://{S3_BUCKET_NAME}/youtube/raw_reference_data'
    table_prefix = 'youtube_'
    
    # Create the Glue Crawler
    create_glue_crawler( CRAWLER_NAME, DB_NAME, s3_path, table_prefix)
    
    # Run the Glue Crawler after creation
    run_glue_crawler(CRAWLER_NAME)
