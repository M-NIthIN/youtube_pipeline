from stream_to_s3 import stream_kaggle_to_s3
from glue_crawler import create_glue_crawler, run_glue_crawler
from config import S3_BUCKET_NAME, CRAWLER_NAME, DB_NAME

def main():

    # To stream data to S3, call this function
    dataset_name = "datasnaek/youtube-new" 
    stream_kaggle_to_s3(dataset_name)


    # To create the Glue crawler, call this function
    s3_path = f's3://{S3_BUCKET_NAME}/youtube/raw_reference_data'
    table_prefix = 'youtube_'
    create_glue_crawler(CRAWLER_NAME, DB_NAME, s3_path, table_prefix)

    # To run the Glue crawler, call this function
    run_glue_crawler(CRAWLER_NAME)
    pass

if __name__ == "__main__":
    main()
