from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def upload_to_s3(BUCKET_NAME, KEY, string_data, aws_conn_id="aws_default"):
    
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    
    s3.load_string(
        string_data = string_data,
        key = KEY,
        bucket_name = BUCKET_NAME,
        replace = True
    )
    
    