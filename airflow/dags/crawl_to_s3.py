from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import sys
import os

# Add scripts folder to path
sys.path.insert(0, '/opt/airflow/scripts')

# Import từ scripts
from Crawl_API import run_crawl

def create_s3_bucket(**context):
    BUCKET_NAME = "raw-food-project"
    s3 = S3Hook(aws_conn_id="aws_default")
    
    try:
        s3.create_bucket(bucket_name=BUCKET_NAME, region_name='ap-southeast-1')
        print(f"Created bucket {BUCKET_NAME}")
    except Exception as e:
        # Bucket có thể đã tồn tại hoặc lỗi khác, chỉ log lại
        print(f"Bucket creation info: {str(e)}")
        print(f"Bucket {BUCKET_NAME} exists or creation skipped")

def run_crawler(**context):
    """Crawl data and upload to S3"""
    print("Starting crawler...")
    try:
        run_crawl()
        print("Crawler completed successfully")
        return {"status": "success"}
    except Exception as e:
        print(f"Error running crawler: {str(e)}")
        raise

with DAG(
    dag_id="crawl_to_s3_v27",
    schedule_interval="@daily",
    start_date=datetime(2025, 11, 11),
    catchup=False,
    tags=["crawl", "s3"],
    default_args={"execution_timeout": timedelta(hours=2)}
) as dag:
    
    create_bucket_task = PythonOperator(
        task_id="create_s3_bucket",
        python_callable=create_s3_bucket,
        execution_timeout=timedelta(minutes=5)
    )
    
    crawl_task = PythonOperator(
        task_id="crawl_to_s3",
        python_callable=run_crawler,
        execution_timeout=timedelta(hours=2)
    )
    
    create_bucket_task >> crawl_task