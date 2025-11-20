import os
import io
import csv
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta

# S3 Config
S3_BUCKET_NAME = "raw-food-project"
S3_PREFIX = "bronze/"

# Snowflake Config - read from environment variables
SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT") or "si00918.ap-southeast-1"
SNOWFLAKE_USER = os.environ.get("SNOWFLAKE_USER") or "huybui04"
SNOWFLAKE_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD") or "4DN2ehSHayu_sww"
SNOWFLAKE_DATABASE = os.environ.get("SNOWFLAKE_DATABASE") or "FOOD"
SNOWFLAKE_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA") or "RAW"
SNOWFLAKE_TABLE = os.environ.get("SNOWFLAKE_TABLE") or "PRODUCTS"
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE") or "COMPUTE_WH"

print(f"Snowflake Account: {SNOWFLAKE_ACCOUNT}")
print(f"Snowflake User: {SNOWFLAKE_USER}")
print(f"Snowflake Database: {SNOWFLAKE_DATABASE}")


def list_s3_files(**context):
    """List all CSV files in S3 bucket with given prefix"""
    s3 = S3Hook(aws_conn_id="aws_default")
    
    # List all keys with prefix
    keys = s3.list_keys(bucket_name=S3_BUCKET_NAME, prefix=S3_PREFIX)
    csv_files = [key for key in keys if key.endswith('.csv')]
    
    print(f"Found {len(csv_files)} CSV files in S3")
    
    # Push to XCom for downstream tasks
    context['task_instance'].xcom_push(key='csv_files', value=csv_files)
    
    return csv_files


def connect_snowflake():
    """Create Snowflake connection"""
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    return conn


def create_snowflake_table():
    """Create Snowflake table if not exists"""
    conn = connect_snowflake()
    cursor = conn.cursor()
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
        id STRING,
        code STRING,
        product_name STRING,
        brands STRING,
        countries STRING,
        categories STRING,
        ingredients_text STRING,
        nutriscore_grade STRING,
        energy_100g FLOAT,
        sugars_100g FLOAT
    )
    """
    
    try:
        cursor.execute(create_table_sql)
        print(f"Table {SNOWFLAKE_TABLE} created or already exists")
    except Exception as e:
        print(f"Error creating table: {str(e)}")
    finally:
        cursor.close()
        conn.close()


def load_s3_to_snowflake(**context):
    """Download CSV from S3 and load into Snowflake"""
    # Get CSV files from XCom
    task_instance = context['task_instance']
    csv_files = task_instance.xcom_pull(task_ids='list_s3_files', key='csv_files')
    
    if not csv_files:
        print("No CSV files found to load")
        return
    
    s3 = S3Hook(aws_conn_id="aws_default")
    conn = connect_snowflake()
    cursor = conn.cursor()
    
    total_rows = 0
    
    for csv_file in csv_files:
        try:
            # Download CSV from S3
            print(f"Downloading {csv_file} from S3...")
            csv_content = s3.read_key(key=csv_file, bucket_name=S3_BUCKET_NAME)
            
            # Parse CSV content
            csv_buffer = io.StringIO(csv_content)
            reader = csv.DictReader(csv_buffer)
            rows = list(reader)
            
            if not rows:
                print(f"File {csv_file} is empty, skipping")
                continue
            
            # Prepare data for Snowflake insert
            insert_sql = f"""
            INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
            (id, code, product_name, brands, countries, categories, ingredients_text, nutriscore_grade, energy_100g, sugars_100g)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # Insert batch
            batch_data = []
            for row in rows:
                batch_data.append((
                    row.get('id', ''),
                    row.get('code', ''),
                    row.get('product_name', ''),
                    row.get('brands', ''),
                    row.get('countries', ''),
                    row.get('categories', ''),
                    row.get('ingredients_text', ''),
                    row.get('nutriscore_grade', ''),
                    float(row.get('energy_100g', 0)) if row.get('energy_100g') else None,
                    float(row.get('sugars_100g', 0)) if row.get('sugars_100g') else None
                ))
            
            cursor.executemany(insert_sql, batch_data)
            total_rows += len(rows)
            print(f"Loaded {len(rows)} rows from {csv_file}")
            
        except Exception as e:
            print(f"Error loading {csv_file}: {str(e)}")
            continue
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Total rows loaded: {total_rows}")


# DAG Definition
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 11, 18),
}

with DAG(
    dag_id='s3_to_snowflake_v4',
    default_args=default_args,
    description='Load CSV data from S3 to Snowflake',
    schedule_interval='@daily',
    catchup=False,
    tags=['s3', 'snowflake', 'etl']
) as dag:
    
    # Task 1: List S3 files
    task_list_files = PythonOperator(
        task_id='list_s3_files',
        python_callable=list_s3_files,
        provide_context=True
    )
    
    # Task 2: Create Snowflake table
    task_create_table = PythonOperator(
        task_id='create_snowflake_table',
        python_callable=create_snowflake_table
    )
    
    # Task 3: Load data from S3 to Snowflake
    task_load_data = PythonOperator(
        task_id='load_s3_to_snowflake',
        python_callable=load_s3_to_snowflake,
        provide_context=True
    )
    
    # Task dependencies
    task_list_files >> task_create_table >> task_load_data

