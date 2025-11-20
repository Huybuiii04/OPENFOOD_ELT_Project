import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

BUCKET_NAME = "raw-food-project"
KEY = "checkpoint/checkpoint.json"

def load_checkpoint(aws_conn_id = "aws_default"):
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    
    # Tạo bucket nếu chưa tồn tại
    try:
        s3.create_bucket(bucket_name=BUCKET_NAME, region_name='ap-southeast-1')
    except Exception as e:
        # Bucket đã tồn tại hoặc lỗi khác, không cần handle
        pass
    
    # Nếu checkpoint chưa tồn tại, bắt đầu từ trang 1
    try:
        if not s3.check_for_key(KEY, BUCKET_NAME):
            return 1
        
        data = s3.read_key(KEY, BUCKET_NAME)
        return json.loads(data).get("last_crawled_page", 1)
    except Exception as e:
        # Nếu lỗi HeadObject (403) hoặc lỗi khác, bắt đầu từ trang 1
        print(f"Checkpoint load error (will start from page 1): {str(e)}")
        return 1

def save_checkpoint(last_crawled_page, aws_conn_id="aws_default"):
    payload = json.dumps({"last_crawled_page": last_crawled_page})
    
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    
    try:
        s3.load_string(
            string_data=payload,
            key=KEY,
            bucket_name=BUCKET_NAME,
            replace=True
        )
        print(f"Checkpoint saved: page {last_crawled_page}")
    except Exception as e:
        # Lỗi upload checkpoint (InvalidAccessKeyId, etc) - log nhưng không crash
        print(f"Checkpoint save error (page {last_crawled_page}): {str(e)}")
    except Exception as e:
        # If checkpoint save fails, just log and continue
        print(f"Checkpoint save error (progress not saved): {str(e)}")