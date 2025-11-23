import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    role="ACCOUNTADMIN"
)

cursor = conn.cursor()

# Drop snapshot tables to reset
snapshots = ['snap_products', 'snap_brands', 'snap_categories', 'snap_countries']

for snap in snapshots:
    try:
        cursor.execute(f"DROP TABLE IF EXISTS SNAPSHOTS.{snap} CASCADE;")
        print(f"✅ Dropped SNAPSHOTS.{snap}")
    except Exception as e:
        print(f"❌ Error dropping {snap}: {e}")

conn.close()
print("✅ Snapshots reset complete!")
