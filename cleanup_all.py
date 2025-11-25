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

# Drop all tables in SILVER schema
print("🔍 Dropping SILVER layer tables...")
silver_tables = ['stg_products', 'stg_brands', 'stg_categories', 'stg_countries']
for table in silver_tables:
    try:
        cursor.execute(f"DROP TABLE IF EXISTS FOOD.SILVER.{table} CASCADE;")
        print(f"  ✅ Dropped FOOD.SILVER.{table}")
    except Exception as e:
        print(f"  ❌ Error: {e}")

# Drop all tables in GOLD schema
print("\n🔍 Dropping GOLD layer tables...")
gold_tables = ['dim_product', 'dim_brand', 'dim_category', 'dim_country', 'fact_nutrition']
for table in gold_tables:
    try:
        cursor.execute(f"DROP TABLE IF EXISTS FOOD.GOLD.{table} CASCADE;")
        print(f"  ✅ Dropped FOOD.GOLD.{table}")
    except Exception as e:
        print(f"  ❌ Error: {e}")

# Drop all snapshot tables
print("\n🔍 Dropping Snapshot tables...")
snapshots = ['snap_products', 'snap_brands', 'snap_categories', 'snap_countries']
for snap in snapshots:
    try:
        cursor.execute(f"DROP TABLE IF EXISTS FOOD.SNAPSHOTS.{snap} CASCADE;")
        print(f"  ✅ Dropped FOOD.SNAPSHOTS.{snap}")
    except Exception as e:
        print(f"  ❌ Error: {e}")

conn.close()
print("\n✅ Cleanup complete! All SILVER, GOLD, and SNAPSHOTS tables removed.")
