# OpenFood Data Warehouse Project

## 📋 Project Overview

This project builds a **complete Data Warehouse** for OpenFoodFacts data using modern **Medallion Architecture** (RAW → SILVER → GOLD). The project includes a full ETL pipeline, from API crawling to analytics-ready data.

### 🎯 What Has Been Implemented

#### 1. **Data Ingestion & ETL Pipeline**
- **Asynchronous Crawler**: Uses aiohttp to crawl OpenFoodFacts API with 20 concurrent connections
- **Checkpoint Tracking**: Monitors crawl progress, resumable on interruptions
- **Rate Limiting**: Avoids API blocking with 0.5s delay per request
- **Error Handling**: Retry logic and detailed logging

#### 2. **Data Storage & Processing**
- **AWS S3**: Stores raw data as CSV files (10k rows per file)
- **Snowflake**: Main data warehouse with 3 layers
- **dbt**: Transforms data with 500k+ rows, SCD Type 2 for dimensions

#### 3. **Data Quality & Governance**
- **Data Cleaning**: Removes accents, normalizes text, handles NULLs
- **Dimension Explosion**: Splits comma-separated values into individual rows
- **SCD Type 2**: Historical tracking for products, brands, categories, countries
- **Data Testing**: dbt tests for unique keys and referential integrity

#### 4. **Orchestration & Monitoring**
- **Airflow DAGs**: 2 daily scheduled DAGs for crawling and loading
- **Docker Compose**: Containerized environment
- **Logging & Monitoring**: Detailed logs for troubleshooting

## 🛠️ Tools & Technologies

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Orchestration** | Apache Airflow | Schedule & monitor ETL jobs |
| **Data Warehouse** | Snowflake | Store & process large datasets |
| **Transformation** | dbt (Data Build Tool) | SQL-based data transformation |
| **Storage** | AWS S3 | Raw data lake storage |
| **Crawling** | Python (aiohttp, asyncio) | Async API data collection |
| **Containerization** | Docker & Docker Compose | Environment consistency |
| **Version Control** | Git | Code management |

## 🏗️ Data Architecture
<img width="1333" height="773" alt="image" src="https://github.com/user-attachments/assets/eb9e6426-677d-4174-9494-9c0406d4fd71" />

### RAW Layer (Bronze)
- **Source**: OpenFoodFacts API via async crawler
- **Storage**: AWS S3 (CSV files)
- **Snowflake Table**: `FOOD.RAW.PRODUCTS`
- **Volume**: ~100k products

### SILVER Layer (Cleaning & Staging)
- **Transformations**:
  - Text normalization (remove accents, lowercase)
  - NULL handling & data validation
  - Explode dimensions (brands, categories, countries)
- **Tables**:
  - `STG_PRODUCTS`: 100k rows
  - `STG_BRANDS`: 150k rows (exploded)
  - `STG_CATEGORIES`: 200k rows (exploded)
  - `STG_COUNTRIES`: 50k rows (exploded)

### GOLD Layer (Analytics Ready)
- **SCD Type 2 Dimensions**:
  - `DIM_PRODUCT`: Product master with history
  - `DIM_BRAND`: Brand dimension
  - `DIM_CATEGORY`: Category dimension
  - `DIM_COUNTRY`: Country dimension
- **Fact Table**:
  - `FACT_NUTRITION`: Nutrition metrics with bridge connections
  - Volume: ~500k-1M rows (exploded combinations)

## 🚀 How to Run the Project

### Prerequisites
- Python 3.11+
- Docker & Docker Compose
- AWS S3 credentials
- Snowflake account
- dbt CLI

### Setup Steps

1. **Clone & Setup Environment**
   ```bash
   git clone <repo-url>
   cd DE_Project3
   ```

2. **Configure Environment Variables**
   ```bash
   # Create .env file with credentials
   AIRFLOW_DB_USER=airflow
   AIRFLOW_DB_PASSWORD=airflow
   AWS_ACCESS_KEY_ID=your-key
   AWS_SECRET_ACCESS_KEY=your-secret
   SNOWFLAKE_ACCOUNT=si00918.ap-southeast-1
   SNOWFLAKE_USER=your-user
   SNOWFLAKE_PASSWORD=your-password
   ```

3. **Start Airflow**
   ```bash
   docker-compose up -d
   # Access: http://localhost:8080
   ```

4. **Setup dbt**
   ```bash
   cd openfood_project
   dbt deps
   dbt debug  # Verify connection
   ```

5. **Run ETL Pipeline**
   ```bash
   # Crawl data to S3
   airflow dags unpause crawl_to_s3_v27

   # Load to Snowflake
   airflow dags unpause s3_to_snowflake_v4

   # Transform with dbt
   dbt run  # Build SILVER + GOLD layers
   dbt test # Run quality checks
   ```

## 📊 Sample Queries

### Top products by nutriscore
```sql
SELECT
    p.product_name,
    p.nutriscore_grade,
    COUNT(*) as brand_count
FROM GOLD.DIM_PRODUCT p
JOIN GOLD.FACT_NUTRITION f ON p.product_sk = f.product_sk
WHERE p.is_current = true
GROUP BY p.product_name, p.nutriscore_grade
ORDER BY nutriscore_grade;
```

### Nutrition analysis by brand
```sql
SELECT
    b.brand_name,
    AVG(f.energy_100g) as avg_energy,
    AVG(f.sugars_100g) as avg_sugars
FROM GOLD.DIM_BRAND b
JOIN GOLD.FACT_NUTRITION f ON b.brand_sk = f.brand_sk
WHERE b.is_current = true
GROUP BY b.brand_name
ORDER BY avg_energy DESC;
```

## 🔧 Troubleshooting

### Common Issues
- **Airflow DAG not running**: Check `docker logs airflow-scheduler`
- **dbt connection error**: Run `dbt debug`
- **Snapshot slow**: Increase Snowflake warehouse size to LARGE
- **S3 upload fails**: Verify AWS credentials

### Performance Tuning
- **Warehouse size**: `ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'LARGE';`
- **dbt threads**: `dbt run --threads 8`
- **Clustering**: Added cluster keys for snapshots

## 📈 Metrics & Performance

| Layer | Rows | Build Time | Notes |
|-------|------|------------|-------|
| RAW | ~100k | ~5 min | API crawl |
| SILVER | ~500k | ~2 min | dbt transform |
| GOLD | ~1M | ~5 min | SCD + fact generation |

## 📞 Contact & Support

Project: OpenFood Data Warehouse
Author: Huybuiii04
Date: November 2025

---

