# OpenFood Data Warehouse Project

## 📋 Tổng quan Project

Dự án xây dựng **Data Warehouse hoàn chỉnh** cho dữ liệu OpenFoodFacts sử dụng kiến trúc hiện đại **Medallion Architecture** (RAW → SILVER → GOLD). Project bao gồm ETL pipeline từ API crawling đến analytics-ready data.

### 🎯 Những gì đã thực hiện

#### 1. **Data Ingestion & ETL Pipeline**
- **Crawler bất đồng bộ**: Sử dụng aiohttp để crawl OpenFoodFacts API với concurrency 20
- **Checkpoint tracking**: Theo dõi tiến độ crawl, resume được khi bị gián đoạn
- **Rate limiting**: Tránh bị block API với delay 0.5s/request
- **Error handling**: Retry logic và logging chi tiết

#### 2. **Data Storage & Processing**
- **AWS S3**: Lưu trữ raw data dưới dạng CSV (10k rows/file)
- **Snowflake**: Data warehouse chính với 3 layers
- **dbt**: Transform data với 500k+ rows, SCD Type 2 cho dimensions

#### 3. **Data Quality & Governance**
- **Data cleaning**: Loại bỏ accents, normalize text, handle NULLs
- **Dimension explosion**: Tách comma-separated values thành individual rows
- **SCD Type 2**: Historical tracking cho products, brands, categories, countries
- **Data testing**: dbt tests cho unique keys, referential integrity

#### 4. **Orchestration & Monitoring**
- **Airflow DAGs**: 2 DAGs scheduled daily cho crawl và load
- **Docker Compose**: Containerized environment
- **Logging & Monitoring**: Chi tiết logs cho troubleshooting

## 🛠️ Công cụ & Technologies

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Orchestration** | Apache Airflow | Schedule & monitor ETL jobs |
| **Data Warehouse** | Snowflake | Store & process large datasets |
| **Transformation** | dbt (Data Build Tool) | SQL-based data transformation |
| **Storage** | AWS S3 | Raw data lake storage |
| **Crawling** | Python (aiohttp, asyncio) | Async API data collection |
| **Containerization** | Docker & Docker Compose | Environment consistency |
| **Version Control** | Git | Code management |

## 🏗️ Kiến trúc Data

![Medallion Architecture Diagram](images/architecture_diagram.png)

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
  - `FACT_NUTRITION`: Bridge table với metrics (energy, sugars)
  - Volume: ~500k-1M rows (exploded combinations)

## 🚀 Cách chạy Project

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
   # Tạo file .env với credentials
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
- **Clustering**: Added cluster keys cho snapshots

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

**Status**: ✅ Complete ETL pipeline with monitoring & testing