# OpenFood Data Warehouse Project

A complete data warehouse solution for OpenFoodFacts data using **Airflow**, **dbt**, and **Snowflake** with a modern medallion architecture (RAW → SILVER → GOLD).

## 📊 Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    OpenFoodFacts API                         │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
        ┌────────────────────────────────────┐
        │  Airflow DAG: crawl_to_s3_v27     │
        │  - Async crawler (CONCURRENCY=20) │
        │  - Rate limiting & error handling │
        │  - Checkpoint tracking            │
        └────────────┬───────────────────────┘
                     │
                     ▼
        ┌─────────────────────────────────┐
        │  AWS S3 (raw-food-project)      │
        │  - CSV files (10k rows per file)│
        │  - Bronze layer storage         │
        └────────────┬────────────────────┘
                     │
                     ▼
        ┌──────────────────────────────────────┐
        │  Airflow DAG: s3_to_snowflake_v4    │
        │  - List S3 files                    │
        │  - Load into Snowflake              │
        │  - Daily schedule                   │
        └────────────┬─────────────────────────┘
                     │
                     ▼
        ┌──────────────────────────────────────┐
        │      SNOWFLAKE: FOOD Database        │
        │  ┌──────────────────────────────┐   │
        │  │  RAW Layer                   │   │
        │  │  - FOOD.RAW.PRODUCTS         │   │
        │  │  - Raw data from API         │   │
        │  └──────────┬───────────────────┘   │
        │             │                       │
        │             ▼ (dbt run)             │
        │  ┌──────────────────────────────┐   │
        │  │  SILVER Layer                │   │
        │  │  - FOOD.SILVER.STG_PRODUCTS  │   │
        │  │  - FOOD.SILVER.STG_BRANDS    │   │
        │  │  - FOOD.SILVER.STG_CATEG.    │   │
        │  │  - FOOD.SILVER.STG_COUNTRIES │   │
        │  │                              │   │
        │  │  ✓ Cleaned & normalized      │   │
        │  │  ✓ Accents removed           │   │
        │  │  ✓ Exploded dimensions       │   │
        │  └──────────┬───────────────────┘   │
        │             │                       │
        │             ▼ (dbt run)             │
        │  ┌──────────────────────────────┐   │
        │  │  GOLD Layer (SCD Type 2)     │   │
        │  │  - FOOD.GOLD.DIM_PRODUCT     │   │
        │  │  - FOOD.GOLD.DIM_BRAND       │   │
        │  │  - FOOD.GOLD.DIM_CATEGORY    │   │
        │  │  - FOOD.GOLD.DIM_COUNTRY     │   │
        │  │  - FOOD.GOLD.FACT_NUTRITION  │   │
        │  │                              │   │
        │  │  ✓ Surrogate keys            │   │
        │  │  ✓ Historical tracking       │   │
        │  │  ✓ valid_from/valid_to       │   │
        │  │  ✓ is_current flags          │   │
        │  └──────────────────────────────┘   │
        └──────────────────────────────────────┘
```

## 🏗️ Data Layers

### RAW Layer (FOOD.RAW)
**Source:** OpenFoodFacts API via async crawler

**Tables:**
- `PRODUCTS` - Raw product data with all original fields

**Characteristics:**
- Minimal transformation
- Direct from API ingestion
- High volume (100k+ rows)

---

### SILVER Layer (FOOD.SILVER)
**Purpose:** Data cleaning, normalization, and staging

**Tables:**
| Table | Purpose | Rows |
|-------|---------|------|
| `STG_PRODUCTS` | Cleaned products with normalized text | ~100k |
| `STG_BRANDS` | Exploded brand dimension | ~150k |
| `STG_CATEGORIES` | Exploded category dimension | ~200k |
| `STG_COUNTRIES` | Exploded country dimension | ~50k |

**Transformations Applied:**
- ✅ **NULL Handling:** Coalesce to empty strings, filter empty names
- ✅ **Text Normalization:** 
  - Remove Vietnamese/French accents (á, à, ã, é, etc.)
  - Lowercase for brands/categories/ingredients
  - UPPERCASE for country codes
- ✅ **Whitespace Cleanup:**
  - Trim leading/trailing spaces
  - Collapse multiple spaces to single space
- ✅ **Special Character Removal:** Keep alphanumeric, hyphens, commas
- ✅ **Dimension Explosion:** Split comma-separated values into individual rows
- ✅ **Numeric Validation:** Energy > 0, Sugars >= 0

---

### GOLD Layer (FOOD.GOLD)
**Purpose:** Business-ready dimensional and fact tables with SCD Type 2

**Dimensional Tables (with SCD Type 2):**

#### `DIM_PRODUCT`
```sql
Columns: product_sk, product_id, code, product_name, 
         ingredients_text, nutriscore_grade, 
         valid_from, valid_to, is_current
```
- Tracks product changes over time
- Hash detection for changes
- Current flag for active records

#### `DIM_BRAND`
```sql
Columns: brand_sk, brand_name, valid_from, valid_to, is_current
```

#### `DIM_CATEGORY`
```sql
Columns: category_sk, category_name, valid_from, valid_to, is_current
```

#### `DIM_COUNTRY`
```sql
Columns: country_sk, country_code, valid_from, valid_to, is_current
```

**Fact Table:**

#### `FACT_NUTRITION`
```sql
Columns: fact_id, product_sk, brand_sk, category_sk, country_sk,
         energy_100g, sugars_100g, load_time
```
- Bridge table connecting all dimensions
- Nutrition metrics (energy, sugars per 100g)
- Multiple fact rows per product (one per brand×category×country combination)

**SCD Type 2 Features:**
- Surrogate keys for each dimension
- `valid_from` / `valid_to` timestamps
- `is_current` boolean flag
- Full historical tracking of changes
- Hash-based change detection

---

## 🚀 Getting Started

### Prerequisites
- Python 3.11+
- Docker & Docker Compose
- dbt 1.10+
- Snowflake account with credentials

### Setup

#### 1. Clone Repository
```bash
git clone <repo-url>
cd DE_Project3
```

#### 2. Configure Environment Variables
Create/update `.env` file:
```env
# Airflow
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=airflow
AIRFLOW_DB_NAME=airflow

# AWS S3
AWS_ACCESS_KEY_ID=<your-key>
AWS_SECRET_ACCESS_KEY=<your-secret>
AWS_DEFAULT_REGION=ap-southeast-1

# Snowflake
SNOWFLAKE_ACCOUNT=si00918.ap-southeast-1
SNOWFLAKE_USER=huybui04
SNOWFLAKE_PASSWORD=<your-password>
SNOWFLAKE_DATABASE=FOOD
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_TABLE=PRODUCTS
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
```

#### 3. Start Airflow
```bash
docker-compose up -d
# Access at http://localhost:8080
```

#### 4. Configure dbt
```bash
cd openfood_project
dbt deps        # Install packages
dbt debug       # Verify Snowflake connection
```

#### 5. Run dbt Models
```bash
dbt run         # Build SILVER + GOLD layers
dbt test        # Run data quality tests
```

---

## 📋 DAGs Overview

### `crawl_to_s3_v27` (Airflow)
- **Schedule:** Daily @ 00:00 UTC
- **Tasks:**
  1. Create S3 bucket (if needed)
  2. Run async crawler (OpenFoodFacts API)
  3. Save checkpoint progress
  4. Upload CSV to S3
- **Features:**
  - Async requests with CONCURRENCY=20
  - Rate limiting (0.5s delay per request)
  - Checkpoint-based resumption
  - Error handling & logging

### `s3_to_snowflake_v4` (Airflow)
- **Schedule:** Daily @ 01:00 UTC
- **Tasks:**
  1. List CSV files from S3
  2. Create Snowflake table (if needed)
  3. Batch load data into FOOD.RAW.PRODUCTS
- **Features:**
  - Dependency: waits for crawl_to_s3 completion
  - Automatic table creation
  - Batch insert with error handling

---

## 📊 dbt Models

### Silver Models (Data Cleaning)
```bash
dbt run --select models/silver
```
Generates:
- `stg_products` - 1 row per product
- `stg_brands` - 1 row per product-brand combination
- `stg_categories` - 1 row per product-category combination
- `stg_countries` - 1 row per product-country combination

### Gold Models (Dimensional/Fact)
```bash
dbt run --select models/gold
```
Generates:
- `dim_product`, `dim_brand`, `dim_category`, `dim_country`
- `fact_nutrition` - Bridge table with metrics

---

## 🧪 Data Quality Tests

Run all tests:
```bash
dbt test
```

Includes:
- ✅ Unique key tests (product_sk, brand_sk, etc.)
- ✅ Not null tests on primary keys
- ✅ Referential integrity checks
- ✅ Custom data quality validations

---

## 📈 Metrics & Performance

| Layer | Tables | Rows | Update Frequency | Purpose |
|-------|--------|------|------------------|---------|
| RAW | 1 | ~100k | Daily | Source data |
| SILVER | 4 | ~500k | Daily | Cleaned data |
| GOLD | 5 | ~500k | Daily | Analytics-ready |

**Sample Query Times:**
- STG_PRODUCTS: ~1s (100k rows)
- FACT_NUTRITION: ~90s (500k rows, fact generation)

---

## 🔍 Query Examples

### Get products by nutriscore grade
```sql
SELECT 
    p.product_name,
    p.nutriscore_grade,
    COUNT(DISTINCT f.brand_sk) as num_brands,
    COUNT(DISTINCT f.category_sk) as num_categories
FROM GOLD.DIM_PRODUCT p
JOIN GOLD.FACT_NUTRITION f ON p.product_sk = f.product_sk
WHERE p.is_current = true
GROUP BY p.product_name, p.nutriscore_grade
ORDER BY p.nutriscore_grade;
```

### Track product metadata changes (SCD Type 2)
```sql
SELECT 
    product_id,
    product_name,
    valid_from,
    valid_to,
    is_current
FROM GOLD.DIM_PRODUCT
WHERE product_id = 'xxxx'
ORDER BY valid_from DESC;
```

### Nutrition facts by brand
```sql
SELECT 
    b.brand_name,
    AVG(f.energy_100g) as avg_energy,
    AVG(f.sugars_100g) as avg_sugars,
    COUNT(f.fact_id) as num_products
FROM GOLD.DIM_BRAND b
JOIN GOLD.FACT_NUTRITION f ON b.brand_sk = f.brand_sk
WHERE b.is_current = true
GROUP BY b.brand_name
ORDER BY avg_energy DESC;
```

---

## 🛠️ Troubleshooting

### Airflow DAG not running
```bash
docker logs airflow-scheduler
```

### dbt connection error
```bash
dbt debug
# Check profiles.yml at ~/.dbt/profiles.yml
```

### Snowflake query timeout
- Increase warehouse size in profiles.yml
- Run `ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'LARGE';`

### S3 upload failing
- Verify AWS credentials in `.env`
- Check S3 bucket permissions
- Ensure bucket name matches in Crawl_API.py

---

## 📚 Project Structure

```
DE_Project3/
├── .env                                 # Configuration
├── docker-compose.yml                   # Airflow containers
├── Crawl_API/                          # API crawler scripts
│   ├── main.py
│   ├── checkpoint.py
│   ├── uploadS3.py
│   └── logs/
├── airflow/
│   ├── dags/
│   │   ├── crawl_to_s3_v27.py          # Crawler orchestration
│   │   └── s3_to_snowflake_v4.py       # S3 to Snowflake loader
│   ├── scripts/
│   │   ├── Crawl_API.py
│   │   ├── checkpoint.py
│   │   └── uploadS3.py
│   └── logs/                            # Airflow logs
└── openfood_project/                    # dbt project
    ├── dbt_project.yml
    ├── packages.yml
    ├── profiles.yml                     # dbt Snowflake config
    ├── macros/
    │   └── generate_schema_name.sql     # Custom schema naming
    └── models/
        ├── silver/                      # Staging layer
        │   ├── stg_products.sql
        │   ├── stg_brands.sql
        │   ├── stg_categories.sql
        │   ├── stg_countries.sql
        │   └── schema.yml
        └── gold/                        # Dimensional layer
            ├── dim_product.sql
            ├── dim_brand.sql
            ├── dim_category.sql
            ├── dim_country.sql
            ├── fact_nutrition.sql
            └── schema.yml
```

---

## 🔄 Data Flow Summary

1. **Crawl Phase** (Airflow DAG: `crawl_to_s3_v27`)
   - Async crawler hits OpenFoodFacts API
   - Saves 10k rows per CSV file to S3
   - Tracks progress with checkpoint

2. **Load Phase** (Airflow DAG: `s3_to_snowflake_v4`)
   - Lists CSV files from S3
   - Batch inserts into FOOD.RAW.PRODUCTS

3. **Transform Phase** (dbt: `dbt run`)
   - SILVER: Clean, normalize, explode dimensions
   - GOLD: Build dimensional model with SCD Type 2
   - All data ready for analytics

---

## 📞 Contact & Support

For issues or questions:
1. Check Airflow logs: `docker logs airflow-scheduler`
2. Check dbt logs: `target/run/` directory
3. Query Snowflake tables directly for data validation

---

## 📝 License

Internal Project - OpenFood Data Warehouse

---

**Last Updated:** November 2025
**Version:** 1.0.0
