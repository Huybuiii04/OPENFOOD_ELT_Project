# 📊 Hướng Dẫn Chi Tiết về dbt (Data Build Tool) trong Project

## 📑 Mục Lục
1. [Giới thiệu về dbt](#giới-thiệu-về-dbt)
2. [Quick Start - Chạy Project](#quick-start---chạy-project)
3. [Cài đặt dbt](#cài-đặt-dbt)
4. [Cấu hình Connection](#cấu-hình-connection)
5. [Cấu trúc Project](#cấu-trúc-project)
6. [Pipeline Architecture](#pipeline-architecture)
7. [Chi tiết từng Layer](#chi-tiết-từng-layer)
8. [Testing & Monitoring](#testing--monitoring)
9. [Troubleshooting](#troubleshooting)

---

## 🚀 Quick Start - Chạy Project

### ⚡ Chạy toàn bộ pipeline (từ đầu đến cuối):

```bash
# Bước 1: Di chuyển vào thư mục dbt project
cd openfood_project

# Bước 2: Kiểm tra kết nối Snowflake
dbt debug --profiles-dir .

# Bước 3: Cài đặt dependencies (dbt_utils)
dbt deps --profiles-dir .

# Bước 4: Chạy snapshots trước (tạo SCD Type 2 tables)
dbt snapshot --profiles-dir .

# Bước 5: Chạy tất cả models (SILVER → GOLD)
dbt run --profiles-dir .

# Bước 6: Chạy tests để kiểm tra data quality
dbt test --profiles-dir .

# Bước 7: Generate documentation
dbt docs generate --profiles-dir .

# Bước 8: Xem documentation trong browser
dbt docs serve --profiles-dir .
```

### 📊 Kết quả mong đợi:
---

## 🔐 Cấu hình Connection

### Cách 1: Dùng profiles.yml trong project (Recommended cho project này)

File: `openfood_project/profiles.yml`

```yaml
# Snowflake connection profile for openfood_project
openfood_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: sb30236.ap-southeast-1  # Format: account.region
      user: huybuii04
      password: "YOUR_PASSWORD_HERE"
      role: ACCOUNTADMIN
      database: FOOD
      warehouse: COMPUTE_WH
      schema: RAW
      threads: 4
      client_session_keep_alive: False
```

**Lấy thông tin Snowflake account:**
```sql
-- Chạy trong Snowflake worksheet
SELECT 
    CURRENT_ACCOUNT() AS account,
    CURRENT_REGION() AS region,
    LOWER(CURRENT_ACCOUNT()) || '.' || LOWER(REPLACE(CURRENT_REGION(), '_', '-')) AS account_identifier;
```

### Cách 2: Dùng environment variables (Production)

File: `openfood_project/profiles.yml`

```yaml
openfood_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: ACCOUNTADMIN
      database: FOOD
      warehouse: COMPUTE_WH
      schema: RAW
      threads: 4
```

Set variables:
```powershell
# PowerShell
$env:SNOWFLAKE_ACCOUNT = "sb30236.ap-southeast-1"
$env:SNOWFLAKE_USER = "huybuii04"
$env:SNOWFLAKE_PASSWORD = "your-password"
```

### 3. Kiểm tra connection

```bash
cd openfood_project
dbt debug --profiles-dir .

# Expected output:
# ✅ profiles.yml file [OK found and valid]
# ✅ dbt_project.yml file [OK found and valid]
# ✅ Connection test: [OK connection ok]
# ✅ All checks passed!
## 🔧 Cài đặt dbt

### 1. Cài đặt dbt-core và adapter

```bash
# Cài dbt-core và dbt-snowflake adapter
pip install dbt-core dbt-snowflake

# Kiểm tra version
dbt --version
```

### 2. Cấu hình Connection Profile

Tạo file `~/.dbt/profiles.yml` (Windows: `C:\Users\<username>\.dbt\profiles.yml`):

```yaml
openfood_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: si00918.ap-southeast-1  # Snowflake account
      user: YOUR_USERNAME
      password: YOUR_PASSWORD
      role: ACCOUNTADMIN
      database: FOOD
      warehouse: COMPUTE_WH
      schema: silver
      threads: 4
      client_session_keep_alive: False
    
    prod:
      type: snowflake
      account: si00918.ap-southeast-1
      user: YOUR_USERNAME
      password: YOUR_PASSWORD
      role: ACCOUNTADMIN
      database: FOOD
      warehouse: COMPUTE_WH
      schema: gold
      threads: 8
      client_session_keep_alive: False
```

### 3. Kiểm tra connection

```bash
cd openfood_project
dbt debug
```

---

## 📂 Cấu trúc Project

```
openfood_project/
├── dbt_project.yml          # Cấu hình chính của project
├── profiles.yml             # Connection settings (Snowflake)
├── packages.yml             # Dependencies (dbt_utils)
├── README.md
│
├── models/                  # ⭐ SQL models (CORE của dbt)
│   ├── silver/             # 🥈 Staging/Cleaning layer
│   │   ├── schema.yml      # Source và column definitions
│   │   ├── stg_products.sql
│   │   ├── stg_brands.sql
│   │   ├── stg_categories.sql
│   │   └── stg_countries.sql
│   │
│   ├── gold/               # 🥇 Analytics/Business layer
│   │   ├── schema.yml
│   │   ├── dim_product.sql
│   │   ├── dim_brand.sql
│   │   ├── dim_category.sql
│   │   ├── dim_country.sql
│   │   ├── fact_nutrition.sql
│   │   ├── bridge_product_brand.sql
│   │   ├── bridge_product_category.sql
│   │   └── bridge_product_country.sql
│   │
│   └── monitoring/         # 📊 Data quality monitoring
│       └── duplicate_check_report.sql
│
├── snapshots/              # 📸 SCD Type 2 tracking
│   ├── snap_products.sql
│   ├── snap_brands.sql
│   ├── snap_categories.sql
│   └── snap_countries.sql
│
├── macros/                 # 🔧 Custom functions
│   └── generate_schema_name.sql
│
├── tests/                  # ✅ Custom data tests
├── analyses/               # 📈 Ad-hoc queries
├── seeds/                  # 📄 Static CSV data
└── target/                 # 🎯 Compiled SQL và artifacts
    ├── compiled/
    ├── run/
    └── manifest.json
```

---

## 🏛️ Pipeline Architecture

### Data Flow: RAW → SILVER → GOLD

```
┌─────────────────────────────────────────────────────────────┐
│  RAW LAYER (Snowflake: FOOD.RAW.PRODUCTS_RAW)              │
│  Source: S3 bucket via Airflow                              │
│  Records: 99,940                                            │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
    ┌──────────────────────────────────────┐
    │  dbt source('raw', 'products')       │
    │  Defined in: models/silver/schema.yml│
    └──────────────┬───────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│  SILVER LAYER (FOOD.SILVER.*)                               │
│  Purpose: Cleaning, Normalization, Deduplication            │
├─────────────────────────────────────────────────────────────┤
│  • stg_products (93,183 rows) - Remove nulls, clean text   │
│  • stg_brands - Explode comma-separated brands             │
│  • stg_categories - Explode categories                     │
│  • stg_countries - Explode countries                       │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
    ┌──────────────────────────────────────┐
    │  dbt snapshot (SCD Type 2)           │
    │  FOOD.SNAPSHOTS.*                    │
    │  Track historical changes            │
    └──────────────┬───────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│  GOLD LAYER (FOOD.GOLD.*) - Star Schema                    │
│  Purpose: Business-ready analytics tables                   │
├─────────────────────────────────────────────────────────────┤
│  📊 FACT TABLE:                                            │
│    • fact_nutrition (93,183 rows) - Nutrition metrics      │
│                                                             │
│  📁 DIMENSION TABLES:                                      │
│    • dim_product - Product master data                     │
│    • dim_brand - Brand master                              │
│    • dim_category - Category master                        │
│    • dim_country - Country master                          │
│                                                             │
│  🔗 BRIDGE TABLES (Many-to-Many):                         │
│    • bridge_product_brand                                  │
│    • bridge_product_category                               │
│    • bridge_product_country                                │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔍 Chi tiết từng Layer

### 1️⃣ RAW Layer (Bronze)

**Location**: `FOOD.RAW.PRODUCTS_RAW`  
**Source**: Loaded từ S3 via Airflow DAG

**Cấu hình trong dbt** (`models/silver/schema.yml`):
```yaml
sources:
  - name: raw
    database: FOOD
    schema: RAW
    tables:
      - name: products
        identifier: PRODUCTS_RAW  # Actual table name
        description: Raw product data from OpenFoodFacts API
```

### 2️⃣ SILVER Layer (Staging)

**Purpose**: Data cleaning, normalization, deduplication

#### `stg_products.sql` - Main staging table
```sql
-- Key transformations:
• Remove null/empty product names
• Clean and normalize text (remove accents, trim spaces)
• Lowercase codes
• Handle NULL values in numeric fields
• Add loaded_at timestamp
• Deduplicate by id (row_number() partition)
```

**Data quality**:
- Input: 99,940 rows
- Output: 93,183 rows (93.2% quality rate)
- Removed: 6,757 rows (null/empty names)

#### `stg_brands.sql`, `stg_categories.sql`, `stg_countries.sql`
```sql
-- Explode comma-separated values into individual rows
Example:
  brands: "Coca-Cola, PepsiCo"
  →
  Row 1: Coca-Cola
  Row 2: PepsiCo
```

### 3️⃣ SNAPSHOTS Layer

**Purpose**: Track historical changes (SCD Type 2)

**Columns auto-generated**:
- `dbt_valid_from` - Start date
- `dbt_valid_to` - End date (NULL = current)
- `dbt_updated_at` - Last update timestamp
- `dbt_scd_id` - Unique version ID

**Strategy**: Check specific columns for changes
```yaml
strategy: check
check_cols: 
  - product_name
  - ingredients_text
  - nutriscore_grade
```

### 4️⃣ GOLD Layer (Analytics)

#### **Star Schema Design**:

**Fact Table**: `fact_nutrition`
- Grain: One row per product
- Measures: energy_100g, sugars_100g
- Foreign keys: product_sk

**Dimension Tables**:
- `dim_product`: product_id, code, name, ingredients
- `dim_brand`: brand_name (deduped, uppercase)
- `dim_category`: category_name
- `dim_country`: country_name, country_code

**Bridge Tables**: Handle many-to-many relationships
- 1 product → N brands
- 1 product → N categories  
- 1 product → N countries

---

## 🧪 Testing & Monitoring

### Built-in Tests

```yaml
# models/silver/schema.yml
tests:
  - unique            # No duplicates
  - not_null          # No NULL values
  - relationships     # Foreign key integrity
  - accepted_values   # Value in allowed list
```

**Chạy tests**:
```bash
# All tests
dbt test --profiles-dir .

# Specific model
dbt test --select stg_products --profiles-dir .

# Specific test type
dbt test --select test_type:unique --profiles-dir .
```

### Custom Monitoring Model

**`models/monitoring/duplicate_check_report.sql`**
```sql
-- Comprehensive duplicate check across all layers
-- Check by: id, code, product_name+brand
-- Output: duplicate_count, occurrence_count per key
```

**Query results**:
```sql
SELECT * FROM FOOD.MONITORING.DUPLICATE_CHECK_REPORT
ORDER BY duplicate_count DESC;
```

### Data Quality Metrics

```sql
-- Check data counts across layers
SELECT 
    'RAW' AS layer,
    COUNT(*) as total_rows,
    COUNT(DISTINCT id) as unique_ids
FROM FOOD.RAW.PRODUCTS_RAW

UNION ALL

SELECT 
    'SILVER',
    COUNT(*),
    COUNT(DISTINCT id)
FROM FOOD.SILVER.STG_PRODUCTS

UNION ALL

SELECT 
    'GOLD - Fact',
    COUNT(*),
    COUNT(DISTINCT product_sk)
FROM FOOD.GOLD.FACT_NUTRITION;
```

---
├── snapshots/              # SCD Type 2 tracking
└── tests/                   # Custom SQL tests
```

### Bước 3: Cấu hình dbt_project.yml

```yaml
name: 'openfood_project'
version: '1.0.0'
profile: 'openfood_project'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:
  - "target"
  - "dbt_packages"

models:
  openfood_project:
    +materialized: table
    silver:
      +schema: silver
      +materialized: table
    gold:
      +schema: gold
      +materialized: table
```

### Bước 4: Cài đặt packages (dependencies)

Tạo file `packages.yml`:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```

Cài đặt packages:

```bash
dbt deps
```

---

## 📂 Cấu trúc Project OpenFood

### Tổng quan kiến trúc dữ liệu

```
RAW (Snowflake)
    ↓
SILVER (dbt staging)
    ↓
SNAPSHOTS (SCD Type 2)
    ↓
GOLD (dbt analytics)
```

### Chi tiết các thư mục

#### 1. **models/silver/** - Data Cleaning & Staging

Chứa các models làm sạch và chuẩn hóa dữ liệu từ RAW layer.

**File: `models/silver/stg_products.sql`**
```sql
{{ config(
    materialized='table',
    schema='silver'
) }}

with raw_products as (
    select
        id,
        code,
        product_name,
        brands,
        countries,
        categories,
        ingredients_text,
        nutriscore_grade,
        energy_100g,
        sugars_100g,
        current_timestamp() as loaded_at
    from {{ source('raw', 'products') }}
    where product_name is not null
),

cleaned_products as (
    select
        coalesce(trim(id), '') as id,
        coalesce(trim(lower(code)), '') as code,
        -- Remove accents, normalize text
        trim(
            regexp_replace(
                translate(
                    product_name,
                    'áàảãạăắằẳẵặâấầẩẫậéèẻẽẹêếềểễệíìỉĩịóòỏõọ',
                    'aaaaaaaaaaaaaaaaaeeeeeeeeeeiiiiioooooo'
                ),
                '\s+', ' '
            )
        ) as product_name,
        brands,
        countries,
        categories,
        ingredients_text,
        nutriscore_grade,
        energy_100g,
        sugars_100g,
        loaded_at
    from raw_products
)

select * from cleaned_products
```

**File: `models/silver/stg_brands.sql`** - Dimension Explosion
```sql
{{ config(
    materialized='table',
    schema='silver'
) }}

-- Split comma-separated brands into individual rows
with split_brands as (
    select
        id as product_id,
        trim(value) as brand_name
    from {{ ref('stg_products') }},
    lateral flatten(input => split(brands, ','))
    where trim(value) != ''
)

select distinct
    product_id,
    brand_name
from split_brands
```

**Files tương tự:**
- `stg_categories.sql` - Split categories
- `stg_countries.sql` - Split countries

**Schema file: `models/silver/schema.yml`**
```yaml
version: 2

sources:
  - name: raw
    database: food
    schema: raw
    tables:
      - name: products
        description: Raw products from OpenFoodFacts API

models:
  - name: stg_products
    description: Cleaned products with normalized text
    columns:
      - name: id
        description: Product ID
        tests:
          - unique
          - not_null
      - name: product_name
        description: Product name (normalized)
        tests:
          - not_null
```

#### 2. **snapshots/** - SCD Type 2 Implementation

Snapshots tự động track thay đổi của dữ liệu theo thời gian.

**File: `snapshots/snap_products.sql`**
```sql
{% snapshot snap_products %}
    {{
        config(
            target_schema='snapshots',
            unique_key='id',
            strategy='check',
            check_cols=['code', 'product_name', 'ingredients_text', 'nutriscore_grade']
        )
    }}
    
    with dedup as (
        select
            id,
            code,
            product_name,
            brands,
            countries,
            categories,
            ingredients_text,
            nutriscore_grade,
            energy_100g,
            sugars_100g,
            loaded_at,
            row_number() over (partition by id order by loaded_at desc) as rn
        from {{ ref('stg_products') }}
    )
    
    select
        id,
        code,
        product_name,
        brands,
        countries,
        categories,
        ingredients_text,
        nutriscore_grade,
        energy_100g,
        sugars_100g,
        loaded_at
    from dedup
    where rn = 1
    
{% endsnapshot %}
```

**Cách snapshot hoạt động:**
- **strategy='check'**: Kiểm tra các cột được chỉ định
- **check_cols**: Danh sách cột để theo dõi thay đổi
- **dbt_valid_from**: Timestamp bắt đầu valid
- **dbt_valid_to**: Timestamp kết thúc valid (NULL nếu là current)
- **dbt_scd_id**: Unique ID cho mỗi snapshot row

#### 3. **models/gold/** - Dimension & Fact Tables

**File: `models/gold/dim_product.sql`**
```sql
{{ config(
    materialized='table',
    schema='gold'
) }}

select
    {{ dbt_utils.generate_surrogate_key(['id']) }} as product_sk,
    id as product_id,
    code,
    product_name,
    ingredients_text,
    nutriscore_grade,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    case when dbt_valid_to is null then true else false end as is_current
from {{ ref('snap_products') }}
```

**File: `models/gold/bridge_product_brand.sql`**
```sql
{{ config(
    materialized='table',
    schema='gold'
) }}

-- Bridge table to connect products with multiple brands
select
    product_id,
    brand_name
from {{ ref('stg_brands') }}
```

**File: `models/gold/fact_nutrition.sql`**
```sql
{{ config(
    materialized='table',
    schema='gold'
) }}

with fact_base as (
    select
        id as product_id,
        energy_100g,
        sugars_100g,
        nutriscore_grade,
        loaded_at
    from {{ ref('stg_products') }}
)

select
    row_number() over (order by p.product_sk) as fact_id,
    p.product_sk,
    b.brand_sk,
    c.category_sk,
    co.country_sk,
    fb.energy_100g,
    fb.sugars_100g,
    fb.nutriscore_grade,
    fb.loaded_at as load_time
from fact_base fb
left join {{ ref('dim_product') }} p 
    on fb.product_id = p.product_id 
    and p.is_current = true
left join {{ ref('bridge_product_brand') }} pb 
    on fb.product_id = pb.product_id
left join {{ ref('dim_brand') }} b 
    on pb.brand_name = b.brand_name 
    and b.is_current = true
left join {{ ref('bridge_product_category') }} pc 
    on fb.product_id = pc.product_id
left join {{ ref('dim_category') }} c 
    on pc.category_name = c.category_name 
    and c.is_current = true
left join {{ ref('bridge_product_country') }} pco 
    on fb.product_id = pco.product_id
left join {{ ref('dim_country') }} co 
    on pco.country_code = co.country_code 
    and co.is_current = true
```

**Schema file: `models/gold/schema.yml`**
```yaml
version: 2

models:
  - name: dim_product
    description: Product dimension with SCD Type 2
    columns:
      - name: product_sk
        description: Surrogate key for product
        tests:
          - unique
          - not_null
      - name: product_id
        description: Natural key
      - name: is_current
        description: Whether this is the current version

  - name: fact_nutrition
    description: Nutrition facts bridge table
    columns:
      - name: fact_id
        description: Fact table ID
        tests:
          - unique
          - not_null
      - name: product_sk
        description: Foreign key to dim_product
        tests:
          - relationships:
              to: ref('dim_product')
              field: product_sk
```

#### 4. **macros/** - Custom Macros

**File: `macros/generate_schema_name.sql`**
```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

Macro này override behavior mặc định để schema name không bao gồm prefix.

---

## � Troubleshooting

### Lỗi thường gặp

#### 1. Connection Error
```
Error: 250001 (08001): Failed to connect to DB: Incorrect username or password
```

**Giải pháp**:
- Kiểm tra username (lowercase: `huybuii04` không phải `HUYBUII04`)
- Verify password trong profiles.yml
- Check account identifier format: `account.region` (ví dụ: `sb30236.ap-southeast-1`)

```sql
-- Lấy đúng account identifier trong Snowflake:
SELECT 
    LOWER(CURRENT_ACCOUNT()) || '.' || 
    LOWER(REPLACE(CURRENT_REGION(), '_', '-')) AS account_identifier;
```

#### 2. Schema Not Found
```
Error: 002003 (02000): SQL compilation error: Schema 'FOOD.SNAPSHOTS' does not exist
```

**Giải pháp**:
```bash
# Chạy snapshot trước khi chạy models
dbt snapshot --profiles-dir .
dbt run --profiles-dir .
```

#### 3. Source Not Found
```
Error: Database Error in model stg_products (models\silver\stg_products.sql)
  Object 'FOOD.RAW.PRODUCTS' does not exist
```

**Giải pháp**:
- Kiểm tra table name trong Snowflake
- Cập nhật source definition trong `schema.yml`:
```yaml
sources:
  - name: raw
    tables:
      - name: products
        identifier: PRODUCTS_RAW  # Actual table name
```

#### 4. Duplicate Key Error
```
Error: Unique test failed for model stg_products column id
```

**Giải pháp**:
- Check duplicates:
```sql
SELECT id, COUNT(*) 
FROM FOOD.RAW.PRODUCTS_RAW 
GROUP BY id 
HAVING COUNT(*) > 1;
```
- Add deduplication logic:
```sql
WITH dedup AS (
    SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) as rn
    FROM source_table
)
SELECT * FROM dedup WHERE rn = 1
```

#### 5. Permission Error
```
Error: 003001 (42501): SQL execution error: Insufficient privileges to operate on schema 'GOLD'
```

**Giải pháp**:
```sql
-- Grant permissions trong Snowflake
GRANT USAGE ON DATABASE FOOD TO ROLE ACCOUNTADMIN;
GRANT CREATE SCHEMA ON DATABASE FOOD TO ROLE ACCOUNTADMIN;
GRANT ALL ON SCHEMA FOOD.SILVER TO ROLE ACCOUNTADMIN;
GRANT ALL ON SCHEMA FOOD.GOLD TO ROLE ACCOUNTADMIN;
```

#### 6. Profile Not Found
```
Error: Could not find profile named 'openfood_project'
```

**Giải pháp**:
```bash
# Use --profiles-dir to point to project directory
dbt run --profiles-dir .

# Or place profiles.yml in ~/.dbt/ directory
```

---

## 📚 Useful Commands

### Selective Run

```bash
# Run specific model
dbt run --select stg_products --profiles-dir .

# Run model + all downstream (dependent) models
dbt run --select stg_products+ --profiles-dir .

# Run model + all upstream (source) models
dbt run --select +dim_product --profiles-dir .

# Run specific folder
dbt run --select silver.* --profiles-dir .
dbt run --select gold.* --profiles-dir .

# Run specific tag
dbt run --select tag:daily --profiles-dir .

# Run modified models only
dbt run --select state:modified+ --profiles-dir .
```

### Testing

```bash
# Run all tests
dbt test --profiles-dir .

# Test specific model
dbt test --select stg_products --profiles-dir .

# Test specific type
dbt test --select test_type:unique --profiles-dir .
dbt test --select test_type:not_null --profiles-dir .
```

### Documentation

```bash
# Generate docs
dbt docs generate --profiles-dir .

# Serve docs on localhost:8080
dbt docs serve --profiles-dir .

# Generate and serve in one command
dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .
```

### Freshness Check

```bash
# Check source freshness
dbt source freshness --profiles-dir .
```

### Cleaning

```bash
# Clean compiled files
dbt clean

# Remove and reinstall packages
rm -rf dbt_packages/
dbt deps --profiles-dir .
```

---

## 🎯 Best Practices

### 1. Naming Conventions
- **Staging models**: `stg_<source>_<entity>` (e.g., `stg_products`)
- **Dimension tables**: `dim_<entity>` (e.g., `dim_product`)
- **Fact tables**: `fact_<entity>` (e.g., `fact_nutrition`)
- **Bridge tables**: `bridge_<relationship>` (e.g., `bridge_product_brand`)

### 2. Model Organization
```
models/
├── silver/          # Staging layer
│   └── schema.yml   # Sources + staging models
├── gold/            # Analytics layer
│   └── schema.yml   # Dimensions + facts
└── monitoring/      # Data quality checks
```

### 3. Testing Strategy
- **Unique**: Primary keys và surrogate keys
- **Not null**: Mandatory fields
- **Relationships**: Foreign key integrity
- **Custom tests**: Business logic validation

### 4. Documentation
- Add descriptions to all models
- Document column meanings
- Include data lineage
- Keep schema.yml up to date

### 5. Performance
- Use incremental models for large tables
- Partition by date for time-series data
- Use appropriate materializations:
  - `table` for frequently queried data
  - `view` for simple transformations
  - `incremental` for large fact tables

---

## 📊 Project Statistics

### Final Results

```
✅ Models: 12
   - SILVER: 4 models (stg_*)
   - GOLD: 8 models (dim_*, fact_*, bridge_*)
   - MONITORING: 1 model

✅ Snapshots: 4
   - snap_products, snap_brands, snap_categories, snap_countries

✅ Tests: 12 passed
   - Unique constraints: 6
   - Not null checks: 6

✅ Data Quality: 93.2%
   - RAW: 99,940 rows
   - SILVER: 93,183 rows (6,757 filtered out)
   - GOLD: 93,183 rows

✅ Documentation: Auto-generated
```

---

## 🔗 Related Files

- Main project: [README.md](README.md)
- Airflow DAGs: [airflow/dags/](airflow/dags/)
- S3 to Snowflake: [s3_to_snowflake.py](airflow/dags/s3_to_snowflake.py)
- DBT project: [openfood_project/](openfood_project/)

---

**Cập nhật lần cuối**: 2026-01-18  
**Author**: Data Engineering Team

### Workflow đầy đủ cho project này

```bash
# Bước 1: Cài đặt packages
cd openfood_project
dbt deps

# Bước 2: Run staging models (SILVER layer)
dbt run --models silver

# Bước 3: Run snapshots để track changes
dbt snapshot

# Bước 4: Run dimension và fact tables (GOLD layer)
dbt run --models gold

# Bước 5: Run all tests
dbt test

# Bước 6: Generate docs
dbt docs generate
dbt docs serve
```

### Run specific models

```bash
# Run một model cụ thể
dbt run --models stg_products

# Run một model và tất cả downstream models
dbt run --models stg_products+

# Run một model và tất cả upstream models
dbt run --models +dim_product

# Run tất cả models trong folder
dbt run --models silver

# Run models có tag
dbt run --models tag:daily
```

### Run với options

```bash
# Full refresh (drop và recreate tables)
dbt run --full-refresh

# Run với specific target
dbt run --target prod

# Run với nhiều threads (parallel execution)
dbt run --threads 8
```

---

## 📋 Schema và Model Configuration

### Config Options trong Models

#### 1. Materialization Types

```sql
-- View (default - nhanh, không tốn storage)
{{ config(materialized='view') }}

-- Table (chậm hơn, tốt cho performance query)
{{ config(materialized='table') }}

-- Incremental (chỉ process data mới)
{{ config(materialized='incremental') }}

-- Ephemeral (không tạo object, dùng như CTE)
{{ config(materialized='ephemeral') }}
```

#### 2. Schema và Tags

```sql
{{ config(
    materialized='table',
    schema='gold',
    tags=['daily', 'analytics']
) }}
```

#### 3. Incremental Models

```sql
{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='append_new_columns'
) }}

select * from {{ ref('stg_products') }}

{% if is_incremental() %}
    where loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}
```

### Sources Configuration

**File: `models/silver/schema.yml`**
```yaml
version: 2

sources:
  - name: raw
    database: food
    schema: raw
    tables:
      - name: products
        description: Raw products from S3
        columns:
          - name: id
            description: Product ID
            tests:
              - unique
              - not_null
```

### Tests trong dbt

#### Built-in Tests

```yaml
columns:
  - name: product_sk
    tests:
      - unique
      - not_null
      - accepted_values:
          values: ['A', 'B', 'C', 'D', 'E']
      - relationships:
          to: ref('dim_product')
          field: product_sk
```

#### Custom Tests

**File: `tests/test_nutrition_values.sql`**
```sql
-- Kiểm tra nutrition values không âm
select *
from {{ ref('fact_nutrition') }}
where energy_100g < 0 or sugars_100g < 0
```

---

## 🎨 Best Practices

### 1. Naming Conventions

```
Sources (RAW):       products, orders
Staging (SILVER):    stg_products, stg_orders
Snapshots:           snap_products, snap_orders
Dimensions (GOLD):   dim_product, dim_customer
Facts (GOLD):        fact_sales, fact_nutrition
Bridge tables:       bridge_product_brand
```

### 2. Folder Structure

```
models/
├── silver/           # Staging & cleaning
│   ├── schema.yml
│   ├── stg_products.sql
│   └── stg_brands.sql
├── gold/            # Analytics ready
│   ├── schema.yml
│   ├── dim_product.sql
│   └── fact_nutrition.sql
└── intermediate/    # Helper models (optional)
    └── int_product_enriched.sql
```

### 3. Documentation

- Luôn document models trong `schema.yml`
- Viết description cho columns quan trọng
- Thêm tests cho business rules
- Generate và review docs thường xuyên

### 4. Performance Optimization

```sql
-- Sử dụng QUALIFY thay vì subquery
select *
from my_table
qualify row_number() over (partition by id order by date desc) = 1

-- Tránh SELECT *
select id, name, price  -- Tốt
select *                 -- Không tốt

-- Sử dụng incremental models cho large tables
{{ config(materialized='incremental') }}
```

### 5. Version Control

```bash
# Commit thường xuyên
git add models/
git commit -m "Add dim_product model"
git push

# Sử dụng branches cho features mới
git checkout -b feature/add-sales-model
```

---

## 🔍 Troubleshooting

### 1. Connection Issues

```bash
# Error: Could not connect to Snowflake
Solution:
- Kiểm tra ~/.dbt/profiles.yml
- Verify credentials
- Check network/firewall
- Run: dbt debug
```

### 2. Compilation Errors

```bash
# Error: Compilation Error in model stg_products
Solution:
- Check Jinja syntax
- Verify ref() và source() references
- Run: dbt compile --models stg_products
```

### 3. Test Failures

```bash
# Error: Test unique_dim_product_product_sk failed
Solution:
- Query the model to find duplicates:
  SELECT product_sk, COUNT(*)
  FROM gold.dim_product
  GROUP BY product_sk
  HAVING COUNT(*) > 1
- Fix the upstream logic
```

### 4. Snapshot Issues

```bash
# Error: Snapshot not detecting changes
Solution:
- Check check_cols configuration
- Verify unique_key is truly unique
- Run: dbt snapshot --full-refresh (rebuild from scratch)
```

### 5. Performance Issues

```bash
# Models taking too long to run
Solution:
- Use incremental materialization
- Optimize SQL queries
- Increase threads: dbt run --threads 8
- Create indexes in warehouse
```

---

## 📊 dbt Documentation

### Generate và View Docs

```bash
# Generate docs
dbt docs generate

# Serve docs locally (mở browser tự động)
dbt docs serve --port 8080
```

Docs bao gồm:
- **Model Lineage**: Dependency graph
- **Column Descriptions**: From schema.yml
- **Test Results**: Pass/fail status
- **Source Freshness**: Data freshness checks

---

## 🎯 Kết luận

Với dbt trong project OpenFood này:

✅ **SILVER layer**: Clean và normalize data từ RAW
✅ **SNAPSHOTS**: Track historical changes (SCD Type 2)
✅ **GOLD layer**: Tạo star schema với dimensions và facts
✅ **Bridge tables**: Handle many-to-many relationships
✅ **Tests**: Ensure data quality
✅ **Documentation**: Auto-generated và maintainable

### Tài nguyên học thêm

- **Official Docs**: https://docs.getdbt.com/
- **dbt Discourse**: https://discourse.getdbt.com/
- **dbt Slack**: https://www.getdbt.com/community/join-the-community/
- **GitHub**: https://github.com/dbt-labs/dbt-core

---

## 📞 Support

Nếu gặp vấn đề, check:
1. `dbt debug` - Connection issues
2. `dbt compile` - Syntax errors
3. `target/run_results.json` - Execution details
4. `logs/dbt.log` - Detailed logs
