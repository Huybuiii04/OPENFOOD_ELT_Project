# 📊 Hướng Dẫn Chi Tiết về dbt (Data Build Tool) trong Project

## 📑 Mục Lục
1. [Giới thiệu về dbt](#giới-thiệu-về-dbt)
2. [Cài đặt dbt](#cài-đặt-dbt)
3. [Tạo dbt Project từ đầu](#tạo-dbt-project-từ-đầu)
4. [Cấu trúc Project](#cấu-trúc-project)
5. [Schema và Models](#schema-và-models)
6. [Cách chạy dbt](#cách-chạy-dbt)
7. [Best Practices](#best-practices)
8. [Troubleshooting](#troubleshooting)

---

## 🎯 Giới thiệu về dbt

**dbt (Data Build Tool)** là công cụ transformation cho phép bạn:
- ✅ Viết SQL để transform data trong warehouse
- ✅ Tự động quản lý dependencies giữa các models
- ✅ Test data quality
- ✅ Document data models
- ✅ Version control cho SQL code
- ✅ Implement SCD Type 2 (Slowly Changing Dimensions)

### Tại sao dùng dbt?
- **Modular**: Chia nhỏ SQL phức tạp thành các models đơn giản
- **DRY Principle**: Không lặp lại code, tái sử dụng models
- **Testing**: Built-in data tests
- **Documentation**: Tự động generate docs
- **Jinja Templates**: Sử dụng macros và loops trong SQL

---

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

## 🏗️ Tạo dbt Project từ đầu

### Bước 1: Khởi tạo project mới

```bash
# Tạo project mới
dbt init openfood_project

# Di chuyển vào thư mục project
cd openfood_project
```

### Bước 2: Cấu trúc thư mục được tạo tự động

```
openfood_project/
├── dbt_project.yml          # Cấu hình chính của project
├── profiles.yml             # Connection settings (optional, nếu không dùng ~/.dbt/profiles.yml)
├── packages.yml             # Dependencies/packages
├── README.md
├── analyses/                # SQL queries để phân tích (không build)
├── macros/                  # Jinja macros tái sử dụng
├── models/                  # SQL models (CORE của dbt)
│   ├── silver/             # Staging layer
│   └── gold/               # Analytics layer
├── seeds/                   # CSV files để load vào warehouse
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

## 🚀 Cách chạy dbt

### Các lệnh cơ bản

```bash
# 1. Kiểm tra connection
dbt debug

# 2. Cài đặt dependencies
dbt deps

# 3. Compile models (không run)
dbt compile

# 4. Run models (tạo tables/views)
dbt run

# 5. Run snapshots (SCD Type 2)
dbt snapshot

# 6. Run tests
dbt test

# 7. Generate documentation
dbt docs generate

# 8. View documentation
dbt docs serve
```

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
