# Tiki Product Scraper - PostgreSQL Integration

## Cấu hình Database

### 1. Cài đặt PostgreSQL
Đảm bảo PostgreSQL đã được cài đặt và đang chạy trên máy của bạn.

### 2. Tạo Database
```sql
CREATE DATABASE tiki_products;
```

### 3. Cấu hình kết nối
Chỉnh sửa file `config.py` với thông tin database của bạn:

```python
DB_HOST = "localhost"        # Địa chỉ server PostgreSQL
DB_PORT = 5432               # Cổng PostgreSQL (mặc định: 5432)
DB_NAME = "tiki_products"    # Tên database
DB_USER = "postgres"         # Username
DB_PASSWORD = "postgres"     # Password
```

### 4. Bật/tắt lưu vào database
```python
WRITE_TO_DB = True   # True: lưu vào DB, False: chỉ lưu JSON
```

## Cấu trúc bảng Products

Bảng `products` sẽ được tạo tự động với cấu trúc:

```sql
CREATE TABLE products (
    id BIGINT PRIMARY KEY,
    name VARCHAR(500),
    url_key VARCHAR(500),
    price DECIMAL(15, 2),
    description TEXT,
    images_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Chạy chương trình

```bash
python project_code.py
```

Chương trình sẽ:
1. Crawl data từ Tiki API
2. Lưu vào file JSON (như trước)
3. Đồng thời lưu vào PostgreSQL database
4. Tự động xử lý conflict (update nếu product_id đã tồn tại)

## Kiểm tra dữ liệu

```sql
-- Xem tổng số sản phẩm
SELECT COUNT(*) FROM products;

-- Xem 10 sản phẩm mới nhất
SELECT * FROM products ORDER BY created_at DESC LIMIT 10;

-- Tìm sản phẩm theo tên
SELECT * FROM products WHERE name LIKE '%điện thoại%';
```
