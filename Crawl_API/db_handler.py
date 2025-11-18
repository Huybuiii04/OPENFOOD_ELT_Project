import psycopg2
from psycopg2.extras import execute_values
from typing import List, Dict

class PostgreSQLHandler:
    def __init__(self, host="localhost", port=5432, database="tiki_products",
                 user="postgres", password="postgres"):
        self.config = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password
        }

    def connect(self):
        self.conn = psycopg2.connect(**self.config)
        self.cur = self.conn.cursor()

    def create_table(self):
        sql = """
        CREATE TABLE IF NOT EXISTS products (
            id BIGINT PRIMARY KEY,
            name VARCHAR(500),
            url_key VARCHAR(500),
            price DECIMAL(15, 2),
            description TEXT,
            images_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        self.cur.execute(sql)
        self.conn.commit()

    def save_products(self, products: List[Dict]):
        if not products:
            return 0

        # Chuẩn hóa data → list of tuples
        rows = [
            (
                p.get("id"),
                p.get("name"),
                p.get("url_key"),
                p.get("price"),
                p.get("description"),
                str(p.get("images_url", []))
            )
            for p in products
        ]

        sql = """
        INSERT INTO products (id, name, url_key, price, description, images_url)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            url_key = EXCLUDED.url_key,
            price = EXCLUDED.price,
            description = EXCLUDED.description,
            images_url = EXCLUDED.images_url,
            updated_at = CURRENT_TIMESTAMP;
        """

        # 🔥 fastest way
        execute_values(self.cur, sql, rows)
        self.conn.commit()

        return len(products)

    def close(self):
        self.cur.close()
        self.conn.close()
