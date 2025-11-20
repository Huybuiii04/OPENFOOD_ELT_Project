import os
import aiohttp
import asyncio
import logging
import io
import csv
import sys

# Fix import path
sys.path.insert(0, os.path.dirname(__file__))

from checkpoint import load_checkpoint, save_checkpoint
from uploadS3 import upload_to_s3


BUCKET_NAME = "raw-food-project"
PREFIX = "bronze/"

BASE_URL = "https://world.openfoodfacts.org/api/v2/search?page={}&page_size=100"
TOTAL_PAGES = 1000
CONCURRENCY = 10  # 10 concurrent - balanced for rate limiting
MAX_ROW_PER_FILE = 10000
REQUEST_DELAY = 0.5  # 0.5 second delay between requests to spread load


write_lock = asyncio.Lock()
SEMAPHORE = asyncio.Semaphore(CONCURRENCY)
last_request_time = 0

async def fetch_page(session, page):
    global last_request_time
    
    # Rate limiting: ensure at least REQUEST_DELAY seconds between requests
    now = asyncio.get_event_loop().time()
    sleep_time = max(0, REQUEST_DELAY - (now - last_request_time))
    if sleep_time > 0:
        await asyncio.sleep(sleep_time)
    last_request_time = asyncio.get_event_loop().time()
    
    url = BASE_URL.format(page)
    timeout = aiohttp.ClientTimeout(total=180)  # 180s timeout for heavily throttled responses
    for attempt in range(8):  # 8 attempts for heavy rate limiting
        try:
            async with session.get(url, timeout=timeout) as res:
                if res.status == 200:
                    data = await res.json()
                    logging.info(f"Page {page} fetched successfully")
                    return data.get("products", [])
                elif res.status == 429:  # Rate limit
                    logging.warning(f"Page {page} rate limited (429), attempt {attempt+1}/8")
                    if attempt < 7:
                        wait_time = 10 * (2 ** attempt)  # 10s, 20s, 40s, 80s, 160s, 320s, 640s
                        logging.info(f"Rate limited - waiting {wait_time}s")
                        await asyncio.sleep(wait_time)
                else:
                    logging.warning(f"Page {page} HTTP {res.status}, attempt {attempt+1}/8")
                    if attempt < 7:
                        await asyncio.sleep(5 * (2 ** attempt))
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            logging.warning(f"Page {page} {type(e).__name__}, attempt {attempt+1}/8")
            if attempt < 7:
                wait_time = 15 * (2 ** attempt)  # 15s, 30s, 60s, 120s, 240s...
                logging.info(f"Waiting {wait_time}s before retry")
                await asyncio.sleep(wait_time)
        except Exception as e:
            logging.warning(f"Page {page} {type(e).__name__}: {str(e)}, attempt {attempt+1}/8")
            if attempt < 7:
                await asyncio.sleep(5 * (2 ** attempt))
    
    logging.error(f"Failed to fetch page {page} after 8 attempts")
    return []

async def write_rows(rows , writer):
    async with asyncio.Lock():
        for row in rows :
            writer.writerow(row)
            

async def process_page(session , page , writer):
    async with SEMAPHORE :
        products = await fetch_page(session , page)
        rows = []
        
        for p in products :
            rows.append([
                p.get("id" , ""),
                p.get("code" , ""),
                p.get("product_name", ""),
                p.get("brands" , ""),
                p.get("countries" , ""),
                p.get("categories" , ""),
                p.get("ingredients_text" , ""),
                p.get("nutriscore_grade" , ""),
                p.get("nutriments" , {}).get("energy_100g" , ""),
                p.get("nutriments" , {}).get("sugars_100g" , ""),
                
            ])
        await write_rows(rows , writer)
        return len(rows)

async def crawl_async():
        start_page = load_checkpoint()
        logging.info(f"Resuming from page {start_page}")
        
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        
        writer.writerow([
            "id", "code", "product_name", "brands", "countries",
            "categories", "ingredients_text", "nutriscore_grade",
            "energy_100g", "sugars_100g"
        ])
        
        row_count = 0
        batch_number = 1
        current_page = start_page
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            
            for page in range(start_page, TOTAL_PAGES + 1):
                
                tasks.append(process_page(session, page, writer))
                current_page = page
                
                if len(tasks) >= CONCURRENCY:
                    result = await asyncio.gather(*tasks)
                    rows_in_batch = sum(result)
                    row_count += rows_in_batch
                    tasks = []
                    logging.info(f"Processed up to page {page} with {row_count} total rows")
                    
                    # Save checkpoint after successful page batch
                    save_checkpoint(page)
                    
                    # Upload when reaching 10k rows
                    if row_count > MAX_ROW_PER_FILE:
                        s3_key = f"{PREFIX}product_part_{batch_number}.csv"
                        
                        try:
                            upload_to_s3(
                                BUCKET_NAME=BUCKET_NAME,
                                KEY=s3_key,
                                string_data=buffer.getvalue()
                            )
                            logging.info(f"Uploaded {s3_key} to S3with {row_count} rows")
                        except Exception as e:
                            logging.warning(f"Failed to upload {s3_key}: {str(e)}")
                        
                        # Reset buffer and writer
                        batch_number += 1
                        row_count = 0
                        buffer = io.StringIO()
                        writer = csv.writer(buffer)
                        
                        writer.writerow([
                            "id", "code", "product_name", "brands", "countries",
                            "categories", "ingredients_text", "nutriscore_grade",
                            "energy_100g", "sugars_100g"
                        ])
            
            # Process remaining tasks
            if tasks:
                result = await asyncio.gather(*tasks)
                rows_in_batch = sum(result)
                row_count += rows_in_batch
                logging.info(f"Processed final batch up to page {current_page} with {row_count} total rows")
                save_checkpoint(current_page)
            
            # Upload remaining data
            if row_count > 0:
                s3_key = f"{PREFIX}product_part_{batch_number}.csv"
                try:
                    upload_to_s3(
                        BUCKET_NAME=BUCKET_NAME,
                        KEY=s3_key,
                        string_data=buffer.getvalue()
                    )
                    logging.info(f"Uploaded final batch {s3_key} to S3 with {row_count} rows")
                except Exception as e:
                    logging.warning(f"Failed to upload final batch {s3_key}: {str(e)}")


def run_crawl():
    asyncio.run(crawl_async())
                
        


                