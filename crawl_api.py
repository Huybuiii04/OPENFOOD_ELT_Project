import aiohttp
import asyncio
import json
import csv
import os
import logging
from aiohttp import ClientTimeout
from tqdm import tqdm


BASE_URL = "https://world.openfoodfacts.org/api/v2/search?page={}&page_size=100"

TOTAL_PAGES = 1000
CONCURRENCY = 50

OUTPUT_DIR = "output"
CHECKPOINT_FILE = "checkpoint.json"
ERROR_FILE = "errors.json"
LOG_FILE = "logs/crawler.log"

MAX_ROWS_PER_FILE = 10_000     # mỗi file 10k dòng


write_lock = asyncio.Lock()
SEMAPHORE = asyncio.Semaphore(CONCURRENCY)


# =========================================================
# SETUP LOGGING
# =========================================================
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    filename=LOG_FILE,
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logging.info("🔵 START CRAWLER")


# =========================================================
# LOAD CHECKPOINT
# =========================================================
def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            ck = json.load(f)
            logging.info(f"▶️ LOAD CHECKPOINT — last_page={ck.get('last_page')}")
            return ck
    return {"last_page": 0}


def save_checkpoint(last_page):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump({"last_page": last_page}, f, indent=2)
    logging.info(f"💾 SAVE CHECKPOINT — last_page={last_page}")


# =========================================================
# FETCH PAGE WITH RETRY + BACKOFF
# =========================================================
async def fetch_page(session, page: int):
    url = BASE_URL.format(page)
    retries = 0

    logging.info(f"🌐 Fetching page {page} — {url}")

    while retries < 5:
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("products", [])

                elif resp.status in (429, 500, 502, 503):
                    wait = 1 + retries * 2
                    logging.warning(f"⚠️ Page {page}: Status {resp.status}. Retry in {wait}s")
                    await asyncio.sleep(wait)
                    retries += 1

                else:
                    logging.error(f"❌ Page {page}: Unexpected status {resp.status}")
                    return []

        except Exception as e:
            wait = 1 + retries
            logging.error(f"🔥 Page {page}: Exception {e}. Retry in {wait}s")
            await asyncio.sleep(wait)
            retries += 1

    logging.error(f"🚫 Page {page}: Max retries exceeded")
    return []


# =========================================================
# WRITE CSV ROWS (THREAD-SAFE)
# =========================================================
async def write_rows_to_csv(rows, writer):
    async with write_lock:
        for row in rows:
            writer.writerow(row)


# =========================================================
# PROCESS 1 PAGE
# =========================================================
async def process_page(session, page: int, writer, error_pages):
    async with SEMAPHORE:
        products = await fetch_page(session, page)

        if not products:
            error_pages.append(page)
            logging.error(f"❌ Page {page}: No products returned")
            return

        rows = []
        for p in products:
            rows.append([
                p.get("id", ""),
                p.get("code", ""),
                p.get("product_name", ""),
                p.get("brands", ""),
                p.get("categories", ""),
                p.get("countries", ""),
                p.get("ingredients_text", ""),
                p.get("nutriscore_grade", ""),
                p.get("energy_100g", ""),
                p.get("sugars_100g", ""),
            ])

        await write_rows_to_csv(rows, writer)
        await asyncio.sleep(0.005)

        logging.info(f"✅ Page {page} written ({len(products)} products)")


# =========================================================
# OPEN NEW CSV FILE WITH HEADERS
# =========================================================
def open_new_csv_file(index):
    filename = os.path.join(OUTPUT_DIR, f"products_{index}.csv")
    f = open(filename, "w", encoding="utf-8", newline="")
    writer = csv.writer(f)
    writer.writerow([
        "id", "code", "name", "brands", "categories",
        "countries", "ingredients", "nutri_score",
        "energy_100g", "sugars_100g"
    ])

    logging.info(f"📄 OPEN NEW FILE: {filename}")
    return f, writer


# =========================================================
# MAIN
# =========================================================
async def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    checkpoint = load_checkpoint()
    start_page = checkpoint["last_page"] + 1

    timeout = ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)

    headers = {
        "User-Agent": "OpenFoodFactsCrawler - Laptop"
    }

    async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers) as session:

        error_pages = []
        tasks = []

        # file chia theo batch 10k rows
        file_index = 1
        current_rows = 0

        f, writer = open_new_csv_file(file_index)

        for page in tqdm(range(start_page, TOTAL_PAGES + 1)):

            # đủ 10k dòng → tạo file mới
            if current_rows >= MAX_ROWS_PER_FILE:
                logging.info(f"🔀 Split file — {current_rows} rows reached")
                f.close()
                file_index += 1
                f, writer = open_new_csv_file(file_index)
                current_rows = 0

            tasks.append(process_page(session, page, writer, error_pages))
            current_rows += 100

            if len(tasks) >= CONCURRENCY:
                await asyncio.gather(*tasks)
                tasks = []

            save_checkpoint(page)

        if tasks:
            await asyncio.gather(*tasks)

        f.close()

    # save error pages
    with open(ERROR_FILE, "w", encoding="utf-8") as f:
        json.dump(error_pages, f, indent=2)

    logging.info(f"⚠️ TOTAL ERROR PAGES: {len(error_pages)}")
    logging.info("🟢 DONE! All output saved")


if __name__ == "__main__":
    asyncio.run(main())
