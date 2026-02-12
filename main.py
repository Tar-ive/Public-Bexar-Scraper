from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.firefox.options import Options
from time import sleep, time
import csv
import os
import random
import json
from datetime import datetime
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# CONFIGURATION
# =============================================================================
OUTPUT_FILE = "deed_2026_v4.csv"
STATE_FILE = "scraper_state.json"
URL_TEMPLATE = "https://bexar.tx.publicsearch.us/results?department=RP&docTypes=DEED&limit=250&recordedDateRange={start_date}%2C{end_date}&searchType=advancedSearch&sort=desc&sortBy=recordedDate"

START_DATE_FIXED = "18000101"
DEFAULT_END_DATE = "20260121"
WINDOW_YEARS = 10

# Environment
HEADLESS = os.environ.get("HEADLESS", "false").lower() == "true"
IS_GITHUB_ACTIONS = os.environ.get("GITHUB_ACTIONS", "false").lower() == "true"
DATABASE_URL = os.environ.get("DATABASE_URL")

# Proxy
PROXY_HOST = "127.0.0.1"
PROXY_PORT = "9050"
USE_PROXY = False  # Disabled: Tor exit nodes being blocked

# Delays
MIN_DELAY = 3
MAX_DELAY = 7
BREAK_EVERY_N_PAGES = 50
BREAK_DURATION_MIN = 60
BREAK_DURATION_MAX = 180

# Runtime limits (3 hours targeted)
# 3 hours = 180 mins. Conservative 1 min/page = 180 pages.
MAX_PAGES_PER_SESSION = 180 if IS_GITHUB_ACTIONS else 1000
RECORDS_PER_PAGE = 250
OFFSET_LIMIT = 9500 
BATCH_SIZE = 1000

# =============================================================================
# COLUMN MAPPING
# =============================================================================
COLUMN_MAP = {
    'col-3': 'Grantor',
    'col-4': 'Grantee',
    'col-5': 'Doc_Type',
    'col-6': 'Recorded_Date',
    'col-7': 'Doc_Number',
    'col-8': 'Book_Volume_Page',
    'col-9': 'Legal_Description',
    'col-10': 'Lot',
    'col-11': 'Block',
    'col-12': 'NCB',
    'col-13': 'County_Block',
    'col-14': 'Property_Address'
}

FIELDNAMES = [
    'Grantor', 'Grantee', 'Doc_Type', 'Recorded_Date', 'Doc_Number',
    'Book_Volume_Page', 'Legal_Description', 'Lot', 'Block', 'NCB',
    'County_Block', 'Property_Address'
]

# =============================================================================
# DATABASE FUNCTIONS
# =============================================================================

def init_db():
    if not DATABASE_URL:
        print("⚠️ No DATABASE_URL found. Skipping DB init.")
        return
        
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS land_records (
                id SERIAL PRIMARY KEY,
                doc_number TEXT UNIQUE,
                grantor TEXT,
                grantee TEXT,
                doc_type TEXT,
                recorded_date DATE,
                book_volume_page TEXT,
                legal_description TEXT,
                lot TEXT,
                block TEXT,
                ncb TEXT,
                county_block TEXT,
                property_address TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("✓ Database initialized.")
    except Exception as e:
        print(f"❌ DB Init failed: {e}")

def get_db_stats():
    """Returns (count, min_date) from DB"""
    if not DATABASE_URL:
        return 0, None
        
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM land_records;")
        count = cur.fetchone()[0]
        
        cur.execute("SELECT MIN(recorded_date) FROM land_records;")
        min_date = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        
        # Format date straight away if exists
        formatted_date = None
        if min_date:
            formatted_date = min_date.strftime("%Y%m%d")
            
        return count, formatted_date
    except Exception as e:
        print(f"❌ DB Stats failed: {e}")
        return 0, None

def batch_push_to_db(records):
    if not DATABASE_URL or not records:
        return

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        args_list = []
        for r in records:
            r_date = None
            try:
                if r.get('Recorded_Date'):
                    r_date = datetime.strptime(r['Recorded_Date'], "%m/%d/%Y").strftime("%Y-%m-%d")
            except: pass
            
            args_list.append((
                r.get('Doc_Number'), r.get('Grantor'), r.get('Grantee'), r.get('Doc_Type'),
                r_date, r.get('Book_Volume_Page'), r.get('Legal_Description'),
                r.get('Lot'), r.get('Block'), r.get('NCB'), r.get('County_Block'), r.get('Property_Address')
            ))
            
        query = """
            INSERT INTO land_records 
            (doc_number, grantor, grantee, doc_type, recorded_date, book_volume_page, legal_description, lot, block, ncb, county_block, property_address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (doc_number) DO NOTHING;
        """
        cur.executemany(query, args_list)
        conn.commit()
        cur.close()
        conn.close()
        print(f"🚀 Synced {len(records)} records to DB.")
    except Exception as e:
        print(f"❌ DB Push failed: {e}")

# =============================================================================
# STATE & HELPERS
# =============================================================================

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
        except: pass
    return {'current_end_date': DEFAULT_END_DATE, 'current_offset': 0}

def save_state(end_date, offset):
    state = {'current_end_date': end_date, 'current_offset': offset, 'last_updated': datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

MIN_START_DATE = datetime.strptime(START_DATE_FIXED, "%Y%m%d")


def random_delay():
    sleep(random.uniform(MIN_DELAY, MAX_DELAY))


def get_start_date(end_date_str: str) -> str:
    try:
        end_date = datetime.strptime(end_date_str, "%Y%m%d")
    except ValueError:
        return START_DATE_FIXED

    target_year = max(end_date.year - WINDOW_YEARS, MIN_START_DATE.year)
    try:
        candidate = end_date.replace(year=target_year)
    except ValueError:
        candidate = end_date.replace(year=target_year, day=28)

    if candidate < MIN_START_DATE:
        candidate = MIN_START_DATE

    return candidate.strftime("%Y%m%d")

def take_break():
    duration = random.uniform(BREAK_DURATION_MIN, BREAK_DURATION_MAX)
    print(f"\n☕ Break: {int(duration)}s...")
    sleep(duration)

def create_driver():
    options = Options()
    if HEADLESS or IS_GITHUB_ACTIONS:
        options.add_argument("--headless")
    options.set_preference("general.useragent.override", "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0")
    if USE_PROXY:
        options.set_preference("network.proxy.type", 1)
        options.set_preference("network.proxy.socks", PROXY_HOST)
        options.set_preference("network.proxy.socks_port", int(PROXY_PORT))
        options.set_preference("network.proxy.socks_version", 5)
        options.set_preference("network.proxy.socks_remote_dns", True)
    driver = webdriver.Firefox(options=options)
    driver.set_page_load_timeout(120)
    return driver

def extract_row_data(row):
    data = {}
    for col_class, field in COLUMN_MAP.items():
        try:
            cell = row.find_element(By.CSS_SELECTOR, f"td.{col_class}, td[class*='{col_class}']")
            data[field] = cell.text.strip()
        except: data[field] = ""
    return data

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("="*60)
    print("Bexar Scraper: DB-Driven Mode")
    print("="*60)
    
    init_db()
    
    state = load_state()
    current_end_date = state.get('current_end_date', DEFAULT_END_DATE)
    current_offset = state.get('current_offset', 0)
    
    # 1. Check DB State for Window Logic
    db_count, oldest_db_date = get_db_stats()
    print(f"DB Records: {db_count:,}")
    if oldest_db_date:
        print(f"Oldest Record: {oldest_db_date}")
    
    # Force slide if DB has > 9500 records and we haven't already slid past the default
    if db_count >= OFFSET_LIMIT:
        print("\n🛑 DB record count > 9500. Checking Window Slide...")
        if oldest_db_date and oldest_db_date != DEFAULT_END_DATE:
            # If current_end_date is "newer" than oldest_db_date, it means we haven't slid down yet
            # Or if we just want to ensure we are always chasing the oldest date
            if current_end_date > oldest_db_date:
                print(f"🔄 Sliding Window: {current_end_date} -> {oldest_db_date}")
                current_end_date = oldest_db_date
                current_offset = 0 # Reset offset when window moves
                save_state(current_end_date, current_offset)
            else:
                print("✓ Window already aligned with oldest DB date.")
    
    page_num = (current_offset // RECORDS_PER_PAGE) + 1
    start_date = get_start_date(current_end_date)
    url = URL_TEMPLATE.format(start_date=start_date, end_date=current_end_date) + f"&offset={current_offset}"
    
    print(f"📅 Active Window: {start_date} to {current_end_date}")
    print(f"📄 Start Page: {page_num} (Offset: {current_offset})")
    
    driver = create_driver()
    pages_session = 0
    batch_buffer = []
    
    try:
        driver.get(url)
        sleep(5)
        try:
            for btn in driver.find_elements(By.XPATH, "//button[contains(text(), 'Accept') or contains(text(), 'Close')]"):
                btn.click()
                sleep(1)
        except: pass
        
        while True:
            pages_session += 1
            if MAX_PAGES_PER_SESSION > 0 and pages_session > MAX_PAGES_PER_SESSION:
                print("⏸️  Session limit reached.")
                break
                
            print(f"\n--- Page {page_num} (Rel Offset: {current_offset}) ---")
            
            # Mid-session limit check (using offset as proxy for 10k limit)
            if current_offset >= OFFSET_LIMIT:
                print("🛑 Limit approaching mid-session.")
                # We stop here so next run performs the DB slide check
                save_state(current_end_date, current_offset)
                break

            if pages_session > 1 and (pages_session % BREAK_EVERY_N_PAGES == 0):
                take_break()
            
            try:
                WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.XPATH, "//table//tbody//tr")))
            except TimeoutException:
                print("⚠️  Timeout - Error/Limit?")
                page_text = driver.page_source
                page_text_lower = page_text.lower()
                if "limit" in page_text_lower or "error" in page_text_lower:
                    raw_snippet = " ".join(page_text.split())
                    snippet = raw_snippet[:400]
                    if len(raw_snippet) > 400:
                        snippet += "..."
                    print(f"🛑 Limit hit! Snippet: {snippet}")
                    # Save state at LIMIT so next run forces slide
                    save_state(current_end_date, OFFSET_LIMIT)
                    break
                driver.refresh()
                sleep(10)
                continue

            rows = driver.find_elements(By.XPATH, "//table//tbody//tr")
            if not rows:
                print("No rows found.")
                break
                
            count = 0
            for row in rows:
                try:
                    d = extract_row_data(row)
                    if d.get('Doc_Number'):
                        batch_buffer.append(d)
                        count += 1
                except: pass
            
            print(f"  ✓ +{count} records")
            
            # DB Push
            if len(batch_buffer) >= BATCH_SIZE:
                batch_push_to_db(batch_buffer)
                batch_buffer = []
            
            current_offset += RECORDS_PER_PAGE
            page_num += 1
            save_state(current_end_date, current_offset)
            
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                sleep(1)
                next_btn = None
                for sel in ["//button[contains(@aria-label, 'Next')]", "(//nav//button)[last()]"]:
                    btns = driver.find_elements(By.XPATH, sel)
                    if btns and btns[-1].is_enabled():
                        next_btn = btns[-1]
                        break
                if next_btn:
                    next_btn.click()
                    sleep(3)
                    random_delay()
                else: break
            except: break

    except KeyboardInterrupt:
        print("\n⚠️ Interrupted")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        if batch_buffer:
            batch_push_to_db(batch_buffer)
        
        save_state(current_end_date, current_offset)
        driver.quit()
        print("Done.")

if __name__ == "__main__":
    main()
