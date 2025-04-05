#!/usr/bin/env python3
import os
import pandas as pd
import time
import multiprocessing
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

# Configuration
COMUNI_CSV = "comuni.csv"
DPI_CSV = "dpiu_stores.csv"
OUTPUT_DIR = "scrape_data"
LOG_DIR = "logs"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
CHROME_DRIVER_PATH = "/usr/local/bin/chromedriver"

# Setup logging
def setup_logging():
    log_filename = os.path.join(LOG_DIR, f"scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # File handler with rotation (10MB per file, keep 5 backups)
    file_handler = RotatingFileHandler(
        log_filename, 
        maxBytes=10*1024*1024, 
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    ))
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    ))
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

def setup_driver():
    """Initialize a headless Chrome WebDriver."""
    try:
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-gpu")
        options.add_argument("--remote-debugging-port=9222")
        
        service = Service(CHROME_DRIVER_PATH)
        driver = webdriver.Chrome(service=service, options=options)
        return driver
    except Exception as e:
        logger.error(f"Failed to initialize WebDriver: {str(e)}")
        raise

def scrape_dpiu_stores(comune, output_file):
    """Scrape D-Piu stores for a specific comune and append to CSV."""
    driver = None
    stores = []
    
    try:
        driver = setup_driver()
        logger.info(f"Processing comune: {comune}")
        
        # Open the website
        driver.get('https://www.d-piu.com/dpiu-locator/')
        time.sleep(3)  # Wait for the page to load

        # Locate the input field
        input_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, 'search-input-6c01972'))
        )
        input_field.clear()
        input_field.send_keys(comune)
        time.sleep(3)  # Wait for results

        try:
            # Check if results are present
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.jet-ajax-search__results-slide'))
            )
            
            # Parse the store list
            store_items = driver.find_elements(By.CSS_SELECTOR, 'div.jet-ajax-search__results-item')
            logger.info(f"Found {len(store_items)} stores for {comune}")

            for store in store_items:
                try:
                    # Extract shop name
                    shop_name = store.find_element(By.CSS_SELECTOR, 'div.jet-search-title-fields__item-value').text
                    
                    # Extract link
                    link = store.find_element(By.CSS_SELECTOR, 'a.jet-ajax-search__item-link').get_attribute('href')
                    
                    # Extract address
                    address = store.find_element(By.CSS_SELECTOR, 'div.jet-ajax-search__item-content').text
                    
                    stores.append({
                        'Comune': comune,
                        'Shop Name': shop_name,
                        'Link': link,
                        'Address': address
                    })
                    
                except Exception as e:
                    logger.error(f"Failed to extract store information for {comune}: {str(e)}")
                    continue

        except Exception as e:
            logger.info(f"No results found for {comune}")
            return  # Skip saving if no results

    except Exception as e:
        logger.error(f"Error processing {comune}: {str(e)}")

    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                logger.error(f"Error closing driver for {comune}: {str(e)}")

        # Append results to CSV after processing each comune
        if stores:
            try:
                df = pd.DataFrame(stores)
                file_exists = os.path.isfile(output_file)
                df.to_csv(output_file, mode='a', header=not file_exists, index=False, encoding='utf-8')
                logger.info(f"Successfully saved {len(stores)} stores for {comune}")
            except Exception as e:
                logger.error(f"Failed to save results for {comune}: {str(e)}")

def main():
    try:
        logger.info("Starting D-Piu store scraper")
        
        # Load comuni data
        try:
            comuni_df = pd.read_csv(COMUNI_CSV)
            comuni = comuni_df['Comune'].unique().tolist()
            logger.info(f"Loaded {len(comuni)} comuni from {COMUNI_CSV}")
        except Exception as e:
            logger.error(f"Failed to load comuni data: {str(e)}")
            raise

        output_file = os.path.join(OUTPUT_DIR, DPI_CSV)

        # Clear existing file if it exists
        if os.path.exists(output_file):
            try:
                os.remove(output_file)
                logger.info(f"Removed existing output file: {output_file}")
            except Exception as e:
                logger.error(f"Failed to remove existing output file: {str(e)}")
                raise

        # Use multiprocessing with a lock for file writing
        logger.info("Starting scraping process with 4 workers")
        with multiprocessing.Pool(processes=4) as pool:
            # Map each comune to the scrape function with the output file
            pool.starmap(scrape_dpiu_stores, [(comune, output_file) for comune in comuni])

        logger.info(f"Scraping completed. Final data saved to {output_file}")

    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")
    finally:
        logging.shutdown()

if __name__ == '__main__':
    main()
