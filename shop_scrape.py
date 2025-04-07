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

# Configuration - 10 Major Italian Cities
MAJOR_CITIES = [
    "Milano",       # Milan
    "Roma",         # Rome
    "Napoli",       # Naples
    "Torino",       # Turin
    "Palermo",      # Palermo
    "Genova",       # Genoa
    "Bologna",      # Bologna
    "Firenze",      # Florence
    "Bari",         # Bari
    "Livorno"       # Livorno
]

DPI_CSV = "dpiu_stores.csv"
OUTPUT_DIR = "scrape_data"
TEMP_DIR = "temp_data"
LOG_DIR = "logs"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)
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

def save_temp_results(city, stores):
    """Save temporary results for a city."""
    if not stores:
        return False
        
    try:
        temp_file = os.path.join(TEMP_DIR, f"temp_{city}_shops.csv")
        df = pd.DataFrame(stores)
        df.to_csv(temp_file, index=False, encoding='utf-8')
        logger.info(f"Saved temporary results for {city} to {temp_file}")
        return True
    except Exception as e:
        logger.error(f"Failed to save temporary results for {city}: {str(e)}")
        return False

def merge_temp_files(output_file):
    """Merge all temporary files into the final output."""
    try:
        # Get all temporary files
        temp_files = [f for f in os.listdir(TEMP_DIR) if f.startswith('temp_') and f.endswith('.csv')]
        
        if not temp_files:
            logger.warning("No temporary files found to merge")
            return False
            
        # Read and concatenate all temp files
        dfs = []
        for temp_file in temp_files:
            try:
                df = pd.read_csv(os.path.join(TEMP_DIR, temp_file))
                dfs.append(df)
                logger.info(f"Loaded data from {temp_file}")
            except Exception as e:
                logger.error(f"Failed to load {temp_file}: {str(e)}")
                continue
                
        if not dfs:
            logger.error("No valid data found in temporary files")
            return False
            
        final_df = pd.concat(dfs, ignore_index=True)
        final_df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"Merged {len(dfs)} temporary files into {output_file}")
        
        # Clean up temp files
        for temp_file in temp_files:
            try:
                os.remove(os.path.join(TEMP_DIR, temp_file))
            except Exception as e:
                logger.error(f"Failed to remove {temp_file}: {str(e)}")
                
        return True
        
    except Exception as e:
        logger.error(f"Failed to merge temporary files: {str(e)}")
        return False

def scrape_dpiu_stores(city):
    """Scrape D-Piu stores for a specific city and save temporary results."""
    driver = None
    stores = []
    
    try:
        driver = setup_driver()
        logger.info(f"Processing city: {city}")
        
        # Open the website
        driver.get('https://www.d-piu.com/dpiu-locator/')
        time.sleep(3)  # Wait for the page to load

        # Locate the input field
        input_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, 'search-input-6c01972'))
        )
        input_field.clear()
        input_field.send_keys(city)
        time.sleep(3)  # Wait for results

        try:
            # Check if results are present
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.jet-ajax-search__results-slide'))
            )
            
            # Parse the store list
            store_items = driver.find_elements(By.CSS_SELECTOR, 'div.jet-ajax-search__results-item')
            logger.info(f"Found {len(store_items)} stores for {city}")

            for store in store_items:
                try:
                    # Extract shop name
                    shop_name = store.find_element(By.CSS_SELECTOR, 'div.jet-search-title-fields__item-value').text
                    
                    # Extract link
                    link = store.find_element(By.CSS_SELECTOR, 'a.jet-ajax-search__item-link').get_attribute('href')
                    
                    # Extract address
                    address = store.find_element(By.CSS_SELECTOR, 'div.jet-ajax-search__item-content').text
                    
                    stores.append({
                        'City': city,
                        'Shop Name': shop_name,
                        'Link': link,
                        'Address': address
                    })
                    
                except Exception as e:
                    logger.error(f"Failed to extract store information for {city}: {str(e)}")
                    continue

        except Exception as e:
            logger.info(f"No results found for {city}")
            return False  # No results found

    except Exception as e:
        logger.error(f"Error processing {city}: {str(e)}")
        return False  # Processing failed

    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                logger.error(f"Error closing driver for {city}: {str(e)}")

        # Save temporary results if we found any stores
        if stores:
            return save_temp_results(city, stores)
        return False

def main():
    try:
        logger.info("Starting D-Piu store scraper for major Italian cities")
        
        output_file = os.path.join(OUTPUT_DIR, DPI_CSV)

        # Clear existing output and temp files
        if os.path.exists(output_file):
            try:
                os.remove(output_file)
                logger.info(f"Removed existing output file: {output_file}")
            except Exception as e:
                logger.error(f"Failed to remove existing output file: {str(e)}")
                raise

        # Clear temp directory
        for f in os.listdir(TEMP_DIR):
            try:
                os.remove(os.path.join(TEMP_DIR, f))
            except Exception as e:
                logger.error(f"Failed to remove temp file {f}: {str(e)}")

        # Use multiprocessing with 4 workers
        logger.info(f"Starting scraping process for {len(MAJOR_CITIES)} major cities with 4 workers")
        with multiprocessing.Pool(processes=4) as pool:
            results = pool.map(scrape_dpiu_stores, MAJOR_CITIES)
            
        successful_scrapes = sum(1 for result in results if result)
        logger.info(f"Completed scraping. Successfully processed {successful_scrapes} of {len(MAJOR_CITIES)} cities")

        # Merge all temporary files into final output
        if not merge_temp_files(output_file):
            logger.error("Failed to merge temporary files into final output")
            raise Exception("Failed to merge results")

        logger.info(f"Scraping completed. Final data saved to {output_file}")

    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")
        raise
    finally:
        logging.shutdown()

if __name__ == '__main__':
    main()
