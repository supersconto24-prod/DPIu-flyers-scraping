#!/usr/bin/env python3
import os
import pandas as pd
import time
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
COMUNI_CSV = "comuni.csv"  # CSV file containing city names in 'Comune' column
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
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        service = Service(CHROME_DRIVER_PATH)
        driver = webdriver.Chrome(service=service, options=options)
        
        # Set timeouts for better stability
        driver.set_page_load_timeout(30)
        driver.set_script_timeout(30)
        
        return driver
    except Exception as e:
        logger.error(f"Failed to initialize WebDriver: {str(e)}")
        raise

def accept_cookies(driver):
    """Handle cookie acceptance banner if present"""
    try:
        # First wait for the cookie banner container
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.iubenda-cs-content"))
        )
        
        # Then look for the accept button
        accept_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.iubenda-cs-accept-btn"))
        )
        accept_button.click()
        logger.info("Accepted cookies")
        time.sleep(2)  # Small delay after accepting
    except Exception as e:
        logger.debug("Cookie banner not found or could not be accepted")
        pass  # Continue if cookie banner not present

def scrape_city(driver, city):
    """Scrape stores for a single city"""
    stores = []
    try:
        logger.info(f"Processing city: {city}")
        
        # Locate the input field
        input_field = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, 'search-input-6c01972'))
        )
        input_field.clear()
        input_field.send_keys(city)
        
        # Wait for results with flexible conditions
        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.find_elements(By.CSS_SELECTOR, 'div.jet-ajax-search__results-item') or 
                          d.find_elements(By.CSS_SELECTOR, 'div.jet-ajax-search__no-results')
            )
            
            # Check for no results first
            no_results = driver.find_elements(By.CSS_SELECTOR, 'div.jet-ajax-search__no-results')
            if no_results:
                logger.info(f"No results found for {city}")
                return []
            
            # Parse the store list
            store_items = driver.find_elements(By.CSS_SELECTOR, 'div.jet-ajax-search__results-item')
            logger.info(f"Found {len(store_items)} stores for {city}")
            
            for store in store_items:
                try:
                    shop_name = store.find_element(By.CSS_SELECTOR, 'div.jet-search-title-fields__item-value').text
                    link = store.find_element(By.CSS_SELECTOR, 'a.jet-ajax-search__item-link').get_attribute('href')
                    address = store.find_element(By.CSS_SELECTOR, 'div.jet-ajax-search__item-content').text
                    
                    stores.append({
                        'City': city,
                        'Shop Name': shop_name,
                        'Link': link,
                        'Address': address
                    })
                    
                except Exception as e:
                    logger.warning(f"Failed to extract store information for {city}: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.info(f"No results found for {city} (timeout)")
            return []
            
    except Exception as e:
        logger.error(f"Search failed for {city}: {str(e)}")
        return []
    
    return stores

def main():
    all_stores = []
    driver = None
    
    try:
        logger.info("Starting D-Piu store scraper")
        
        # Load comuni data
        try:
            comuni_df = pd.read_csv(COMUNI_CSV, encoding="iso-8859-1")
            cities = comuni_df['Comune'].unique().tolist()
            logger.info(f"Loaded {len(cities)} cities from {COMUNI_CSV}")
        except Exception as e:
            logger.error(f"Failed to load comuni data: {str(e)}")
            raise

        # Initialize WebDriver
        driver = setup_driver()
        
        # Open the website
        driver.get('https://www.d-piu.com/dpiu-locator/')
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, 'body'))
        )
        time.sleep(2)
        
        # Handle cookie acceptance
        accept_cookies(driver)
        
        # Process each city sequentially
        for city in cities:
            city_stores = scrape_city(driver, city)
            all_stores.extend(city_stores)
            
            # Small delay between cities
            time.sleep(2)
            
        # Save all results
        if all_stores:
            output_file = os.path.join(OUTPUT_DIR, DPI_CSV)
            df = pd.DataFrame(all_stores)
            
            # Atomic write operation
            output_tmp = output_file + '.tmp'
            df.to_csv(output_tmp, index=False, encoding='utf-8')
            os.replace(output_tmp, output_file)
            
            logger.info(f"Saved data for {len(all_stores)} stores to {output_file}")
        else:
            logger.warning("No store data was collected")
            
    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")
    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                logger.warning(f"Error closing driver: {str(e)}")
        logging.shutdown()

if __name__ == '__main__':
    main()
