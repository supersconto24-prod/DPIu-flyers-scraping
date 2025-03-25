#!/usr/bin/env python3

import os
import sys
import logging
import pandas as pd
import time
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Configuration
INPUT_CSV = "scrape_data/pam.csv"
OUTPUT_CSV = "pam_details_with_coordinates.csv"
LOG_FILE = "scraper.log"
OUTPUT_DIR = "scrape_data"
DEBUG_DIR = "debug_screenshots"

# Set up directories
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(DEBUG_DIR, exist_ok=True)

# Set up logging
def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler = logging.FileHandler(os.path.join(OUTPUT_DIR, LOG_FILE))
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

logger = setup_logging()

def setup_driver():
    try:
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--headless=new")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        
        service = Service('/usr/local/bin/chromedriver')
        driver = webdriver.Chrome(service=service, options=options)
        return driver
    except Exception as e:
        logger.error(f"Failed to initialize WebDriver: {str(e)}")
        sys.exit(1)

def extract_coordinates(url):
    try:
        # Try multiple patterns to extract coordinates
        patterns = [
            r'destination=([-+]?\d+\.\d+),([-+]?\d+\.\d+)',
            r'!3d([-+]?\d+\.\d+)!4d([-+]?\d+\.\d+)',
            r'@([-+]?\d+\.\d+),([-+]?\d+\.\d+)',
            r'&ll=([-+]?\d+\.\d+),([-+]?\d+\.\d+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1), match.group(2)
        return 'N/A', 'N/A'
    except Exception as e:
        logger.warning(f"Failed to extract coordinates from {url} - {str(e)}")
        return 'N/A', 'N/A'

def handle_maps_link(driver, index):
    try:
        # Get current window handle
        main_window = driver.current_window_handle
        
        # Find and click the maps link
        maps_link = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'span.mapsLink, a.mapsLink')))
        
        # Take screenshot before clicking
        # maps_link.screenshot(f'{DEBUG_DIR}/before_click_{index}.png')
        
        # Click using JavaScript to avoid interception issues
        driver.execute_script("arguments[0].click();", maps_link)
        time.sleep(3)  # Wait for new tab to open
        
        # Switch to new tab
        if len(driver.window_handles) > 1:
            new_window = [w for w in driver.window_handles if w != main_window][0]
            driver.switch_to.window(new_window)
            
            # Wait for maps to load
            WebDriverWait(driver, 15).until(
                lambda d: 'maps.google.com' in d.current_url.lower())
            
            # Take screenshot of maps page
            # driver.save_screenshot(f'{DEBUG_DIR}/maps_page_{index}.png')
            
            # Get URL and extract coordinates
            maps_url = driver.current_url
            logger.info(f"Maps URL: {maps_url}")
            
            # Close maps tab and switch back
            driver.close()
            driver.switch_to.window(main_window)
            return extract_coordinates(maps_url)
        
        # If no new tab opened, try current URL
        if 'maps.google.com' in driver.current_url.lower():
            maps_url = driver.current_url
            driver.back()  # Go back to original page
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'h1.title')))
            return extract_coordinates(maps_url)
            
        return 'N/A', 'N/A'
    except Exception as e:
        logger.warning(f"Failed to handle maps link: {str(e)}")
        return 'N/A', 'N/A'

def extract_store_details(driver, url, index):
    try:
        logger.info(f"Processing store #{index}: {url}")
        driver.get(url)
        # driver.save_screenshot(f'{DEBUG_DIR}/page_load_{index}.png')
        
        # Wait for page to load
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'h1.title')))
        time.sleep(2)

        # Extract basic info
        store_name = driver.find_element(By.CSS_SELECTOR, 'h1.title').text.strip()
        
        address = 'N/A'
        try:
            address_section = driver.find_element(By.CSS_SELECTOR, 'div.StoreInfoNewSection.indirizzo')
            address_items = address_section.find_elements(By.CSS_SELECTOR, 'li.addressListItem')
            address = ', '.join([item.text.strip() for item in address_items])
        except Exception as e:
            logger.warning(f"Address extraction failed: {str(e)}")

        contact = 'N/A'
        try:
            contact_section = driver.find_element(By.CSS_SELECTOR, 'div.StoreInfoNewSection.contatti')
            contact_items = contact_section.find_elements(By.CSS_SELECTOR, 'li.contactListItem')
            contact = ', '.join([item.text.strip() for item in contact_items])
        except Exception as e:
            logger.warning(f"Contact extraction failed: {str(e)}")

        # Handle maps link and coordinates
        latitude, longitude = handle_maps_link(driver, index)

        return {
            'Store Name': store_name,
            'Address': address,
            'Contact': contact,
            'Latitude': latitude,
            'Longitude': longitude,
            'Store URL': url
        }

    except Exception as e:
        logger.error(f"Failed to process {url}: {str(e)}")
        return None

def main():
    logger.info("=== Starting store details extraction ===")
    driver = setup_driver()
    
    try:
        df = pd.read_csv(INPUT_CSV)
        logger.info(f"Loaded {len(df)} store URLs")
        
        results = []
        for index, row in df.iterrows():
            store_data = extract_store_details(driver, row['Store URL'], index)
            if store_data:
                results.append(store_data)
            
            if (index + 1) % 5 == 0:
                pd.DataFrame(results).to_csv(os.path.join(OUTPUT_DIR, f"temp_{OUTPUT_CSV}"), index=False)
                logger.info(f"Saved progress after {index + 1} stores")

        pd.DataFrame(results).to_csv(os.path.join(OUTPUT_DIR, OUTPUT_CSV), index=False)
        logger.info(f"Saved final results with {len(results)} stores")

    except Exception as e:
        logger.error(f"Script failed: {str(e)}")
    finally:
        driver.quit()
        logger.info("=== Extraction completed ===")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)