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
INPUT_CSV = "pam.csv"
OUTPUT_CSV = "pam_details_with_coordinates.csv"
LOG_FILE = "scraper.log"
OUTPUT_DIR = "scrape_data"

# Set up directories
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Set up logging
def setup_logging():
    """Configure logging to both file and console"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # File handler
    file_handler = logging.FileHandler(os.path.join(OUTPUT_DIR, LOG_FILE))
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

def setup_driver():
    """Initialize and configure Chrome WebDriver for Linux"""
    try:
        options = Options()
        
        # Linux-specific options
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        
        # Headless mode - uncomment for production
        # options.add_argument("--headless")
        
        # Path to chromedriver - update this for your Linux system
        service = Service('/usr/bin/chromedriver')
        
        driver = webdriver.Chrome(service=service, options=options)
        return driver
    except Exception as e:
        logger.error(f"Failed to initialize WebDriver: {str(e)}")
        sys.exit(1)

def extract_coordinates(url):
    """Extract latitude and longitude from Google Maps URL"""
    try:
        match = re.search(r'destination=([-+]?\d+\.\d+),([-+]?\d+\.\d+)', url)
        if match:
            return match.group(1), match.group(2)
        return 'N/A', 'N/A'
    except Exception as e:
        logger.warning(f"Failed to extract coordinates from {url} - {str(e)}")
        return 'N/A', 'N/A'

def extract_store_details(driver, url):
    """Extract store details from the store page"""
    try:
        logger.info(f"Processing store URL: {url}")
        driver.get(url)
        
        # Wait for page to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'h1.title')))
        time.sleep(1)  # Additional small delay

        # Extract store name
        store_name = driver.find_element(By.CSS_SELECTOR, 'h1.title').text.strip()
        
        # Extract address
        address = 'N/A'
        try:
            address_section = driver.find_element(By.CSS_SELECTOR, 'div.StoreInfoNewSection.indirizzo')
            address_items = address_section.find_elements(By.CSS_SELECTOR, 'li.addressListItem')
            address = ', '.join([item.text.strip() for item in address_items])
        except Exception as e:
            logger.warning(f"Address extraction failed for {url}: {str(e)}")

        # Extract contact information
        contact = 'N/A'
        try:
            contact_section = driver.find_element(By.CSS_SELECTOR, 'div.StoreInfoNewSection.contatti')
            contact_items = contact_section.find_elements(By.CSS_SELECTOR, 'li.contactListItem')
            contact = ', '.join([item.text.strip() for item in contact_items])
        except Exception as e:
            logger.warning(f"Contact extraction failed for {url}: {str(e)}")

        # Extract coordinates
        latitude, longitude = 'N/A', 'N/A'
        try:
            maps_link = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'span.mapsLink')))
            
            if maps_link:
                # Get href if it's a direct link
                if maps_link.tag_name == 'a':
                    maps_url = maps_link.get_attribute('href')
                    if maps_url:
                        latitude, longitude = extract_coordinates(maps_url)
                else:
                    # Click the element if it's not a direct link
                    original_window = driver.current_window_handle
                    driver.execute_script("arguments[0].click();", maps_link)
                    time.sleep(2)
                    
                    if len(driver.window_handles) > 1:
                        driver.switch_to.window(driver.window_handles[-1])
                        maps_url = driver.current_url
                        latitude, longitude = extract_coordinates(maps_url)
                        driver.close()
                        driver.switch_to.window(original_window)
                    else:
                        maps_url = driver.current_url
                        latitude, longitude = extract_coordinates(maps_url)
        except Exception as e:
            logger.warning(f"Failed to extract coordinates for {url}: {str(e)}")

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
    
    # Initialize WebDriver
    driver = setup_driver()
    
    try:
        # Load input CSV
        try:
            df = pd.read_csv(INPUT_CSV)
            logger.info(f"Loaded {len(df)} store URLs from {INPUT_CSV}")
        except Exception as e:
            logger.error(f"Failed to load input CSV: {str(e)}")
            return

        # Process each store
        results = []
        for index, row in df.iterrows():
            store_url = row['Store URL']
            store_data = extract_store_details(driver, store_url)
            if store_data:
                results.append(store_data)
            
            # Save progress periodically
            if (index + 1) % 5 == 0:
                temp_df = pd.DataFrame(results)
                temp_df.to_csv(os.path.join(OUTPUT_DIR, f"temp_{OUTPUT_CSV}"), index=False)
                logger.info(f"Saved temporary results after {index + 1} stores")

        # Save final results
        result_df = pd.DataFrame(results)
        result_df.to_csv(os.path.join(OUTPUT_DIR, OUTPUT_CSV), index=False)
        logger.info(f"Saved final results to {OUTPUT_CSV} ({len(result_df)} stores)")

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