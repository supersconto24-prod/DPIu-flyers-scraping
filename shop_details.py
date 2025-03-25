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
    """Configure logging to both file and console"""
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
    """Initialize and configure Chrome WebDriver for Linux"""
    try:
        options = Options()
        
        # Linux-specific options
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        
        # Headless mode with new implementation
        options.add_argument("--headless=new")
        
        # Additional options to mimic real browser
        options.add_argument("--disable-gpu")
        options.add_argument("--remote-debugging-port=9222")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        options.add_argument("--disable-blink-features=AutomationControlled")
        
        # Path to chromedriver
        service = Service('/usr/local/bin/chromedriver')
        
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
        
        # Alternative pattern matching
        match = re.search(r'!3d([-+]?\d+\.\d+)!4d([-+]?\d+\.\d+)', url)
        if match:
            return match.group(1), match.group(2)
            
        return 'N/A', 'N/A'
    except Exception as e:
        logger.warning(f"Failed to extract coordinates from {url} - {str(e)}")
        return 'N/A', 'N/A'

def extract_store_details(driver, url, index):
    """Extract store details from the store page"""
    try:
        logger.info(f"Processing store #{index}: {url}")
        driver.get(url)
        
        # Take debug screenshot
        driver.save_screenshot(f'{DEBUG_DIR}/page_load_{index}.png')
        
        # Wait for page to load
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'h1.title')))
        time.sleep(2)  # Additional delay for stability

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

        # Extract coordinates - main logic
        latitude, longitude = 'N/A', 'N/A'
        try:
            # Try to find and click the maps link
            maps_link = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'span.mapsLink, a.mapsLink')))
            
            # Take screenshot of the element
            maps_link.screenshot(f'{DEBUG_DIR}/maps_link_{index}.png')
            
            if maps_link:
                # Get direct URL if available
                if maps_link.tag_name == 'a':
                    maps_url = maps_link.get_attribute('href')
                    if maps_url and ('maps.google.com' in maps_url or 'goo.gl/maps' in maps_url):
                        latitude, longitude = extract_coordinates(maps_url)
                    else:
                        # If not a direct maps link, try clicking
                        original_window = driver.current_window_handle
                        driver.execute_script("arguments[0].click();", maps_link)
                        time.sleep(3)
                        
                        # Check if new tab opened
                        if len(driver.window_handles) > 1:
                            driver.switch_to.window(driver.window_handles[-1])
                            WebDriverWait(driver, 10).until(
                                lambda d: 'maps.google.com' in d.current_url.lower())
                            maps_url = driver.current_url
                            latitude, longitude = extract_coordinates(maps_url)
                            driver.close()
                            driver.switch_to.window(original_window)
                        else:
                            # Check current URL for maps
                            WebDriverWait(driver, 10).until(
                                lambda d: 'maps.google.com' in d.current_url.lower())
                            maps_url = driver.current_url
                            latitude, longitude = extract_coordinates(maps_url)
                            
                            # Navigate back to original page
                            driver.back()
                            WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, 'h1.title')))
        except Exception as e:
            logger.warning(f"Primary map extraction failed for {url}: {str(e)}")
            # Fallback method
            try:
                driver.get(f"{url}/map")
                WebDriverWait(driver, 10).until(
                    lambda d: 'maps.google.com' in d.current_url.lower())
                maps_url = driver.current_url
                latitude, longitude = extract_coordinates(maps_url)
            except Exception as fallback_e:
                logger.warning(f"Fallback map extraction also failed for {url}: {str(fallback_e)}")

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
            store_data = extract_store_details(driver, store_url, index)
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