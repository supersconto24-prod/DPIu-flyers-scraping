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
        main_window = driver.current_window_handle
        
        # Try multiple ways to find the maps link
        maps_link = None
        selectors = [
            'span.mapsLink', 
            'a.mapsLink',
            'a[href*="maps.google.com"]',
            'a[href*="goo.gl/maps"]',
            '//span[contains(text(), "vieni a trovarci")]'  # XPath alternative
        ]
        
        for selector in selectors:
            try:
                if selector.startswith('//'):
                    maps_link = driver.find_element(By.XPATH, selector)
                else:
                    maps_link = driver.find_element(By.CSS_SELECTOR, selector)
                break
            except:
                continue
        
        if not maps_link:
            logger.warning("No maps link found with any selector")
            return 'N/A', 'N/A'
        
        # Take screenshot before interaction
        maps_link.screenshot(f'{DEBUG_DIR}/before_click_{index}.png')
        
        # Get href if it's a direct link
        if maps_link.tag_name == 'a':
            maps_url = maps_link.get_attribute('href')
            if maps_url and ('maps.google.com' in maps_url or 'goo.gl/maps' in maps_url):
                logger.info(f"Found direct maps URL: {maps_url}")
                return extract_coordinates(maps_url)
        
        # If not a direct link, try clicking
        logger.info("Attempting to click maps link...")
        original_url = driver.current_url
        driver.execute_script("arguments[0].scrollIntoView();", maps_link)
        driver.execute_script("arguments[0].click();", maps_link)
        time.sleep(5)  # Increased wait time
        
        # Check if URL changed in current tab
        if driver.current_url != original_url:
            if 'maps.google.com' in driver.current_url.lower():
                maps_url = driver.current_url
                driver.back()  # Return to original page
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'h1.title')))
                return extract_coordinates(maps_url)
        
        # Check for new tab
        if len(driver.window_handles) > 1:
            new_window = [w for w in driver.window_handles if w != main_window][0]
            driver.switch_to.window(new_window)
            
            try:
                WebDriverWait(driver, 15).until(
                    lambda d: 'maps.google.com' in d.current_url.lower())
                
                maps_url = driver.current_url
                logger.info(f"New tab maps URL: {maps_url}")
                
                # Take screenshot of maps page
                driver.save_screenshot(f'{DEBUG_DIR}/maps_page_{index}.png')
                
                coordinates = extract_coordinates(maps_url)
                driver.close()
                driver.switch_to.window(main_window)
                return coordinates
            except Exception as e:
                logger.warning(f"Failed to process new tab: {str(e)}")
                if len(driver.window_handles) > 1:
                    driver.close()
                driver.switch_to.window(main_window)
                return 'N/A', 'N/A'
        
        # Final fallback - try to construct maps URL from address
        logger.info("Attempting fallback method using address")
        try:
            address_section = driver.find_element(By.CSS_SELECTOR, 'div.StoreInfoNewSection.indirizzo')
            address = ', '.join([item.text.strip() for item in 
                               address_section.find_elements(By.CSS_SELECTOR, 'li.addressListItem')])
            
            if address:
                maps_url = f"https://www.google.com/maps/search/?api=1&query={address}"
                logger.info(f"Constructed maps URL: {maps_url}")
                return extract_coordinates(maps_url)
        except Exception as e:
            logger.warning(f"Fallback method failed: {str(e)}")
        
        return 'N/A', 'N/A'
        
    except Exception as e:
        logger.error(f"Critical error in handle_maps_link: {str(e)}")
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