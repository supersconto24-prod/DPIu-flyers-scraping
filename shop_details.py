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
        logger.info(f"Main window handle: {main_window}")
        
        # Take screenshot before any interaction
        driver.save_screenshot(f'{DEBUG_DIR}/pre_click_{index}.png')
        
        # Find maps link with multiple selector attempts
        maps_link = None
        selectors = [
            'span.mapsLink', 
            'a.mapsLink',
            'a[href*="maps"]',
            '//*[contains(text(), "vieni a trovarci")]'
        ]
        
        for selector in selectors:
            try:
                if selector.startswith('//'):
                    maps_link = driver.find_element(By.XPATH, selector)
                else:
                    maps_link = driver.find_element(By.CSS_SELECTOR, selector)
                logger.info(f"Found maps link using selector: {selector}")
                break
            except Exception as e:
                logger.debug(f"Selector failed {selector}: {str(e)}")
                continue
        
        if not maps_link:
            logger.warning("No maps link found with any selector")
            return 'N/A', 'N/A'
        
        # Take screenshot of located element
        maps_link.screenshot(f'{DEBUG_DIR}/maps_link_{index}.png')
        
        # Get direct URL if available
        if maps_link.tag_name == 'a':
            maps_url = maps_link.get_attribute('href')
            if maps_url and ('maps.google.com' in maps_url or 'goo.gl/maps' in maps_url):
                logger.info(f"Direct maps URL found: {maps_url}")
                return extract_coordinates(maps_url)
        
        # Prepare for click interaction
        original_url = driver.current_url
        logger.info(f"Original URL: {original_url}")
        
        # Scroll and click using JavaScript
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", maps_link)
        time.sleep(1)
        driver.execute_script("arguments[0].click();", maps_link)
        logger.info("Maps link clicked")
        time.sleep(5)  # Wait for any navigation
        
        # Check all possible outcomes
        if len(driver.window_handles) > 1:
            # New tab opened
            logger.info(f"New tab detected. Window handles: {driver.window_handles}")
            new_window = [w for w in driver.window_handles if w != main_window][0]
            
            try:
                driver.switch_to.window(new_window)
                logger.info(f"Switched to new window: {driver.current_url}")
                
                # Wait for maps to load with multiple checks
                WebDriverWait(driver, 15).until(
                    lambda d: 'maps.google.com' in d.current_url.lower() or 
                             'google.com/maps' in d.current_url.lower())
                
                maps_url = driver.current_url
                logger.info(f"Maps URL in new tab: {maps_url}")
                driver.save_screenshot(f'{DEBUG_DIR}/maps_tab_{index}.png')
                
                coordinates = extract_coordinates(maps_url)
                
                # Close tab and return to main window
                driver.close()
                driver.switch_to.window(main_window)
                return coordinates
            except Exception as e:
                logger.error(f"Failed to process new tab: {str(e)}")
                try:
                    if len(driver.window_handles) > 1:
                        driver.close()
                    driver.switch_to.window(main_window)
                except:
                    pass
                return 'N/A', 'N/A'
        elif 'maps.google.com' in driver.current_url.lower():
            # Same tab navigation
            maps_url = driver.current_url
            logger.info(f"Maps URL in same tab: {maps_url}")
            driver.save_screenshot(f'{DEBUG_DIR}/maps_same_tab_{index}.png')
            
            # Return to original page
            driver.back()
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'h1.title')))
            return extract_coordinates(maps_url)
        else:
            # No apparent navigation occurred
            logger.warning("No navigation detected after click")
            driver.save_screenshot(f'{DEBUG_DIR}/no_nav_{index}.png')
            return 'N/A', 'N/A'
            
    except Exception as e:
        logger.error(f"Critical error in handle_maps_link: {str(e)}")
        try:
            # Try to recover browser state
            if len(driver.window_handles) > 1:
                driver.switch_to.window(main_window)
        except:
            pass
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