import os
import pandas as pd
import time
import multiprocessing
import logging
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
LOG_FILE = "pam_details.log"
OUTPUT_DIR = "scrape_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)
CHROME_DRIVER_PATH = "/usr/local/bin/chromedriver"

# Set up logging
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(os.path.join(OUTPUT_DIR, LOG_FILE)),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

def setup_driver():
    """Initialize a headless Chrome WebDriver for each process."""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    service = Service(CHROME_DRIVER_PATH)
    return webdriver.Chrome(service=service, options=options)

def extract_coordinates(url):
    """Extract latitude and longitude from Google Maps URL"""
    try:
        match = re.search(r'destination=([-+]?\d+\.\d+),([-+]?\d+\.\d+)', url)
        if match:
            return match.group(1), match.group(2)
        return 'N/A', 'N/A'
    except Exception as e:
        logger.warning(f"Coordinates extraction failed: {str(e)}")
        return 'N/A', 'N/A'

def extract_store_details(url):
    """Extract detailed information from a single store URL including coordinates."""
    driver = setup_driver()
    try:
        logger.debug(f"Processing store URL: {url}")
        driver.get(url)
        time.sleep(2)  # Reduced wait time

        # Extract store name
        store_name = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'h1.title')))
        store_name = store_name.text.strip() if store_name else 'N/A'

        # Extract address
        address_section = driver.find_elements(By.CSS_SELECTOR, 'div.StoreInfoNewSection.indirizzo')
        address = 'N/A'
        if address_section:
            try:
                address_items = address_section[0].find_elements(By.CSS_SELECTOR, 'li.addressListItem')
                address = ', '.join([item.text.strip() for item in address_items])
            except Exception as e:
                logger.warning(f"Address extraction failed for {url}: {str(e)}")

        # Extract contact information
        contact_section = driver.find_elements(By.CSS_SELECTOR, 'div.StoreInfoNewSection.contatti')
        contact = 'N/A'
        if contact_section:
            try:
                contact_items = contact_section[0].find_elements(By.CSS_SELECTOR, 'li.contactListItem')
                contact = ', '.join([item.text.strip() for item in contact_items])
            except Exception as e:
                logger.warning(f"Contact extraction failed for {url}: {str(e)}")

        # Extract coordinates from maps link
        latitude, longitude = 'N/A', 'N/A'
        try:
            maps_link = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'span.mapsLink')))
            
            if maps_link:
                # Get the href attribute if it's an <a> tag, otherwise click it
                if maps_link.tag_name == 'a':
                    maps_url = maps_link.get_attribute('href')
                    if maps_url:
                        latitude, longitude = extract_coordinates(maps_url)
                else:
                    # Click the element to open maps (might open in new tab)
                    original_window = driver.current_window_handle
                    driver.execute_script("arguments[0].click();", maps_link)
                    time.sleep(2)  # Wait for maps to load
                    
                    # Check if new tab opened
                    if len(driver.window_handles) > 1:
                        driver.switch_to.window(driver.window_handles[-1])
                        maps_url = driver.current_url
                        latitude, longitude = extract_coordinates(maps_url)
                        driver.close()
                        driver.switch_to.window(original_window)
                    else:
                        # If no new tab, use current URL
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
        return {
            'Store Name': 'N/A',
            'Address': 'N/A',
            'Contact': 'N/A',
            'Latitude': 'N/A',
            'Longitude': 'N/A',
            'Store URL': url
        }
    finally:
        driver.quit()

def process_batch(urls):
    """Process a batch of URLs and return their details."""
    return [extract_store_details(url) for url in urls]

def main():
    logger.info("=== Starting Store Details Extraction ===")
    
    # Load and preprocess input data
    try:
        df = pd.read_csv(INPUT_CSV)
        logger.info(f"Loaded {len(df)} store URLs from {INPUT_CSV}")
        
        # Remove duplicates before processing
        initial_count = len(df)
        df.drop_duplicates(subset=['Store URL'], keep='first', inplace=True)
        logger.info(f"Removed {initial_count - len(df)} duplicate URLs")
        
        urls = df['Store URL'].tolist()
    except Exception as e:
        logger.error(f"Failed to load input data: {str(e)}")
        return

    # Configure multiprocessing
    num_processes = 4
    batch_size = 10
    results = []
    
    try:
        logger.info(f"Starting processing with {num_processes} processes")
        
        # Split URLs into batches for processing
        url_batches = [urls[i:i + batch_size] for i in range(0, len(urls), batch_size)]
        
        with multiprocessing.Pool(processes=num_processes) as pool:
            for i, batch_result in enumerate(pool.imap(process_batch, url_batches)):
                results.extend(batch_result)
                logger.info(f"Processed batch {i+1}/{len(url_batches)} - {len(batch_result)} stores")
                
                # Save progress periodically
                if (i + 1) % 5 == 0:
                    temp_df = pd.DataFrame(results)
                    temp_df.to_csv(os.path.join(OUTPUT_DIR, f"temp_{OUTPUT_CSV}"), index=False)
                    logger.info(f"Saved temporary results ({len(results)} stores)")

    except Exception as e:
        logger.error(f"Processing failed: {str(e)}")
    finally:
        # Save final results
        result_df = pd.DataFrame(results)
        
        # Remove any duplicates that might have occurred
        final_count = len(result_df)
        result_df.drop_duplicates(subset=['Store URL'], keep='first', inplace=True)
        logger.info(f"Removed {final_count - len(result_df)} duplicate results")
        
        result_df.to_csv(os.path.join(OUTPUT_DIR, OUTPUT_CSV), index=False)
        logger.info(f"Saved final results to {OUTPUT_CSV} ({len(result_df)} stores)")
        logger.info("=== Extraction completed ===")

if __name__ == '__main__':
    main()