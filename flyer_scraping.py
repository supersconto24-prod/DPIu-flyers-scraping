#!/usr/bin/env python3
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
import sys
import logging
from datetime import datetime

# Environment configuration
CHROME_DRIVER_PATH = "/usr/local/bin/chromedriver"
LOG_DIR = "logs"
OUTPUT_DIR = "scrape_data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "PAM_flyers.csv")  # Fixed output filename
INPUT_CSV = "scrape_data/store_PAM_with_ids.csv"

def setup_logging():
    """Configure comprehensive logging system"""
    os.makedirs(LOG_DIR, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(LOG_DIR, f"scraper_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger()

def setup_driver():
    """Initialize a headless Chrome WebDriver."""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    service = Service(CHROME_DRIVER_PATH)
    return webdriver.Chrome(service=service, options=options)

def process_store(driver, store_id, store_url, logger):
    """Process a single store URL and return flyer data"""
    result = {
        'shop_id': store_id,
        'store_url': store_url,
        'pdf_links': [],
        'status': 'No flyers'
    }
    logger.info(f"Starting processing for store {store_id}")
    
    try:
        # Navigate to the store page
        logger.debug(f"Loading store URL: {store_url}")
        driver.get(store_url)
        time.sleep(3)  # Wait for page to load

        # Find all flyer links in the carousel section
        flyer_links = []
        try:
            carousel = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".CardCarousel.notStandalone")))
            links = carousel.find_elements(By.CSS_SELECTOR, "a[href^='/volantini/']")
            flyer_links = [link.get_attribute("href") for link in links]
            logger.info(f"Found {len(flyer_links)} flyers for store {store_id}")
        except Exception as e:
            logger.warning(f"Could not find flyer carousel for store {store_id}: {str(e)}")
            return result

        # If no flyers found, return with empty pdf_links
        if not flyer_links:
            return result

        # Process each flyer for this store
        pdf_links = []
        for flyer_url in flyer_links:
            try:
                logger.debug(f"Processing flyer: {flyer_url}")
                driver.get(flyer_url)
                time.sleep(2)  # Wait for page to load
                
                # Try to find PDF links
                # First format
                try:
                    download_div = driver.find_element(By.CSS_SELECTOR, ".downloadFlyerContainer")
                    pdf_link = download_div.find_element(By.TAG_NAME, "a").get_attribute("href")
                    if pdf_link and pdf_link.lower().endswith('.pdf'):
                        pdf_links.append(pdf_link)
                        logger.debug(f"Found PDF (format 1): {pdf_link}")
                except Exception as e:
                    logger.debug(f"Format 1 not found: {str(e)}")
                    pass
                
                # Second format
                try:
                    pdf_elements = driver.find_elements(By.CSS_SELECTOR, "li.stats a[target='_blank']")
                    for element in pdf_elements:
                        href = element.get_attribute("href")
                        if href and href.lower().endswith('.pdf'):
                            pdf_links.append(href)
                            logger.debug(f"Found PDF (format 2): {href}")
                except Exception as e:
                    logger.debug(f"Format 2 not found: {str(e)}")
                    pass
                
            except Exception as e:
                logger.error(f"Error processing flyer {flyer_url} for store {store_id}: {str(e)}")
                continue

        # Update result based on findings
        if pdf_links:
            result['pdf_links'] = pdf_links
            result['status'] = 'success'
            
    except Exception as e:
        logger.error(f"Error processing store {store_id}: {str(e)}")
        result['status'] = 'error'
    
    logger.info(f"Completed processing for store {store_id}")
    return result

def save_results(results, logger):
    """Save results to the fixed output file with error handling"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    try:
        results_df = pd.DataFrame(results)
        
        # Write to CSV with error handling
        try:
            results_df.to_csv(OUTPUT_FILE, index=False)
            logger.info(f"Successfully saved {len(results_df)} records to {OUTPUT_FILE}")
        except Exception as e:
            logger.error(f"Failed to write CSV file {OUTPUT_FILE}: {str(e)}")
            
            # Attempt to save with timestamp if fixed filename fails
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                fallback_file = os.path.join(OUTPUT_DIR, f"PAM_flyers_{timestamp}.csv")
                results_df.to_csv(fallback_file, index=False)
                logger.info(f"Saved results to fallback file {fallback_file}")
            except Exception as fallback_e:
                logger.critical(f"Failed to save fallback CSV file: {str(fallback_e)}")
                
                # As last resort, log the results data
                logger.info("Results data that failed to save:")
                for i, row in enumerate(results):
                    logger.info(f"Row {i}: {str(row)}")
    
    except Exception as e:
        logger.error(f"Failed to create DataFrame from results: {str(e)}")
        logger.info("Raw results data:")
        logger.info(str(results))

def main():
    logger = setup_logging()
    logger.info("Starting scraper")
    
    try:
        # Load the store data with error handling
        try:
            logger.info(f"Loading input data from {INPUT_CSV}")
            data = pd.read_csv(INPUT_CSV)
            data = data.dropna(subset=['Shop ID', 'Store URL'])
            logger.info(f"Loaded {len(data)} valid store records")
        except Exception as e:
            logger.error(f"Failed to load input CSV {INPUT_CSV}: {str(e)}")
            raise
        
        # Initialize driver
        driver = setup_driver()
        all_results = []
        
        try:
            # Process each store
            for index, row in data.iterrows():
                store_id = row['Shop ID']
                store_url = row['Store URL']
                
                try:
                    store_result = process_store(driver, store_id, store_url, logger)
                    all_results.append(store_result)
                    
                    # Periodic save every 10 stores
                    if index % 10 == 0 and index > 0:
                        save_results(all_results, logger)
                        
                except Exception as e:
                    logger.error(f"Unexpected error processing store {store_id}: {str(e)}")
                    all_results.append({
                        'shop_id': store_id,
                        'store_url': store_url,
                        'pdf_links': [],
                        'status': 'error'
                    })
                
        finally:
            # Ensure driver quits even if error occurs
            driver.quit()
            logger.info("WebDriver closed")
        
        # Final save
        save_results(all_results, logger)
        
    except Exception as e:
        logger.critical(f"Fatal error in main execution: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()