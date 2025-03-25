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
    results = []
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
            return [{
                'shop_id': store_id,
                'store_url': store_url,
                'flyer_url': "N/A",
                'pdf_links': f"No flyer carousel found: {str(e)}",
                'status': 'error'
            }]

        # Process each flyer for this store
        for flyer_url in flyer_links:
            try:
                logger.debug(f"Processing flyer: {flyer_url}")
                driver.get(flyer_url)
                time.sleep(2)  # Wait for page to load
                
                # Try to find PDF links
                pdf_links = []
                
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
                
                status = 'success' if pdf_links else 'no_pdf_found'
                results.append({
                    'shop_id': store_id,
                    'store_url': store_url,
                    'flyer_url': flyer_url,
                    'pdf_links': pdf_links if pdf_links else "No PDF found",
                    'status': status
                })
                logger.info(f"Processed flyer {flyer_url} - Status: {status}")
                
            except Exception as e:
                logger.error(f"Error processing flyer {flyer_url} for store {store_id}: {str(e)}")
                results.append({
                    'shop_id': store_id,
                    'store_url': store_url,
                    'flyer_url': flyer_url,
                    'pdf_links': f"Error: {str(e)}",
                    'status': 'error'
                })
                
    except Exception as e:
        logger.error(f"Error processing store {store_id}: {str(e)}")
        results.append({
            'shop_id': store_id,
            'store_url': store_url,
            'flyer_url': "N/A",
            'pdf_links': f"Store page error: {str(e)}",
            'status': 'error'
        })
    
    logger.info(f"Completed processing for store {store_id}")
    return results

def main():
    logger = setup_logging()
    logger.info("Starting scraper")
    
    try:
        # Load the store data
        logger.info(f"Loading input data from {INPUT_CSV}")
        data = pd.read_csv(INPUT_CSV)
        data = data.dropna(subset=['Shop ID', 'Store URL'])
        logger.info(f"Loaded {len(data)} valid store records")
        
        # Initialize driver
        driver = setup_driver()
        all_results = []
        
        try:
            # Process each store
            for index, row in data.iterrows():
                store_id = row['Shop ID']
                store_url = row['Store URL']
                
                store_results = process_store(driver, store_id, store_url, logger)
                all_results.extend(store_results)
                
                # Periodic save
                if index % 10 == 0 and index > 0:
                    save_results(all_results, logger)
                
        finally:
            # Ensure driver quits even if error occurs
            driver.quit()
            logger.info("WebDriver closed")
        
        # Final save
        save_results(all_results, logger)
        
    except Exception as e:
        logger.critical(f"Fatal error in main execution: {str(e)}", exc_info=True)
        raise

def save_results(results, logger):
    """Save results to CSV with timestamp"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(OUTPUT_DIR, f"flyer_results_{timestamp}.csv")
    
    results_df = pd.DataFrame(results)
    results_df.to_csv(output_file, index=False)
    logger.info(f"Saved {len(results_df)} records to {output_file}")

if __name__ == "__main__":
    main()