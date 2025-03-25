import os
import pandas as pd
import requests
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging.handlers import RotatingFileHandler

# Configuration
INPUT_CSV = "scrape_data/pam_details.csv"
OUTPUT_CSV = "scrape_data/pam_details_geocode.csv"
LOG_FILE = "scrape_data/geocoding.log"
GOOGLE_API_KEY = "YOUR_API_KEY"  # Replace with your actual key
MAX_RETRIES = 3
RATE_LIMIT = 50  # requests per second (Google's limit)

# Set up proper logging
def setup_logger():
    """Configure rotating logs with detailed formatting"""
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    
    logger = logging.getLogger('geocoder')
    logger.setLevel(logging.DEBUG)
    
    # Rotating file handler (5MB per file, keep 3 backups)
    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=5*1024*1024,
        backupCount=3,
        encoding='utf-8'
    )
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s | %(module)s:%(lineno)d'
    ))
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s'
    ))
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

logger = setup_logger()

def geocode_address(address, api_key=GOOGLE_API_KEY, retry=0):
    """Robust geocoding with retry logic"""
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {'address': address, 'key': api_key}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data['status'] == 'OK':
            result = data['results'][0]
            location = result['geometry']['location']
            
            # Extract address components
            components = {
                c['types'][0]: c['long_name'] 
                for c in result['address_components']
                if c['types']
            }
            
            return {
                'formatted_address': result.get('formatted_address'),
                'latitude': location['lat'],
                'longitude': location['lng'],
                'street_number': components.get('street_number'),
                'route': components.get('route'),
                'locality': components.get('locality'),
                'postal_code': components.get('postal_code'),
                'administrative_area': components.get('administrative_area_level_1'),
                'country': components.get('country'),
                'place_id': result.get('place_id'),
                'geocode_status': 'SUCCESS'
            }
        else:
            error_msg = f"{data['status']}: {data.get('error_message', '')}"
            if retry < MAX_RETRIES and data['status'] in ['OVER_QUERY_LIMIT', 'UNKNOWN_ERROR']:
                wait_time = (2 ** retry) * 0.5  # Exponential backoff
                logger.warning(f"Retry {retry+1} for {address} in {wait_time}s...")
                time.sleep(wait_time)
                return geocode_address(address, api_key, retry+1)
            else:
                logger.warning(f"Geocode failed for {address}: {error_msg}")
                return {'geocode_status': f"FAILED: {error_msg}"}
            
    except Exception as e:
        logger.error(f"Geocode error for {address}: {str(e)}")
        if retry < MAX_RETRIES:
            time.sleep(1)
            return geocode_address(address, api_key, retry+1)
        return {'geocode_status': f"ERROR: {str(e)}"}

def process_batch(batch_df):
    """Process a batch of addresses with threading"""
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(
                geocode_address, 
                f"{row['Address']}, {row.get('Comune', '')}, Italy"
            ): idx for idx, row in batch_df.iterrows()
        }
        
        for future in as_completed(futures):
            idx = futures[future]
            try:
                result = future.result()
                combined = {**batch_df.iloc[idx].to_dict(), **result}
                results.append(combined)
            except Exception as e:
                logger.error(f"Processing failed for row {idx}: {str(e)}")
    
    return pd.DataFrame(results)

def main():
    try:
        logger.info("=== Starting Geocoding Process ===")
        
        # Load input data
        df = pd.read_csv(INPUT_CSV)
        logger.info(f"Loaded {len(df)} records from {INPUT_CSV}")
        
        # Process in batches
        batch_size = 100
        all_results = []
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} addresses)")
            
            batch_results = process_batch(batch)
            all_results.append(batch_results)
            
            # Save intermediate results
            pd.concat(all_results).to_csv(OUTPUT_CSV, index=False)
            logger.info(f"Saved {len(batch_results)} results from current batch")
            
            # Respect rate limits
            time.sleep(len(batch)/RATE_LIMIT)
        
        # Final save
        final_df = pd.concat(all_results)
        final_df.to_csv(OUTPUT_CSV, index=False)
        logger.info(f"Completed! Saved {len(final_df)} records to {OUTPUT_CSV}")
        
        # Report statistics
        success_rate = (final_df['geocode_status'] == 'SUCCESS').mean()
        logger.info(f"Success rate: {success_rate:.2%}")
        
    except Exception as e:
        logger.error(f"Fatal error in main process: {str(e)}", exc_info=True)
    finally:
        logger.info("=== Geocoding Process Completed ===")

if __name__ == '__main__':
    main()