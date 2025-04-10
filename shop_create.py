import requests
import pandas as pd
import json
import logging
from datetime import datetime
import sys
import argparse

# Configure argument parsing
parser = argparse.ArgumentParser()
parser.add_argument('--log-level', default='INFO', 
                    choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                    help='Set the logging level')
args = parser.parse_args()

# Configure logging
log_filename = f'shop_creation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=getattr(logging, args.log_level),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def validate_data(row):
    """Validate required fields"""
    required_fields = [
        'formatted_address', 'locality',
    ]
    missing = [f for f in required_fields if pd.isna(row.get(f))]
    if missing:
        logger.warning(f"Missing fields: {', '.join(missing)}")
        return False
    return True

def create_shop_payload(row):
    """Create API payload from row data"""
    try:
        return {
            "shopName": row["formatted_address"],
            "brand": "PAM",
            "customizedName": f"PAM {row['locality']}",
            "telephone": "N/A", 
            "website": row.get('Store URL', ''),
            "description": "N/A",
            "location": {
                "address": row["formatted_address"],
                "city": row["locality"],
                "cityCode": row["locality"],
                "country": row["country"],
                "countryCode": row["country_code"],
                "administrativeOne": row["admin_area_1"],
                "administrativeTwo": row["admin_area_2"],
                "administrativeThree": row.get("admin_area_3", ""),
                "street": str(row.get("street_number", "")),
                "route": row["formatted_address"].split(",")[0],
                "postalCode": str(int(row["postal_code"])) if not pd.isna(row["postal_code"]) else ""
            },
            "coordinates": [float(row["longitude"]), float(row["latitude"])],
            "status": "ACTIVE",
            "isOnlineSelling": True,
            "isDelete": False
        }
    except Exception as e:
        logger.error(f"Payload creation failed: {str(e)}", exc_info=True)
        raise

def main():
    try:
        logger.info("=== Starting shop creation process ===")
        
        # Load data
        df = pd.read_csv("scrape_data/pam_details_fully_geocoded.csv")
        logger.info(f"Loaded {len(df)} records from CSV")
        
        df["Shop ID"] = None
        url = "http://localhost:5055/api/shop/admin/create"
        headers = {"Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI2NWJlOGYzOWNlZWFmNjNkMDAxZGVlNjEiLCJuYW1lIjoiQWRtaW4iLCJlbWFpbCI6ImFkbWluQGdtYWlsLmNvbSIsInJvbGUiOiJBZG1pbiIsImFkZHJlc3MiOiIzNzQvQiBIYWxveWEgSGluZGFnYWxhIFBlcmFkZW5peWEiLCJwaG9uZSI6IjM2MC05NDMtNzMzMiIsImltYWdlIjoiaHR0cHM6Ly9pLmliYi5jby9XcE01eVpaLzkucG5nIiwiaWF0IjoxNzQyOTE0ODY4LCJleHAiOjE3NDMwODc2Njh9.qJs7HVht6laDMyKfQ0LsfnMdqvJlLPaakdTJQTbsg4M"}

        success = failure = 0
        for index, row in df.iterrows():
            try:
                logger.info(f"Processing record {index+1}/{len(df)}: {row['formatted_address']}")
                
                if not validate_data(row):
                    logger.error(f"Skipping invalid record {index+1}")
                    failure += 1
                    continue

                payload = {"data": json.dumps(create_shop_payload(row))}
                logger.debug(f"Payload: {json.dumps(payload, indent=2)}")

                response = requests.post(url, headers=headers, json=payload, timeout=60)
                if response.status_code == 200:
                    shop_id = response.json()["data"]["value"]["_id"]
                    df.at[index, "Shop ID"] = shop_id
                    success += 1
                    logger.info(f"Created shop ID: {shop_id}")
                else:
                    failure += 1
                    logger.error(f"API Error: {response.status_code} - {response.text}")

            except Exception as e:
                failure += 1
                logger.error(f"Record {index+1} failed: {str(e)}", exc_info=True)

        # Save results
        output_file = "scrape_data/store_PAM_with_ids.csv"
        df.to_csv(output_file, index=False)
        logger.info(f"=== Completed ===")
        logger.info(f"Success: {success}, Failed: {failure}")
        logger.info(f"Results saved to {output_file}")
        logger.info(f"Detailed log: {log_filename}")

    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()