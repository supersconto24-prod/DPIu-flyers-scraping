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
        'Formatted Address', 'locality', 'Country', 'Country Code',
        'Administrative Area Level 1', 'Administrative Area Level 2',
        'Postal Code', 'Longitude', 'Latitude'
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
            "shopName": row["Formatted Address"],
            "brand": "PAM",
            "customizedName": f"PAM {row['locality']}",
            "telephone": "N/A", 
            "website": row.get('Store Link', ''),
            "description": "N/A",
            "location": {
                "address": row["Formatted Address"],
                "city": row["locality"],
                "cityCode": row["locality"],
                "country": row["Country"],
                "countryCode": row["Country Code"],
                "administrativeOne": row["Administrative Area Level 1"],
                "administrativeTwo": row["Administrative Area Level 2"],
                "administrativeThree": row.get("Administrative Area Level 3", ""),
                "street": str(row.get("Street Number", "")),
                "route": row["Formatted Address"].split(",")[0],
                "postalCode": str(int(row["Postal Code"])) if not pd.isna(row["Postal Code"]) else ""
            },
            "coordinates": [float(row["Longitude"]), float(row["Latitude"])],
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
        headers = {"Authorization": "Bearer YOUR_TOKEN_HERE"}

        success = failure = 0
        for index, row in df.iterrows():
            try:
                logger.info(f"Processing record {index+1}/{len(df)}: {row['Formatted Address']}")
                
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
        output_file = "store_PAM_with_ids.csv"
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