import requests
import pandas as pd
import json
import logging
from datetime import datetime
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('shop_creation.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def setup_logging():
    """Configure logging settings"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'shop_creation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
            logging.StreamHandler()
        ]
    )

def validate_data(row):
    """Validate required fields in the row data"""
    required_fields = [
        'Formatted Address', 'locality', 'Country', 'Country Code',
        'Administrative Area Level 1', 'Administrative Area Level 2',
        'Postal Code', 'Longitude', 'Latitude'
    ]
    
    for field in required_fields:
        if pd.isna(row.get(field)):
            logger.warning(f"Missing required field: {field}")
            return False
    return True

def create_shop_payload(row):
    """Create the shop payload from row data"""
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
            "role": "",
            "vendor": "",
            "shopLogo": "",
            "status": "ACTIVE",
            "isOnlineSelling": True,
            "isDelete": False
        }
    except Exception as e:
        logger.error(f"Error creating payload: {str(e)}")
        raise

def main():
    setup_logging()
    logger.info("Starting shop creation process")
    
    try:
        # Load the CSV file
        df = pd.read_csv("scrape_data/pam_details_fully_geocoded.csv")
        logger.info(f"Loaded CSV with {len(df)} records")
        
        # Add a new column to store the _id
        df["Shop ID"] = None

        # API configuration
        url = "http://localhost:5055/api/shop/admin/create"
        headers = {
            "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI2NWJlOGYzOWNlZWFmNjNkMDAxZGVlNjEiLCJuYW1lIjoiQWRtaW4iLCJlbWFpbCI6ImFkbWluQGdtYWlsLmNvbSIsInJvbGUiOiJBZG1pbiIsImFkZHJlc3MiOiIzNzQvQiBIYWxveWEgSGluZGFnYWxhIFBlcmFkZW5peWEiLCJwaG9uZSI6IjM2MC05NDMtNzMzMiIsImltYWdlIjoiaHR0cHM6Ly9pLmliYi5jby9XcE01eVpaLzkucG5nIiwiaWF0IjoxNzQyOTE0ODY4LCJleHAiOjE3NDMwODc2Njh9.qJs7HVht6laDMyKfQ0LsfnMdqvJlLPaakdTJQTbsg4M"
        }

        success_count = 0
        failure_count = 0

        for index, row in df.iterrows():
            try:
                logger.info(f"Processing record {index + 1}/{len(df)}")
                
                if not validate_data(row):
                    logger.warning(f"Skipping record {index + 1} due to missing data")
                    failure_count += 1
                    continue

                shop_data = create_shop_payload(row)
                payload = {"data": json.dumps(shop_data)}

                # Log the request being sent
                logger.debug(f"Sending request for shop: {shop_data['shopName']}")

                response = requests.post(url, headers=headers, json=payload, timeout=30)
                
                if response.status_code == 200:
                    response_data = response.json()
                    shop_id = response_data["data"]["value"]["_id"]
                    df.at[index, "Shop ID"] = shop_id
                    success_count += 1
                    logger.info(f"Successfully created shop {index + 1}. ID: {shop_id}")
                else:
                    failure_count += 1
                    logger.error(
                        f"Failed to create shop {index + 1}. "
                        f"Status: {response.status_code}, Response: {response.text}"
                    )

            except requests.exceptions.RequestException as e:
                failure_count += 1
                logger.error(f"Request failed for shop {index + 1}: {str(e)}")
            except Exception as e:
                failure_count += 1
                logger.error(f"Unexpected error processing shop {index + 1}: {str(e)}", exc_info=True)

        # Save results
        output_file = "store_PAM_geocoded_details_with_ids.csv"
        df.to_csv(output_file, index=False)
        logger.info(f"Process completed. Success: {success_count}, Failures: {failure_count}")
        logger.info(f"Results saved to {output_file}")

    except Exception as e:
        logger.critical(f"Fatal error in main process: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()