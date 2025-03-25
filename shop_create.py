import requests
import pandas as pd
import json

# Load the CSV file
df = pd.read_csv("scrape_data/pam_details_fully_geocoded.csv")

# Add a new column to store the _id
df["Shop ID"] = None

# API endpoint and headers
url = "http://localhost:5055/api/shop/admin/create"
headers = {
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI2NWJlOGYzOWNlZWFmNjNkMDAxZGVlNjEiLCJuYW1lIjoiQWRtaW4iLCJlbWFpbCI6ImFkbWluQGdtYWlsLmNvbSIsInJvbGUiOiJBZG1pbiIsImFkZHJlc3MiOiIzNzQvQiBIYWxveWEgSGluZGFnYWxhIFBlcmFkZW5peWEiLCJwaG9uZSI6IjM2MC05NDMtNzMzMiIsImltYWdlIjoiaHR0cHM6Ly9pLmliYi5jby9XcE01eVpaLzkucG5nIiwiaWF0IjoxNzQyOTE0ODY4LCJleHAiOjE3NDMwODc2Njh9.qJs7HVht6laDMyKfQ0LsfnMdqvJlLPaakdTJQTbsg4M"
}

# Loop through the DataFrame and create shops
for index, row in df.iterrows():
    try:
        # Prepare the request body
        shop_data = {
            "shopName": row["Formatted Address"],
            "brand": "PAM",
            "customizedName": f"PAM {row['locality']}",
            "telephone":"N/A", 
            "website": row['Store Link'],
            "description": "N/A",
            "location": {
                "address": row["Formatted Address"],
                "city": row["locality"],
                "cityCode": row["locality"],
                "country": row["Country"],
                "countryCode": row["Country Code"],
                "administrativeOne": row["Administrative Area Level 1"],
                "administrativeTwo": row["Administrative Area Level 2"],
                "administrativeThree": row["Administrative Area Level 3"],
                "street": str(row["Street Number"]),  # Ensure street is a string
                "route": row["Formatted Address"].split(",")[0],
                "postalCode": str(int(row["Postal Code"]))  # Ensure postal code is a string
            },
            "coordinates": [row["Longitude"], row["Latitude"]],
            "role": "",
            "vendor": "",
            "shopLogo": "",
            "status": "ACTIVE",
            "isOnlineSelling": True,
            "isDelete": False
        }

        # Wrap the shop data in a "data" field and stringify it
        payload = {
            "data": json.dumps(shop_data)
        }

        # Make the POST request with JSON data
        response = requests.post(url, headers=headers, json=payload)

        # Check the response
        if response.status_code == 200:
            # Parse the response JSON
            response_data = response.json()

            # Extract the _id from the response
            shop_id = response_data["data"]["value"]["_id"]

            # Save the _id to the DataFrame
            df.at[index, "Shop ID"] = shop_id

            print(f"Shop {index + 1} created successfully. Shop ID: {shop_id}")
        else:
            print(f"Failed to create shop {index + 1}. Status code: {response.status_code}, Response: {response.text}")

    except Exception as e:
        print(f"Failed to create shop {index + 1}. Error: {e}")

# Save the updated DataFrame to a new CSV file
df.to_csv("store_PAM_geocoded_details_with_ids.csv", index=False)
print("Shop IDs saved to store_geocoded_details_with_ids.csv")