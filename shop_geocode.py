import pandas as pd
import requests
from tqdm import tqdm
import time

# Load your data
df = pd.read_csv('scrape_data/pam_details_with_coordinates.csv')

# Google Maps API configuration
API_KEY = 'AIzaSyDlJN2hzy6E4SQTBTGNVJmc9oCe_TMR_XU'  # Replace with your actual API key
GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
DEFAULT_DELAY = 0.1  # seconds between API calls

def make_geocode_request(params):
    """Make request to Google Maps Geocoding API"""
    try:
        response = requests.get(GEOCODE_URL, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        return None

def extract_address_components(result):
    """Extract address components from API response"""
    components = {
        'formatted_address': result.get('formatted_address'),
        'street_number': None,
        'route': None,
        'locality': None,
        'admin_area_3': None,
        'admin_area_2': None,
        'admin_area_1': None,
        'country': None,
        'country_code': None,
        'postal_code': None,
        'latitude': result['geometry']['location']['lat'],
        'longitude': result['geometry']['location']['lng']
    }
    
    for component in result['address_components']:
        types = component['types']
        if 'street_number' in types:
            components['street_number'] = component['long_name']
        if 'route' in types:
            components['route'] = component['long_name']
        if 'locality' in types:
            components['locality'] = component['long_name']
        elif 'postal_town' in types:
            components['locality'] = component['long_name']
        if 'administrative_area_level_3' in types:
            components['admin_area_3'] = component['long_name']
        if 'administrative_area_level_2' in types:
            components['admin_area_2'] = component['long_name']
        if 'administrative_area_level_1' in types:
            components['admin_area_1'] = component['long_name']
        if 'country' in types:
            components['country'] = component['long_name']
            components['country_code'] = component['short_name']
        if 'postal_code' in types:
            components['postal_code'] = component['long_name']
    
    return components

def geocode_row(row):
    """Geocode a single row"""
    # Case 1: Reverse geocode if we have coordinates
    if not pd.isna(row['Latitude']) and not pd.isna(row['Longitude']):
        params = {
            'latlng': f"{row['Latitude']},{row['Longitude']}",
            'key': API_KEY
        }
        response = make_geocode_request(params)
        if response and response.get('results'):
            return extract_address_components(response['results'][0])
    
    # Case 2: Forward geocode with name and address
    params = {
        'address': f"{row['Store Name']}, {row['Address']}",
        'key': API_KEY
    }
    response = make_geocode_request(params)
    if response and response.get('results'):
        return extract_address_components(response['results'][0])
    
    return None

# Process all rows
results = []
for _, row in tqdm(df.iterrows(), total=len(df)):
    result = geocode_row(row)
    results.append(result)
    time.sleep(DEFAULT_DELAY)  # Rate limiting

# Create DataFrame from results
geocoded_data = pd.DataFrame([r for r in results if r is not None])

# Merge with original data
final_df = pd.concat([df, geocoded_data], axis=1)

# Save results
final_df.to_csv('scrape_data/pam_details_fully_geocoded.csv', index=False)

print(f"Geocoding complete. Success rate: {len(geocoded_data)}/{len(df)}")
print("Results saved to pam_details_fully_geocoded.csv")