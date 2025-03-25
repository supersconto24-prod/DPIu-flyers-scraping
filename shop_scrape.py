import os
import pandas as pd
import time
import multiprocessing
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

# Configuration
COMUNI_CSV = "comuni.csv"
PAM_CSV = "pam.csv"
OUTPUT_DIR = "scrape_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)
CHROME_DRIVER_PATH = "/usr/local/bin/chromedriver"

def setup_driver():
    """Initialize a headless Chrome WebDriver."""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    service = Service(CHROME_DRIVER_PATH)
    return webdriver.Chrome(service=service, options=options)

def scrape_pam_stores(comune, output_file):
    """Scrape Pam stores for a specific comune and append to CSV."""
    driver = setup_driver()
    stores = []

    try:
        # Open the website
        driver.get('https://www.pampanorama.it/punti-vendita')
        time.sleep(3)  # Wait for the page to load

        # Locate the input field
        input_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'input[placeholder="Digita indirizzo o il CAP"]'))
        )
        input_field.clear()
        input_field.send_keys(comune)
        input_field.send_keys(Keys.RETURN)
        time.sleep(3)  # Wait for results
        # Parse the store list
        store_items = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.StoreItem')))

        for store in store_items:
            try:
                # Extract the store link from the <a> tag with class 'icon'
                store_link = store.find_element(By.CSS_SELECTOR, 'a.icon').get_attribute('href')
                store_url = 'https://www.pampanorama.it' + store_link if store_link.startswith('/') else store_link
                print(f"Extracted store URL: {store_url}")
                stores.append({
                    'Store Name': 'N/A',  # Placeholder for store name
                    'Address': 'N/A',     # Placeholder for address
                    'Contact': 'N/A',     # Placeholder for contact
                    'Store URL': store_url
                })
            except Exception as e:
                print(f"Failed to extract store link - {e}")
                continue  # Skip this store and continue with the next one

    except Exception as e:
        print(f"Failed to process {comune} - {e}")

    finally:
        driver.quit()

        # Append results to CSV after processing each comune
        if stores:
            df = pd.DataFrame(stores)
            file_exists = os.path.isfile(output_file)
            df.to_csv(output_file, mode='a', header=not file_exists, index=False, encoding='utf-8')
            print(f"Saved {len(stores)} stores for {comune} to {output_file}")

def main():
    # Load comuni data
    comuni_df = pd.read_csv(COMUNI_CSV, encoding="iso-8859-1")
    comuni = comuni_df['Comune'].unique().tolist()

    output_file = os.path.join(OUTPUT_DIR, PAM_CSV)

    # Clear existing file if it exists
    if os.path.exists(output_file):
        os.remove(output_file)

    


        # Use multiprocessing with a lock for file writing
    with multiprocessing.Pool(processes=4) as pool:
        # Map each comune to the scrape function with the output file
        pool.starmap(scrape_pam_stores, [(comune, output_file) for comune in comuni])

    print(f"Final data saved to {output_file}")

if __name__ == '__main__':
    main()