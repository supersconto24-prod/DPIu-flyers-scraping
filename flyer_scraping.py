import pandas as pd

# Load the data
data = pd.read_csv('scrape_data\store_PAM_with_ids.csv')

#drop missing values in "_id" and "flyer_url" columns
data = data.dropna(subset=['Shop ID', 'Store URL'])

