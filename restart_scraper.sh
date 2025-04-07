#!/bin/bash

# Reset git to previous commit
sudo git reset --hard HEAD^

# Pull latest changes
git pull

# Make files executable
chmod +x shop_scrape.py

# Remove old log file
rm -rf scraper_output.log

# Run the scraper in background and redirect output to log file
nohup ./shop_scrape.py > scraper_output.log 2>&1 &

# Note: There was a typo in your original command with "&rm" at the end which I removed

echo "Restart shop scraping"
