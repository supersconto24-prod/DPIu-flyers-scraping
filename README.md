# PAM-flyers_scraping
 virtualenv myenv
source myenv/bin/activate
pip install -r requirements.txt






nohup python3 shop_create.py --log-level=DEBUG > shop_creation.log 2>&1 &

nohup python3 flyer_scraping.py > scraper_output.log 2>&1 &