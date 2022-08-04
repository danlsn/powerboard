import configparser
import amberelectric
from pprint import pprint
from amberelectric.api import amber_api
from datetime import date

# Safely Import Credentials Using 'configparser'
CONFIG_FILE = "../config.ini"
config = configparser.ConfigParser()
config.read(CONFIG_FILE)

# Amber Electric API Configuration
amber_u1_config = amberelectric.Configuration(
    access_token=config["AMBER_UNIT1"]["TOKEN"]
)
amber_u2_config = amberelectric.Configuration(
    access_token=config["AMBER_UNIT2"]["TOKEN"]
)

# Create Amber Electric API Instances
amber_u1 = amber_api.AmberApi.create(amber_u1_config)
amber_u2 = amber_api.AmberApi.create(amber_u2_config)

# Fetch Amber Electric Sites
try:
    u1_site = amber_u1.get_sites()[0]
except amberelectric.ApiException as e:
    print("Exception: %s\n" % e)
try:
    u2_site = amber_u2.get_sites()[0]
except amberelectric.ApiException as e:
    print("Exception: %s\n" % e)

# Fetch Amber Electric Prices
site_id = u1_site.id
try:
    start_date = date(2021, 1, 1)
    end_date = date(2021, 12, 31)
    price_history = amber_u1.get_prices(
        site_id, start_date=start_date, end_date=end_date
    )
except amberelectric.ApiException as e:
    print("Exception: %s\n" % e)

pprint(price_history)
pass