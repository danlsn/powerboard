import configparser
import pickle
from datetime import date

import amberelectric
from amberelectric.api import amber_api, AmberApi
from amberelectric.model.site import Site

# Define Constants
DATA_OUT_DIR = "../data"

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
amber_u1: AmberApi = amber_api.AmberApi.create(amber_u1_config)
amber_u2: AmberApi = amber_api.AmberApi.create(amber_u2_config)


# Fetch Amber Electric Sites
def fetch_site(api_client: AmberApi) -> Site:
    try:
        sites = api_client.get_sites()
        return sites[0]
    except amberelectric.ApiException as e:
        print("Exception: %s\n" % e)


# Fetch Amber Electric Prices
def fetch_price_history(api_client, site_id, start_date: date, end_date: date):
    try:
        amber_price_history = api_client.get_prices(
            site_id, start_date=start_date, end_date=end_date
        )
    except amberelectric.ApiException as e:
        print("Exception: %s\n" % e)
        raise Exception

    # Pickle Amber Electric Price History Response Object
    with open(
        f"{DATA_OUT_DIR}/amber/price_history_{start_date}_{end_date}.pickle",
        "wb",
    ) as f:
        pickle.dump(amber_price_history, f, pickle.HIGHEST_PROTOCOL)

    return amber_price_history


# Fetch Amber Electric Power Usage
def fetch_power_usage(api_client: AmberApi, start_date: date, end_date: date):
    site_id = fetch_site(api_client).id
    try:
        power_usage = api_client.get_usage(site_id, start_date, end_date)
    except amberelectric.ApiException as e:
        print("Exception: %s\n" % e)
        raise Exception

    # Pickle Amber Electric Power Usage Response Object
    with open(
            f"{DATA_OUT_DIR}/amber/power_usage_{start_date}_{end_date}.pickle",
            "wb",
    ) as f:
        pickle.dump(power_usage, f, pickle.HIGHEST_PROTOCOL)

    return power_usage


# u1_site: Site = fetch_site(amber_u1)
# u2_site: Site = fetch_site(amber_u2)

# price_history = fetch_price_history(amber_u1, u1_site.id, date(2021, 1, 1), date(2021, 12, 31))

fetch_power_usage(amber_u1, date(2022, 1, 1), date(2022, 1, 31))
pass
