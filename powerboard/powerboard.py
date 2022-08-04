import configparser
import amberelectric
from amberelectric.api import amber_api


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
    u1_sites = amber_u1.get_sites()
except amberelectric.ApiException as e:
    print("Exception: %s\n" % e)
try:
    u2_sites = amber_u2.get_sites()
except amberelectric.ApiException as e:
    print("Exception: %s\n" % e)


