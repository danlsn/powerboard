import configparser

# Safely Import Credentials Using 'configparser'
CONFIG_FILE = '../config.ini'
config = configparser.ConfigParser()
config.read(CONFIG_FILE)
