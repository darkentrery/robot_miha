import datetime
import os
import json
from sys import argv


def get_data(file_keys_path):
    with open(file_keys_path, 'r', encoding='utf-8') as data_file:
        data = json.load(data_file)
    return data

def get_keys(file_keys_path):
    with open(file_keys_path, 'r') as data_file:
        data = json.load(data_file)
        API_KEY =data['trading_apikey_1']
        API_SECRET = data['trading_secretkey_1']
    return API_KEY, API_SECRET

data_base_name = 'dbconfig.json'
directory = os.path.dirname(__file__)
print(directory)

file_keys_path = f"{os.path.dirname(__file__)}{data_base_name}"
file_keys_path = f"{os.path.dirname(__file__)}\\{data_base_name}"

print(f"{file_keys_path=}")

SYMBOL = 'btc'
#SYMBOL = argv[1]
SYMBOL = SYMBOL.upper()
MYSQL_TABLE_READ = SYMBOL + '_price'
TELEGRAM_METADATA = {"channel_id":"-1001541461039", "token":"1878579785:AAH0yC1onsi-5bVkAIygXEHG5PK18-UZisI"}
MYSQL_TABLE_CONFIG = '0_config'

API_KEY, API_SECRET = get_keys(file_keys_path)
data = get_data(file_keys_path)

#configs
robot_is_stoped = True

launch = {}

launch['start_time'] = datetime.datetime(2012, 9, 1)
launch['end_time'] = datetime.datetime(2032, 1, 1)

launch['db'] = {}
launch['db']['user'] = data['db_user']
launch['db']['password'] = data['db_pass']
launch['db']['host'] = data['db_host']
launch['db']['database'] = data['db_name']

launch['mode'] = data['db_name'][:-16]
launch['traiding_mode'] = 'many'

launch['db']['api_key'] = API_KEY
launch['db']['api_sekret'] = API_SECRET
launch['db']['pair'] = SYMBOL
launch['price_table_name'] = MYSQL_TABLE_READ
launch['config_table'] = MYSQL_TABLE_CONFIG
launch['telegram_metadata'] = TELEGRAM_METADATA