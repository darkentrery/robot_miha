import datetime
import os
import json
from sys import argv


def get_data(file_keys_path):
    with open(file_keys_path, 'r', encoding='utf-8') as data_file:
        data = json.load(data_file)
    return data


data_base_name = 'dbconfig.json'
directory = os.path.dirname(__file__)
print(directory)

file_keys_path = f"{os.path.dirname(__file__)}{data_base_name}"
file_keys_path = f"{os.path.dirname(__file__)}\\{data_base_name}"

print(f"{file_keys_path=}")

SYMBOL = 'BTC'
#SYMBOL = argv[1]
#SYMBOL = SYMBOL.upper()
MYSQL_TABLE_READ = SYMBOL + '_price_many_hadge_long'
TELEGRAM_METADATA = {"channel_id":"-1001541461039", "token":"1878579785:AAH0yC1onsi-5bVkAIygXEHG5PK18-UZisI"}
MYSQL_TABLE_CONFIG = '0_config'
MYSQL_TABLE_SUMMARY = '0_summary'

data = get_data(file_keys_path)
print(f"{data=}")

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

launch['mode'] = data['db_name'].split('_')[0]

launch['db']['pair'] = SYMBOL
launch['price_table_name'] = MYSQL_TABLE_READ
launch['config_table'] = MYSQL_TABLE_CONFIG
launch['summary_table'] = MYSQL_TABLE_SUMMARY
launch['telegram_metadata'] = TELEGRAM_METADATA