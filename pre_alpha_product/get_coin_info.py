"""
author jkail

logical path call data create list of dicts per entry in list of dicts reparse sub directory
of data frame into new data frame thend append to frames[] (list)


"""
import requests
import json
import pandas as p
import datetime as dt
from pandas.io.json import json_normalize


coin_types = ["BTC","LTC","BCH","ETH","RPX"]

currency_conversion = "USD"

frames = []
limit = "1"

url = "https://min-api.cryptocompare.com/data/all/coinlist"

headers = {
    'cache-control': "no-cache",
    'postman-token': "a1299df4-9db1-44cc-376e-0357176b776f"
}

response = requests.request("GET", url, headers=headers)

print(response.text)
print type(response)
data = response.json()

print data

user_ids = []
frames = []

for user_id, d in data.iteritems():
    user_ids.append(user_id)
    frames.append(p.DataFrame.from_dict(d, orient='index'))

test_df = p.concat(frames, keys=user_ids)
p.help.fromjson
print test_df