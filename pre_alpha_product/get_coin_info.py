"""
author jkail

logical path call data create list of dicts per entry in list of dicts reparse sub directory
of data frame into new data frame thend append to frames[] (list)

feature store to csv with call, log time over time? (per second etc?) market cap volume info.
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

#print(response.text)
#print type(response)
data = response.json()
print(response.text)
print type(data)
print data

"""
for key in data.keys():
    print(key)
"""
data_extract = data["Data"]

df = p.DataFrame.from_dict(data_extract,orient='index', dtype=None)

df = df.assign (timestamp_api_call = dt.datetime.now() )
coin_list = df.loc[:,"Symbol"]
#print type(coin_list)

symbol_list = df["Symbol"].tolist()
#print dfList
#odd on dfList level has u json prefix
output_list = []
for symbol in symbol_list:
    output_list.append(symbol)
    #print symbol

#print output_list
#print coin_list
#print df
