import requests
import pandas as p
import datetime as dt
import os
import threading
import urllib2
import time

start = time.time()
symbols = ['ETH']

df_get_id = p.DataFrame.from_csv('/Users/jkail/Documents/GitHub/lit_crypto/alpha/data/coinlist_info.csv')
b = df_get_id["Symbol"].tolist()
symbols = b
print symbols


def fetch_url(symbol):
    url = "https://min-api.cryptocompare.com/data/pricemultifull"
    querystring = {"fsyms":symbol,"tsyms":'USD',"e":"CCCAGG"}
    headers = {
        'cache-control': "no-cache",
        'postman-token': "f3d54076-038b-9e2d-1ff3-593ae13aabbf"
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    print response
    print "'%s\' fetched in %ss" % (url, (time.time() - start))


threads = [threading.Thread(target=fetch_url, args=(symbol,)) for symbol in symbols]
for thread in threads:
    thread.start()

for thread in threads:
    thread.join()