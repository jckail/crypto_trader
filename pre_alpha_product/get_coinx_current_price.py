
""""
author jkail
prealpha
calls cryptocompareapi gets current price of many coins to one USD
latency on airplane ~3 seconds supossadly 2ms lagtime on website

"""

import requests
import json
import pandas as p
import datetime as dt
from pandas.io.json import json_normalize

print '------------------ ------- -----  ---------'

run_count = 0
#coin_types = ["BTC","LTC","BCH","ETH","RPX"]

coin_types = ["USD"]
currency_conversions = "USD,BTC,EUR,LTC,BCH,RPX,ETH"

frames = []
limit = "1"
while run_count < 10:
    for x in coin_types:
        url = "https://min-api.cryptocompare.com/data/price"

        querystring = {"fsym":x,"tsyms":currency_conversions}

        headers = {
            'cache-control': "no-cache",
            'postman-token': "c7a884d1-01a6-897a-f839-c246a82f206c"
        }

        response = requests.request("GET", url, headers=headers, params=querystring)

        data = response.json()

        #test_df = p.DataFrame(data)

        print type(data)
        test_df = p.DataFrame.from_dict(data,orient='index', dtype=None)
        a1 = test_df
        test_df = p.DataFrame.transpose(test_df)
        test_df = test_df.assign (cointype = x,timestamp_api_call = dt.datetime.now() )

        frames.append(test_df)
        run_count +=1
        print run_count

df = p.concat(frames)
#df['time'] = p.to_datetime(df['time'],unit='s') #converts to human from utcE GMT ******

#df.to_csv('test.csv',encoding='utf-8', index = False)
print df
#print data_extract