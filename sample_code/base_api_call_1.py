import requests
import json
import pandas as p
import datetime as dt
from pandas.io.json import json_normalize

print '------------------ ------- -----  ---------'

coin_types = ["BTC","LTC","BCH","ETH"]

currency_conversion = "USD"

frames = []
limit = "2000"

for x in coin_types:
    url = "https://min-api.cryptocompare.com/data/histominute"

    querystring = {"fsym":x,"tsym":currency_conversion,"limit":limit,"aggregate":"3","e":"CCCAGG"}

    headers = {
        'cache-control': "no-cache",
        'postman-token': "7d32715c-d0cd-2d35-2b62-f20499f9a994"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)

    data = response.json()

    #test_df = p.DataFrame(data)

    test_df = p.DataFrame.from_dict(data,orient='index', dtype=None)

    a1 = test_df
    test_df = p.DataFrame.transpose(test_df)

    data_extract = a1.loc["Data",0]
    data_extract = p.DataFrame(data_extract)
    data_extract = data_extract.assign (cointype = x,timestamp_api_call = dt.datetime.now() )

    frames.append(data_extract)

df = p.concat(frames)
df['time'] = p.to_datetime(df['time'],unit='s') #converts to human from utcE GMT ******

print df
#print data_extract