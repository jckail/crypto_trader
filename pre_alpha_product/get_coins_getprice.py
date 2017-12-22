"""
author jkail

logical path call data create list of dicts per entry in list of dicts reparse sub directory
of data frame into new data frame thend append to frames[] (list)

this is just a sample script pulls all coins gets historic price for each

v .001

"""
import requests
import pandas as p
import datetime as dt
from itertools import izip

currency_conversion = "USD"

frames = []
api_call_limit = "10" #number of records returned to api
loop_count = 0
limit_loops = 50
history_type= "histominute" #histohour #histoday

url = "https://min-api.cryptocompare.com/data/all/coinlist"

headers = {
    'cache-control': "no-cache",
    'postman-token': "a1299df4-9db1-44cc-376e-0357176b776f"
}

response = requests.request("GET", url, headers=headers)
data = response.json()

data_extract = data["Data"]

df = p.DataFrame.from_dict(data_extract,orient='index', dtype=None)

df = df.assign (timestamp_api_call = dt.datetime.now() )

coin_list = df.loc[:,"Symbol"]

symbol_list = df["Symbol"].tolist()

print 'Number of Coins pulling data for: '+str(len(coin_list))+' coins'


for index, coin in izip(xrange(limit_loops), coin_list):
    loop_count += 1
    url = "https://min-api.cryptocompare.com/data/"+history_type

    querystring = {"fsym":coin,"tsym":currency_conversion,"limit":api_call_limit,"aggregate":"3","e":"CCCAGG"}

    headers = {
        'cache-control': "no-cache",
        'postman-token': "7d32715c-d0cd-2d35-2b62-f20499f9a994"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)
    data = response.json()
    status = data["Response"]

    if status == 'Success' :
        print str(loop_count)+' '+status+': '+coin
        test_df = p.DataFrame.from_dict(data,orient='index', dtype=None)
        a1 = test_df
        test_df = p.DataFrame.transpose(test_df)

        data_extract = a1.loc["Data",0]
        data_extract = p.DataFrame(data_extract)
        data_extract = data_extract.assign (cointype = coin,timestamp_api_call = dt.datetime.now() )

        frames.append(data_extract)
    else:
        print str(loop_count)+' '+status+': '+coin

df = p.concat(frames)
df['time'] = p.to_datetime(df['time'],unit='s') #converts to human from utcE GMT ******

print df
