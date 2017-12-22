


"""
logic, get list of coins, w/ list get current usd price for each coin,

if success append to current price list (new row indicating coin type and price relative to usd

if fail append to current fail list



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

symbol_list = symbol_list[:10]

"""
print symbol_list
symbol_list = str(symbol_list)

for x in ["'",'u']:
    if x in symbol_list:
        symbol_list = symbol_list.replace(x,'')

for x in ['[',']']:
    if x in symbol_list:
        symbol_list = symbol_list.replace(x,'"')
"""

run_count = 0
coin_types = ["USD"]
frames = []
limit = "1"
while run_count < 2:
    for x in coin_types:
        url = "https://min-api.cryptocompare.com/data/price"
        print symbol_list
        querystring = {"fsym":x,"tsyms":symbol_list}

        headers = {
            'cache-control': "no-cache",
            'postman-token': "c7a884d1-01a6-897a-f839-c246a82f206c"
        }

        response = requests.request("GET", url, headers=headers, params=querystring)

        data = response.json()

        test_df = p.DataFrame.from_dict(data,orient='index', dtype=None)
        a1 = test_df
        test_df = p.DataFrame.transpose(test_df)
        test_df = test_df.assign (cointype = x,timestamp_api_call = dt.datetime.now() )

        frames.append(test_df)
        run_count +=1


df = p.concat(frames)
#df['time'] = p.to_datetime(df['time'],unit='s') #converts to human from utcE GMT ******

#df.to_csv('test.csv',encoding='utf-8', index = False)
print df