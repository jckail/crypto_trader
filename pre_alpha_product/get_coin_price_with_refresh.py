


"""
author: jkail

get list of all avalible coins and current price


features:
look into delaying x coin refresh for x seconds

(create kvp between coin and last runtime, then when enter while loop evaluates
that x time has passed for said coin to refresh the coin again)

create output report via starts and stops (this will be emailed or stored in log files etc )

"""

import requests
import pandas as p
import datetime as dt
from itertools import izip

###config
start_time = dt.datetime.now()
symbol_list_restriction = 10  # set to zero if there if its all tickers
loop_restriction = 10
frames = []
relative_value = "USD"
loop_count = 0
error_coins =[]
success_coins = []
requested_symbols =['BTC']
data_source = "cryptocompare"
loop_delay = 10


###apiconfig
url = "https://min-api.cryptocompare.com/data/all/coinlist"

headers = {
    'cache-control': "no-cache",
    'postman-token': "a1299df4-9db1-44cc-376e-0357176b776f"
}


###START INFORMATION###
print "---Completed!---"
print "Started: "+str(start_time)
print "symbol_list_restriction: " + str(symbol_list_restriction)
print "loop_restriction: " + str(loop_restriction)
print "relative values: "+ relative_value


###begin main
if len(requested_symbols) == 0:
    print "Fetching Symbols: no specified request... "
    response = requests.request("GET", url, headers=headers)
    data = response.json()

    data_extract = data["Data"]

    df = p.DataFrame.from_dict(data_extract,orient='index', dtype=None)

    df = df.assign (timestamp_api_call = dt.datetime.now() )

    coin_list = df.loc[:,"Symbol"]

    symbol_list = df["Symbol"].tolist()

    if symbol_list_restriction == 0:
        symbol_list = symbol_list
        print "Full send eh?"

    else:
        symbol_list = symbol_list[:symbol_list_restriction]


    print 'Number of Coins pulling data for: '+str(len(symbol_list))+' coins'
    symbol_list_str = str(symbol_list)
    for x in ["'",'u']:
        if x in symbol_list_str:
            symbol_list_str = symbol_list_str.replace(x,'')

    print "Fetching info for: "+ symbol_list_str


else:
    symbol_list = requested_symbols
    symbol_list_str = str(symbol_list)
    for x in ["'",'u']:
        if x in symbol_list_str:
            symbol_list_str = symbol_list_str.replace(x,'')

    print 'Number of Coins pulling data for: '+str(len(symbol_list))+' coins'
    print "Fetching info for: "+ symbol_list_str


while loop_count < loop_restriction:
    loop_count += 1
    print "Loop Count: "+str(loop_count)+" Start: "+str(dt.datetime.now())

    for symbol in symbol_list:
        #print symbol

        if symbol not in error_coins:
            url = "https://min-api.cryptocompare.com/data/pricemultifull"

            querystring = {"fsyms":symbol,"tsyms":relative_value}

            headers = {
                'cache-control': "no-cache",
                'postman-token': "f3d54076-038b-9e2d-1ff3-593ae13aabbf"
            }

            response = requests.request("GET", url, headers=headers, params=querystring)

            data = response.json()
            keys = data.keys()
            data = data
            #print(response.text)



            if  "Response" not in keys:
                #print "Sucess:"+  symbol
                if symbol not in success_coins:
                    success_coins.append(symbol)
                test_df = p.DataFrame.from_dict(data['RAW'][symbol],orient='Columns', dtype=None)
                a1 = test_df
                test_df = p.DataFrame.transpose(test_df)
                test_df = test_df.assign (coin = symbol,coin_units = 1, timestamp_api_call = dt.datetime.now(),loop = loop_count,computer_name = 'JordanManual',data_source = data_source ) ##replace with ec2ip/region
                #print test_df
                frames.append(test_df)
                #insert directly into data store will latency if aggregate all data frames at the end* per test 20 minute lag time if we do all on one cpu


            else:
                response = data["Response"]
                #print response + ':'+symbol
                error_coins.append(symbol)

df = p.concat(frames)
###end main###



#formating output
success_coins_str = str(success_coins)
for x in ["'",'u']:
    if x in success_coins_str:
        success_coins_str = success_coins_str.replace(x,'')

error_coins_str = str(error_coins)
for x in ["'",'u']:
    if x in error_coins_str:
        error_coins_str = error_coins_str.replace(x,'')


df.to_csv('coin_price_details.csv',encoding='utf-8', index = False)
###Completion information INFORMATION###
print "---Completed!---"
print "Started: "+str(start_time)
print "Ended: "+str(dt.datetime.now())
print "Total Loops: "+str(loop_count)
print "Attempted: "+str(len(symbol_list))+' coins'
print "Success Count: "+str(len(success_coins))+' coins'
print 'Success List: '+success_coins_str
print "Error Count: "+str(len(error_coins))+' coins'
print 'Error List: '+error_coins_str
print df

