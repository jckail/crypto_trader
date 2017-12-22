import requests
import pandas as p
import datetime as dt


symbol_list = ['007', '1337', '1CR', '1ST', '2015', '2BACCO', '2GIVE', '32BIT', '365', '3DES', 'BTC']
#symbol_list = ['365', '3DES', 'BTC']
frames = []
relative_value = "BTC,ETH,USD,EUR"
loop_count = 0
error_coins =[]

while loop_count < 10:
    loop_count += 1
    print loop_count
    for symbol in symbol_list:

        url = "https://min-api.cryptocompare.com/data/price"

        querystring = {"fsym":symbol,"tsyms":relative_value}

        headers = {
            'cache-control': "no-cache",
            'postman-token': "b2062bbb-798d-c41a-5b81-f906bc696e6b"
        }

        response = requests.request("GET", url, headers=headers, params=querystring)
        data = response.json()


        keys = data.keys()
        #print keys

        if  "Response" not in keys:
            #print  symbol
            test_df = p.DataFrame.from_dict(data,orient='index', dtype=None)
            a1 = test_df
            test_df = p.DataFrame.transpose(test_df)
            test_df = test_df.assign (coin = symbol,timestamp_api_call = dt.datetime.now() )
            #print test_df
            frames.append(test_df)

        else:
            response = data["Response"]
            print response + ': '+symbol
            error_coins.append(symbol)




df = p.concat(frames)
print df
print 'Error list: '+str(error_coins)