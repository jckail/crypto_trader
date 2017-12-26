
import requests
import pandas as p
import datetime as dt
import os

concerned_coins = '+DES,BTC,BREAK,4jlkasjdflkasjdf,LTC'

def func_get_coin_list():


    print "Running func_get_coin_list"
    source = "cryptocompare"
    url = "https://min-api.cryptocompare.com/data/all/coinlist"

    headers = {
        'cache-control': "no-cache",
        'postman-token': "a1299df4-9db1-44cc-376e-0357176b776f"
    }

    response = requests.request("GET", url, headers=headers)
    data = response.json()


    df = p.DataFrame.from_dict(data["Data"],orient='index', dtype=None)

    df = df.assign (timestamp_api_call = dt.datetime.now(),source = source )

    df.to_csv('coinlist_info.csv',encoding='utf-8', index = False)

    return df


gcl_output = func_get_coin_list()
symbol_list = gcl_output["Symbol"].tolist()
"""lst1 = "'"+','.join(symbol_list[:2])+',BTC'"'" ## good list to queryable string+
print lst1"""

def validate_price_info(df_gcl_output):
    symbol_list = gcl_output["Symbol"].tolist()
    has_pricing =[]
    cwd = os.getcwd()
    count = 0
    total_symbols = len(symbol_list)
    print
    print "Started: "+str(dt.datetime.now())

    for symbol in symbol_list[:2]:
        count += 1
        url = "https://min-api.cryptocompare.com/data/pricemultifull"

        querystring = {"fsyms":symbol,"tsyms":'USD',"e":"CCCAGG"}

        headers = {
            'cache-control': "no-cache",
            'postman-token': "f3d54076-038b-9e2d-1ff3-593ae13aabbf"
        }
        #add try loop here with response 200 (success)
        response = requests.request("GET", url, headers=headers, params=querystring)
        data = response.json()

        if 'RAW' in data.keys():
            has_pricing.append({'symbol':str(symbol), 'has_pricing':1})

        else:
            has_pricing.append({'symbol':str(symbol),'has_pricing':0})
        print str(count)+' / '+str(total_symbols)

    df_has_pricing = p.DataFrame(has_pricing)
    df_has_pricing.to_csv(cwd+'/data/has_pricing.csv',encoding='utf-8', index = False)
    print cwd+'/data/has_pricing.csv'
    print "Ended: "+str(dt.datetime.now())

    """
    [{'points': 50, 'time': '5:00', 'year': 2010}, 
{'points': 25, 'time': '6:00', 'month': "february"}, 
{'points':90, 'time': '9:00', 'month': 'january'}, 
{'points_h1':20, 'month': 'june'}]
    
    
    
    df_has_pricing.to_csv('has_pricing.csv',encoding='utf-8', index = False)
    print df_has_pricing

    df_no_pricing = p.DataFrame({'no_pricing':no_pricing})
    print df_no_pricing
"""
validate_price_info(gcl_output)


#print symbol_list