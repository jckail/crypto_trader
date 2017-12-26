import requests
import pandas as p
import datetime as dt
from itertools import izip

hardcoded_symbols = '3DES,BTC,BREAK,4jlkasj dflkasjdf,LTC'
#symbols = ['007', '1337', '1CR', '1ST', '2015', '2BACCO', '2GIVE', '32BIT', '365', 'BTC', 'BCH', 'LTC', 'ETH']
symbols = ['007']

def get_get_details_for_symbols(symbols):
    frames = []

    xsymbols = [symbols[x:x+50] for x in xrange(0, len(symbols), 50)]
    print xsymbols

    for symbols in xsymbols:

        api_call_symbols = "'"+','.join(symbols)+"'"
        print api_call_symbols

        url = "https://min-api.cryptocompare.com/data/pricemultifull"

        querystring = {"fsyms":api_call_symbols,"tsyms":'USD',"e":"CCCAGG"}
        #querystring = {"fsyms":symbol,"tsyms":relative_value,"e":"CCCAGG"}

        headers = {
            'cache-control': "no-cache",
            'postman-token': "f3d54076-038b-9e2d-1ff3-593ae13aabbf"
        }

        #add try loop here with response 200 (success)
        response = requests.request("GET", url, headers=headers, params=querystring)
        data = response.json()
        print data

        for key in data['RAW'].keys():
            #print key+'here'
            test_df = p.DataFrame.from_dict(data['RAW'][key],orient='Columns', dtype=None)
            test_df = p.DataFrame.transpose(test_df)
            test_df = test_df.assign (coin = key, coin_units = 1, timestamp_api_call = dt.datetime.now(),computer_name = 'JordanManual') ##replace with ec2ip/region
            frames.append(test_df)

    df = p.concat(frames)
    print df

    #return df

get_get_details_for_symbols(symbols)