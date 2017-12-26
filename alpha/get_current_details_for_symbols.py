import requests
import pandas as p
import datetime as dt
from itertools import izip

hardcoded_symbols = '3DES,BTC,BREAK,4jlkasj dflkasjdf,LTC'


def get_get_details_for_symbols(symbols):
    frames = []
    url = "https://min-api.cryptocompare.com/data/pricemultifull"

    querystring = {"fsyms":hardcoded_symbols,"tsyms":'USD',"e":"CCCAGG"}
    #querystring = {"fsyms":symbol,"tsyms":relative_value,"e":"CCCAGG"}

    headers = {
        'cache-control': "no-cache",
        'postman-token': "f3d54076-038b-9e2d-1ff3-593ae13aabbf"
    }

    #add try loop here with response 200 (success)
    response = requests.request("GET", url, headers=headers, params=querystring)
    data = response.json()


    for key in data['RAW'].keys():
        #print key+'here'
        test_df = p.DataFrame.from_dict(data['RAW'][key],orient='Columns', dtype=None)
        test_df = p.DataFrame.transpose(test_df)
        test_df = test_df.assign (coin = key, coin_units = 1, timestamp_api_call = dt.datetime.now(),computer_name = 'JordanManual') ##replace with ec2ip/region
        frames.append(test_df)


    df = p.concat(frames)
    print df
    #return df

get_get_details_for_symbols(hardcoded_symbols)