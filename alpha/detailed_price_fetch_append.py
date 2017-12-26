#notes
"""
NOTES:
author jkail

intent: retreive detail price info, on loop, appending to existing csv every x seconds for passed through list of cryptsos

moved features: mvp

desired features:
look into delaying x coin refresh for x seconds

(create kvp between coin and last runtime, then when enter while loop evaluates
that x time has passed for said coin to refresh the coin again)

create output report via starts and stops (this will be emailed or stored in log files etc )


notes:
designed on airplane, going to be crap.
intial coinlist:
BTC,ETH,LTC,BCH,RPX,DASH,ZCASH

3DES is bad
"""

import requests
import pandas as p
import datetime as dt
from itertools import izip

hardcoded_symbols = '3DES,BTC,BREAK,4jlkasj dflkasjdf,LTC'
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

# create add to csv etc..
