import json, requests
import pandas as p
from datetime import datetime, timedelta
import dateutil.parser
import time
import numpy as np
import datetime as dt
import os

cwd = os.getcwd()
print cwd
exchanges =[ 'CCCAGG','Cryptsy', 'BTCChina', 'Bitstamp', 'BTER', 'OKCoin','Coinbase', 'Poloniex', 'Cexio', 'BTCE', 'BitTrex', 'Kraken', 'Bitfinex']
#exchanges = ['CCCAGG','Coinbase', 'Bitfinex']
exchange = 'CCCAGG'
tsym = 'USD'

symbols = ['007', '1337', '1CR', '1ST', '2015', '2BACCO', '2GIVE', '32BIT', '365', 'BTC', 'BCH', 'LTC', 'ETH']


url_limit = '2000'

runfocus_symbols_only = 'Y'
focus_symbols = ['BTC','BCH','LTC','ETH']

f
if runfocus_symbols_only == 'Y':
    print"Pulling for only focus_symbols:"+str(focus_symbols)
    symbols = focus_symbols

else:
    print 'processing: '+str(len(symbols))+' symbols'
    symbols = symbols

xsymbols = [symbols[x:x+5] for x in xrange(0, len(symbols), 5)]
print xsymbols

for symbols in xsymbols:
    print symbols
    for symbol in symbols:
        frames = []
        for exchange in exchanges:
            currentTS = str(int(time.time()))

            allData = []
            url = 'https://min-api.cryptocompare.com/data/histominute?fsym=' \
                  +symbol+'&tsym='+ tsym +'&limit='+url_limit+'&aggregate=1&e='+ \
                  exchange +'&toTs=' + currentTS
            resp = requests.get(url=url)
            data = json.loads(resp.text)

            if  data["Data"] != []:
                print 'Sucess: '+currentTS+'   '+exchange+'  '+symbol
                dataSorted = sorted(data['Data'], key=lambda k: int(k['time']))
                allData += dataSorted
                currentTS = str(dataSorted[0]['time'])
                df = p.DataFrame(allData)
                df = df.assign(coin = symbol, coin_units = 1, timestamp_api_call = dt.datetime.now(),computer_name = 'JordanManual',exchange = exchange )
                frames.append(df)

            else:
                print 'Invalid: '+currentTS+'   '+exchange+'  '+symbol
        df_resident = p.DataFrame.from_csv(cwd+'/data/minute_data/'+symbol+'_minute.csv')
        frames.append(df_resident)
        df = p.concat(frames)
        df = df.drop_duplicates()
        df.to_csv(cwd+'/data/minute_data/'+symbol+'_minute.csv')
        print 'Updated: '+cwd+'/data/minute_data/'+symbol+'_minute.csv'
