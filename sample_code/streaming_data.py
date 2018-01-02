import json, requests
import pandas as pd
from datetime import datetime, timedelta
import dateutil.parser
import time
import numpy as np
import datetime as dt

exchanges =[ 'CCCAGG','Cryptsy', 'BTCChina', 'Bitstamp', 'BTER', 'OKCoin',
             'Coinbase', 'Poloniex', 'Cexio', 'BTCE', 'BitTrex', 'Kraken', 'Bitfinex']
tsym = 'USD'
fsym = 'BTC'
loop_count = 0
frames = []
iteration = 2
url_limit = '2000'


while loop_count < 2:
    loop_count += 1
    for exchange in exchanges:
        currentTS = str(int(time.time()))
        print currentTS+'   '+exchange
        allData = []
        for i in range(1,iteration):
            url = 'https://min-api.cryptocompare.com/data/histominute?fsym='+fsym+'&tsym='+ tsym +'&limit='+url_limit+'&aggregate=1&e='+ 'CCCAGG,Cryptsy' +'&toTs=' + currentTS
            #print(url)
            resp = requests.get(url=url)
            data = json.loads(resp.text)
            keys = data.keys()
            #print keys
            if  data["Data"] != []:
                dataSorted = sorted(data['Data'], key=lambda k: int(k['time']))
                allData += dataSorted
                currentTS = str(dataSorted[0]['time'])
                df = pd.DataFrame(allData)
                df = df.assign(coin = fsym,coin_units = 1, timestamp_api_call = dt.datetime.now(),hostname = socket.gethostname(),exchange = exchange )
                frames.append(df)

            else:
                #print 'damn'
                print data['Message']

print frames

df = pd.concat(frames)
df = df.drop_duplicates()
df = df.sort(columns=['time'], ascending=[1])
df['time'] = pd.to_datetime(df['time'],unit='s')
df.to_csv(fsym+ tsym + '_minute.csv')
print df
