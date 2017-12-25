import json, requests
import pandas as pd
from datetime import datetime, timedelta
import dateutil.parser
import time
import numpy as np

exchange = 'CCCAGG'
tsym = 'USD'
fsym = 'BTC'
loop_count = 0
frames = []


while loop_count < 3:
    loop_count += 1
    currentTS = str(int(time.time()))
    print currentTS
    allData = []
    for i in range(1,7):
        url = 'https://min-api.cryptocompare.com/data/histominute?fsym='+fsym+'&tsym='+ tsym +'&limit=2000&aggregate=1&e='+ exchange +'&toTs=' + currentTS
        print(url)
        resp = requests.get(url=url)
        data = json.loads(resp.text)
        dataSorted = sorted(data['Data'], key=lambda k: int(k['time']))
        allData += dataSorted
        currentTS = str(dataSorted[0]['time'])
        df = pd.DataFrame(allData)
        frames.append(df)

    #pair = 'BTC' + tsym
    #df.columns = formatHeader(df, pair)
    #return df


df = pd.concat(frames)
df = df.drop_duplicates()
df = df.sort(columns=['time'], ascending=[1])
df['time'] = pd.to_datetime(df['time'],unit='s')
df.to_csv(fsym+ tsym + '_minute.csv')
print df
