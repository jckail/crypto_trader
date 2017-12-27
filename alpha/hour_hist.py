#!/usr/bin/env python

__author__ = 'jkail'

import json, requests
import pandas as p
from datetime import datetime, timedelta
import dateutil.parser
import time
import numpy as np
import datetime as dt
import os


class GetHourHist(object):

    def __init__(self,symbols, runfocus_symbols_only, focus_symbols):
        self.symbols = symbols
        self.focus_symbols = focus_symbols
        self.runfocus_symbols_only = runfocus_symbols_only

    def get_hour_hist(self):
        runfocus_symbols_only = self.runfocus_symbols_only
        focus_symbols = self.focus_symbols
        symbols = self.symbols
        cwd = os.getcwd()
        #exchanges =[ 'CCCAGG','Cryptsy', 'BTCChina', 'Bitstamp', 'BTER', 'OKCoin','Coinbase', 'Poloniex', 'Cexio', 'BTCE', 'BitTrex', 'Kraken', 'Bitfinex']
        exchanges = ['CCCAGG','Coinbase', 'Bitfinex']
        currentts = str(int(time.time()))
        url_limit = '2000'
        tsym = 'USD'

        if runfocus_symbols_only == 'Y':
            print"Pulling for only focus_symbols:"+str(focus_symbols)
            symbols = focus_symbols

        else:
            print 'processing: '+str(len(symbols))+' symbols'
            symbols = symbols

        xsymbols = [symbols[x:x+5] for x in xrange(0, len(symbols), 5)]


        for symbols in xsymbols:

            for symbol in symbols:
                frames = []
                for exchange in exchanges:

                    url = 'https://min-api.cryptocompare.com/data/histohour?fsym=' \
                          +symbol+'&tsym='+ tsym +'&limit='+url_limit+'&aggregate=1&e='+\
                          exchange +'&toTs=' + currentts
                    resp = requests.get(url=url)
                    data = json.loads(resp.text)

                    if  data["Data"] != []:
                        print 'Sucess: '+currentts+'   '+exchange+'  '+symbol
                        datasorted = sorted(data['Data'], key=lambda k: int(k['time']))
                        df = p.DataFrame(datasorted)
                        df = df.assign(coin = symbol, coin_units = 1, timestamp_api_call = dt.datetime.now(),computer_name = 'JordanManual',exchange = exchange )
                        frames.append(df)

                    else:
                        print 'Invalid: '+currentts+'   '+exchange+'  '+symbol

                my_file = cwd+'/data/hour_data/'+symbol+'_hour.csv'

                if os.path.isfile(my_file):
                    df_resident = p.DataFrame.from_csv(my_file)
                    print 'appending new data: '+symbol
                    frames.append(df_resident)
                else:
                    print 'no new data to append: '+symbol

                df = p.concat(frames)
                df = df.drop_duplicates(['time','exchange','coin'], keep='last')
                df = df.sort_values('time')
                df = df.reset_index(drop=True)

                if not df.empty:
                    df.to_csv(cwd+'/data/hour_data/'+symbol+'_hour.csv', index_label='Id')
                    print 'Updated: '+cwd+'/data/hour_data/'+symbol+'_hour.csv'
                else:
                    print'No minhist for: '+symbol

    def main(self):

        print 'run main'
        print 'begin: GetHourHist.main'
        hpc = GetHourHist(self.symbols, self.runfocus_symbols_only, self.focus_symbols)
        try:
            hpc.get_hour_hist()

        except:
            print 'error get_hour_hist failed'

        print 'end: GetHourHist.main'


if __name__ == '__main__':
    runner = GetHourHist()
    runner.main()


