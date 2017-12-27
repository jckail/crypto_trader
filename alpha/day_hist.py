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


class GetDayHist(object):

    def __init__(self,symbols, runfocus_symbols_only, focus_symbols):
        self.symbols = symbols
        self.focus_symbols = focus_symbols
        self.runfocus_symbols_only = runfocus_symbols_only

    def get_day_hist(self):
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

                    url = 'https://min-api.cryptocompare.com/data/histoday?fsym=' \
                          +symbol+'&tsym='+ tsym +'&limit='+url_limit+'&aggregate=1&e='+ \
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

                my_file = cwd+'/data/day_data/'+symbol+'_day.csv'

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
                    df.to_csv(cwd+'/data/day_data/'+symbol+'_day.csv', index_label='Id')
                    print 'Updated: '+cwd+'/data/day_data/'+symbol+'_day.csv'
                else:
                    print'No dayhist for: '+symbol

    def main(self):


        print 'begin: GetDayHist.main'
        hpc = GetDayHist(self.symbols, self.runfocus_symbols_only, self.focus_symbols)
        try:
            hpc.get_day_hist()

        except:
            print 'error get_day_hist failed'

        print 'end: GetDayHist.main'


if __name__ == '__main__':
    runner = GetDayHist()
    runner.main()


