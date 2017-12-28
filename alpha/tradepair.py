#!/usr/bin/env python

__author__ = 'jkail'

import requests
import pandas as p
import datetime as dt
import os


class GetTradePair(object):

    def __init__(self, symbols):
        self.symbols = symbols


    def trading_partners(self):
        append_list = ['USD','JPY','USDT','EUR','KRW']
        symbols = self.symbols
        source = 'cryptocompare'
        cwd = os.getcwd()
        for x in append_list:
            symbols.append(x)
        for symbol in symbols:
            frames = []
            url = "https://min-api.cryptocompare.com/data/top/pairs"

            querystring = {"fsym":symbol,"limit":"2000"}

            headers = {
                'cache-control': "no-cache",
                'postman-token': "d0123538-5878-5919-128f-7dda59bb21b4"
            }

            response = requests.request("GET", url, headers=headers, params=querystring)

            #print(response.text)
            data = response.json()
            sub = data['Data']
            df = p.DataFrame(sub)
            df = df.assign (timestamp_api_call = dt.datetime.now(),source = source)
            frames.append(df)

            my_file = cwd+'/data/trading_pair/%s_trading_pair.csv' %symbol

            if os.path.isfile(my_file):
                df_resident = p.DataFrame.from_csv(my_file)
                frames.append(df_resident)
            else:
                pass

            if not df.empty:
                df = p.concat(frames)
                df = df.sort_values('fromSymbol')
                df = df.reset_index(drop=True)
                my_file = my_file.replace('python/','')
                df.to_csv(my_file, index_label='Id') #need to add this
                print 'Updated trade pair: '+str(my_file)
            else:
                pass
        for x in append_list:
            self.symbols.remove(x)


    def main(self):
        """

        :return:
        """
        print 'begin: GetTradePair.main'
        try:
            gtp = GetTradePair(self.symbols)
            gtp.trading_partners()





        except:
            print 'Error: GetTradePair.main'

        print 'end: GetTradePair.main'


if __name__ == '__main__':
    """

    :return:
    """
    runner = GetTradePair()
    runner.main()



