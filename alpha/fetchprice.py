#!/usr/bin/env python

__author__ = 'jkail'

import requests
import pandas as p
import datetime as dt
import os
import threading
from tqdm import tqdm
from time import sleep

class GetDtlPrice(object):

    def __init__(self, symbol_list, exchanges, chunksize):
        self.symbol_list = symbol_list
        self.chunksize = chunksize
        self.exchanges = exchanges

    def get_price_details_for_symbols(self,symbol,frames,error_symbols):
        for exchange in self.exchanges:

            url = "https://min-api.cryptocompare.com/data/pricemultifull"

            querystring = {"fsyms":symbol,"tsyms":'USD',"e":exchange}

            headers = {
                'cache-control': "no-cache",
                'postman-token': "f3d54076-038b-9e2d-1ff3-593ae13aabbf"
            }
            try:
                response = requests.request("GET", url, headers=headers, params=querystring)

                if response.status_code == 200:
                    data = response.json()
                    #print data
                    #print keys
                    #print data[1]
                    #if data["RAW"]:
                    #print data.keys()[0]
                    if data.keys()[0] == "RAW":
                        df = p.DataFrame(data["RAW"][symbol])
                        df = p.DataFrame.transpose(df)
                        df = df.assign (coin = symbol, coin_units = 1, timestamp_api_call = dt.datetime.now(),computer_name = 'JordanManual')
                        frames.append(df)
                    else:
                        pass
                else:
                    pass
            except Exception as e:
                print(e) # This is the correct syntax
                error_symbols.append(symbol)
                #.append(symbol)
                sleep(0.2)


    def main(self):
        """

        :return:
        """
        frames = []
        error_symbols = []
        print 'begin: GetDtlPrice.main'
        try:

            cwd = os.getcwd()
            gdl = GetDtlPrice(self.symbol_list,self.exchanges,self.chunksize)

            xsymbols = [self.symbol_list[x:x+self.chunksize] for x in xrange(0, len(self.symbol_list), self.chunksize )]
            for  symbol_list in tqdm(xsymbols,desc='get_price_details_for_symbols'):

                threads = [threading.Thread(target=gdl.get_price_details_for_symbols, args=(symbol,frames,error_symbols,)) for symbol in symbol_list]

                for thread in threads:
                    thread.start()

                ##for thread in tqdm(threads,desc='Closed Threads'):
                for thread in threads:
                    thread.join()

                if len(error_symbols) > 0:
                    xsymbols.append(error_symbols)
                    print 'appending: errors: '+ str(error_symbols)
                    error_symbols = []
                else:
                    pass


            my_file = cwd+'/data/current_dtl_price.csv'
            if os.path.isfile(my_file):
                df_resident = p.read_csv(my_file)
                frames.append(df_resident)

            else:
                pass


            df = p.concat(frames)


            if not df.empty:
                df = df.drop_duplicates(['FROMSYMBOL','LASTUPDATE','LASTMARKET','MARKET'], keep='last')
                df = df.sort_values('LASTUPDATE')
                df = df.reset_index(drop=True)
                df.to_csv(my_file, index = False) #need to add this
            else:
                pass
        except Exception as e:
            print(e)
            print 'Error: GetDtlPrice.main'

    print 'end: GetDtlPrice.main'


if __name__ == '__main__':
    """

    :return:
    """
    #exchanges =['Bitfinex','Bitstamp','coinone','Coinbase','CCCAGG']
    #cwd = os.getcwd()
    #df = p.read_csv(cwd+'/data/coinlist_info.csv')
    #ls_has = df["Symbol"].tolist()
    #ls_has = ls_has
    #ls_has = ['BTC','BCH','LTC','ETH']
    #print len(ls_has)
    #ls_has, 200, exchanges
    #start_time = dt.datetime.now()

    runner = GetDtlPrice()
    runner.main()
    #x =  dt.datetime.now() - start_time
    #print 'Completion time: '+str(x)


    # 42 seconds 100 records