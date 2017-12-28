#!/usr/bin/env python

__author__ = 'jkail'

import requests
import pandas as p
import datetime as dt
import os
import threading
from tqdm import tqdm


class GetDtlPrice(object):

    def __init__(self, symbol_list, chunksize, exchanges):
        self.symbol_list = symbol_list
        self.chunksize = chunksize
        self.exchanges = exchanges

    def get_price_details_for_symbols(self,symbol,frames):
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
                    keys = data.keys()

                    if 'RAW' in keys:
                        data = data['RAW']
                        test_df = p.DataFrame.from_dict(data[symbol],orient='Columns', dtype=None)
                        test_df = p.DataFrame.transpose(test_df)
                        test_df = test_df.assign (coin = symbol, coin_units = 1, timestamp_api_call = dt.datetime.now(),computer_name = 'JordanManual')
                        frames.append(test_df)
                    else:
                        pass
                else:
                    pass


            except requests.exceptions.RequestException as e:  # This is the correct syntax
                print e

    def main(self):
        """

        :return:
        """
        frames = []
        print 'begin: GetDtlPrice.main'
        try:

            cwd = os.getcwd()
            gdl = GetDtlPrice(self.symbol_list,self.chunksize,self.exchanges)
            xsymbols = [self.symbol_list[x:x+self.chunksize] for x in xrange(0, len(self.symbol_list), self.chunksize )]
            for  symbol_list in tqdm(xsymbols,desc='get_price_details_for_symbols'):

                threads = [threading.Thread(target=gdl.get_price_details_for_symbols, args=(symbol,frames,)) for symbol in symbol_list]

                for thread in threads:
                    thread.start()

                for thread in tqdm(threads,desc='Closed Threads'):
                    thread.join()


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
                    df.to_csv(my_file, index_label='Sequence') #need to add this
                else:
                    pass


        except:
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
    #ls_has = ls_has[:1]
    #print len(ls_has)
    #ls_has, 200, exchanges
    runner = GetDtlPrice()
    runner.main()


