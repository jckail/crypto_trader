#!/usr/bin/env python

__author__ = 'jkail'

import requests
import pandas as p
import os
import threading


class HasPricingCheck(object):

    def __init__(self, symbol_list, y):
        self.symbol_list = symbol_list
        self.has_pricing = y

    def validate_price_info(self, symbol):

        url = "https://min-api.cryptocompare.com/data/pricemultifull"

        querystring = {"fsyms":symbol,"tsyms":'USD',"e":"CCCAGG"}

        headers = {
            'cache-control': "no-cache",
            'postman-token': "f3d54076-038b-9e2d-1ff3-593ae13aabbf"
        }
        response = requests.request("GET", url, headers=headers, params=querystring)
        data = response.json()

        if 'RAW' in data.keys():
            self.has_pricing.append({'symbol':str(symbol), 'has_pricing':1})

        else:
            self.has_pricing.append({'symbol':str(symbol),'has_pricing':0})

        #print len(self.has_pricing)
    def main(self):
        cwd = os.getcwd()
        print 'run main'
        print 'begin: HasPricingCheck.main'

        hpc = HasPricingCheck(self.symbol_list,self.has_pricing)

        threads = [threading.Thread(target=hpc.validate_price_info, args=(symbol,)) for symbol in self.symbol_list]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        frames = []
        df = p.DataFrame(self.has_pricing)
        frames.append(df)

        my_file = cwd+'/data/has_pricing.csv'

        if os.path.isfile(my_file):
            df_resident = p.DataFrame.from_csv(my_file)
            frames.append(df_resident)
        else:
            pass

        if not df.empty:
            df = p.concat(frames)
            df = df.sort_values('symbol')
            df = df.drop_duplicates(['symbol'], keep='last')
            df = df.reset_index(drop=True)
            df.to_csv(my_file, index_label='Id') #need to add this
            #print 'Updated trade pair: '+str(my_file)
        else:
            pass




if __name__ == '__main__':
    #df_get_id = p.DataFrame.from_csv('/Users/jkail/Documents/GitHub/lit_crypto/alpha/data/coinlist_info.csv')
    #b = df_get_id["Symbol"].tolist()
    #symbols = b
    #has_pricing = []
    runner = HasPricingCheck()
    runner.main()


