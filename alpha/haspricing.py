#!/usr/bin/env python

__author__ = 'jkail'

import requests
import pandas as p
import datetime as dt
import os


class HasPricingCheck(object):

    def __init__(self,gcl_output, runfocus_symbols_only, focus_symbols):
        self.gcl_output = gcl_output
        self.focus_symbols = focus_symbols
        self.runfocus_symbols_only = runfocus_symbols_only

    def validate_price_info(self):
        df_gcl_output = self.gcl_output
        focus_symbols = self.focus_symbols
        runfocus_symbols_only = self.runfocus_symbols_only

        if runfocus_symbols_only == 'Y':
            symbol_list = focus_symbols
        else:
            x = 10
            symbol_list = df_gcl_output["Symbol"].tolist()
            symbol_list = symbol_list[:x] + focus_symbols

        print symbol_list
        has_pricing =[]
        cwd = os.getcwd()
        count = 0
        total_symbols = len(symbol_list)
        print
        print "Started: "+str(dt.datetime.now())
    
        for symbol in symbol_list:
            count += 1
            url = "https://min-api.cryptocompare.com/data/pricemultifull"
    
            querystring = {"fsyms":symbol,"tsyms":'USD',"e":"CCCAGG"}
    
            headers = {
                'cache-control': "no-cache",
                'postman-token': "f3d54076-038b-9e2d-1ff3-593ae13aabbf"
            }
            response = requests.request("GET", url, headers=headers, params=querystring)
            data = response.json()
    
            if 'RAW' in data.keys():
                has_pricing.append({'symbol':str(symbol), 'has_pricing':1})
    
            else:
                has_pricing.append({'symbol':str(symbol),'has_pricing':0})
            print symbol+': '+str(count)+' / '+str(total_symbols)
    
        df_has_pricing = p.DataFrame(has_pricing)
        df_has_pricing.to_csv(cwd+'/data/has_pricing.csv',encoding='utf-8', index = False)
        print "Ended: "+str(dt.datetime.now())

    def main(self):

        print 'run main'
        print 'begin: HasPricingCheck.main'
        hpc = HasPricingCheck(self.gcl_output, self.runfocus_symbols_only, self.focus_symbols)
        try:
            hpc.validate_price_info()

        except:
            print 'error validate_price_info failed'

        print 'end: HasPricingCheck.main'


if __name__ == '__main__':
    runner = HasPricingCheck()
    runner.main()


