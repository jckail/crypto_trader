#!/usr/bin/env python

__author__ = 'jkail'

import requests
import pandas as p
import os
import threading
from tqdm import tqdm
import savetos3
import socket



class HasPricingCheck(object):

    def __init__(self, symbol_list, has_pricing,chunksize):
        self.symbol_list = symbol_list
        self.has_pricing = has_pricing
        self.chunksize = chunksize

    def validate_price_info(self, symbol):

        url = "https://min-api.cryptocompare.com/data/pricemultifull"

        querystring = {"fsyms":symbol,"tsyms":'USD',"e":"CCCAGG"}

        headers = {
            'cache-control': "no-cache",
            'postman-token': "f3d54076-038b-9e2d-1ff3-593ae13aabbf"
        }
        try:
            response = requests.request("GET", url, headers=headers, params=querystring)

            if response.status_code == 200:
                data = response.json()

                if 'RAW' in data.keys():
                    self.has_pricing.append({'symbol':str(symbol), 'has_pricing':1})

                else:
                    self.has_pricing.append({'symbol':str(symbol),'has_pricing':0})
            else:
                pass
        except requests.exceptions.RequestException as e:  # This is the correct syntax
            print e

    def main(self):
        cwd = os.getcwd()
        print 'run main'
        print 'begin: HasPricingCheck.main'

        hpc = HasPricingCheck(self.symbol_list,self.has_pricing,self.chunksize )

        xsymbols = [self.symbol_list[x:x+self.chunksize] for x in xrange(0, len(self.symbol_list), self.chunksize )]

        print 'Begin: validate_price_info'

        for  symbol_list in tqdm(xsymbols,desc='validate_price_info'):

            threads = [threading.Thread(target=hpc.validate_price_info, args=(symbol,)) for symbol in symbol_list]

            for thread in threads:
                thread.start()

            for thread in tqdm(threads,desc='Closed Threads'):
                thread.join()


        frames = []
        df = p.DataFrame(self.has_pricing)
        frames.append(df)

        my_file = cwd+'/data/has_pricing.csv'

        if os.path.isfile(my_file):
            df_resident = p.read_csv(my_file,  encoding= 'utf-8')
            frames.append(df_resident)
        else:
            pass

        if not df.empty:
            df = p.concat(frames)
            df = df.sort_values('symbol')
            df = df.drop_duplicates(['symbol'], keep='last')
            df = df.reset_index(drop=True)
            df.to_csv(my_file, index_label='Sequence',  encoding= 'utf-8') #need to add this
            #print 'Updated trade pair: '+str(my_file)
        else:
            pass




if __name__ == '__main__':
    #df_get_id = p.DataFrame.from_csv('/Users/jkail/Documents/GitHub/lit_crypto/alpha/data/coinlist_info.csv')
    #b = df_get_id["Symbol"].tolist()
    #symbols = b
    #has_pricing = []
    #symbol_list = [u'GLC', u'GLD', u'GLX', u'GLYPH', u'GML', u'GUE', u'HAL', u'HBN', u'HUC', u'COAL', u'HVC', u'DAXX', u'HYP', u'ICB', u'BWK', u'IFC', u'FNT', u'IOC', u'IXC', u'JBS', u'JKC', u'JUDGE', u'KDC', u'KEY', u'KGC', u'LAB*', u'LGD*', u'LK7', u'LKY', u'LSD', u'LTB', u'LTCD', u'LTCX', u'LXC', u'LYC', u'MAX', u'MEC', u'MED', u'ALEX', u'TBCX', u'MIN', u'MCAR', u'THS', u'ACES', u'MINT', u'MN', u'MNC', u'MRY', u'MYST*', u'MZC', u'NAN', u'NAUT', u'NAV', u'NBL', u'NEC', u'NET', u'NMB', u'NRB', u'NOBL', u'NRS', u'NVC', u'NMC', u'NYAN', u'UAEC', u'OPAL', u'EA', u'ORB', u'PIE', u'OSC', u'PHS', u'POINTS', u'POT', u'PSEUD', u'PTS*', u'PXC', u'PYC', u'RDD', u'RIPO', u'RPC', u'RT2', u'RYC', u'RZR', u'SAT2', u'SBC', u'SDC', u'SFR', u'SHADE', u'SHLD', u'SILK', u'SLG', u'SMC', u'SOLE', u'SPA', u'SPT', u'SRC', u'SSV', u'XLM', u'SUPER', u'SWIFT', u'SYNC', u'SYS', u'TAG', u'TAK', u'TES', u'TGC', u'TIT', u'TOR', u'TRC', u'TTC', u'ULTC', u'UNB', u'UNO', u'URO', u'XMRG', u'BTCE', u'FYP', u'BOXY', u'NGC', u'UTN', u'EGAS', u'DPP', u'ADB', u'TGT', u'XDC', u'BMT', u'BIO', u'MTRC', u'BTCL', u'PCN', u'STH', u'CREA', u'WISC', u'BVC', u'FIND', u'PYP', u'RBTC', u'MLITE', u'STALIN', u'TSE', u'USDE', u'UTC', u'UTIL', u'VDO', u'VIA', u'VOOT', u'VRC', u'VTC', u'WC', u'WDC', u'XAI', u'XBOT', u'XBS', u'CRED', u'XC', u'XCASH', u'XCR', u'XJO', u'XLB', u'XPM', u'XPY', u'XRP', u'XST', u'XXX', u'YAC', u'ZCC', u'ZED', u'ZRC*', u'XMR', u'BTS', u'VLTC', u'BIOB', u'SWT', u'PASL', u'SBTC', u'KLK', u'AC3', u'GTO', u'TNB', u'CHIPS*', u'HKN', u'B2B', u'LOC*', u'ZER', u'CHAT', u'CDN', u'MNT*', u'ITNS', u'SMT*', u'BCN', u'EKN', u'XDN', u'XAU', u'TMC', u'XEM', u'BURST', u'NBT', u'SJCX', u'START', u'HUGE', u'XCP']
    #symbol_list,has_pricing,199
    runner = HasPricingCheck()
    print '--------------------------------------------------------------------------'
    runner.main()
    print '--------------------------------------------------------------------------'


