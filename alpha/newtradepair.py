#!/usr/bin/env python

__author__ = 'jkail'

import requests
import pandas as p
import datetime as dt
import os
import threading
from time import sleep
from tqdm import tqdm
import savetos3


class GetTradePair(object):

    def __init__(self, symbol_list,chunksize,cwd,trade_pair,exchanges,exchange_trade_pair):
        self.symbol_list = symbol_list
        self.chunksize = chunksize
        self.cwd = cwd
        self.trade_pair = trade_pair
        self.exchanges = exchanges
        self.exchange_trade_pair = exchange_trade_pair


    def trading_partners(self,symbol,error_symbols, frames,big_frame_list):
        frames = []
        my_file = self.cwd+'/data/trading_pair/%s_trading_pair.csv' % symbol

        source = "cryptocompare"
        url = "https://min-api.cryptocompare.com/data/top/pairs"

        querystring = {"fsym":symbol,"limit":"2000"}

        headers = {
            'cache-control': "no-cache",
            'postman-token': "d0123538-5878-5919-128f-7dda59bb21b4"
        }

        try:
            response = requests.request("GET", url, headers=headers, params=querystring)

            if response.status_code == 200:
                data = response.json()

                if data["Data"] != [] and data["Response"] == "Success":
                    df = p.DataFrame(data["Data"])
                    df = df.assign(symbol = symbol, coin_units = 1, timestamp_api_call = dt.datetime.now(),computer_name = 'JordanManual',source = source )
                    frames.append(df)
                    #print df
                    if os.path.isfile(my_file):
                        df_resident = p.read_csv(my_file,  encoding= 'utf-8')
                        frames.append(df_resident)
                    else:
                        pass
                    df = p.concat(frames)

                    if not df.empty:
                        df = df.drop_duplicates(['exchange','fromSymbol','toSymbol','volume24h','volume24hTo','source'], keep='last')
                        df = df.sort_values('fromSymbol')
                        #print df
                        big_frame_list.append(df)
                        #print big_frame_list
                        df.to_csv(my_file, index = False,  encoding= 'utf-8') #need to add this
                        #print 'Updated trade pair: '+str(my_file)
                    else:
                        pass

                else:
                    pass
            else:
                pass
        except requests.exceptions.RequestException as e:
            error_symbols.append(symbol)
            sleep(0.2)
            pass
        except OverflowError:
            print 'OverflowError: '+str(symbol)
            pass
        except Exception as e:
            pass



    def main(self):
        frames = []
        big_frame_list =[]
        print 'begin: GetTradePair.main'

        try:
            error_symbols = []
            gtp = GetTradePair(self.symbol_list, self.chunksize, self.cwd, self.trade_pair,self.exchanges, self.exchange_trade_pair)


            xsymbols = [self.symbol_list[x:x+self.chunksize] for x in xrange(0, len(self.symbol_list), self.chunksize )]

            for symbol_list in tqdm(xsymbols,desc='trading_partners'):

                threads = [threading.Thread(target=gtp.trading_partners, args=(symbol,error_symbols,frames,big_frame_list,)) for symbol in symbol_list]

                for thread in threads:
                    thread.start()

                #for thread in tqdm(threads,desc='Closed Threads'):
                for thread in threads:
                    thread.join()

                    if len(error_symbols) > 0:
                        xsymbols.append(error_symbols)
                        error_symbols = []
                    else:
                        pass

            df = p.concat(big_frame_list)
            #dfs = []
            #ex = []
            for symbol in self.symbol_list:

                x = set(df["exchange"].tolist())
                x = list(x)
                #print x

                for exchange in x:
                    #print exchange
                    raw_exchange = exchange
                    exchange = "'"+exchange+"'"
                    df = df.query('exchange == '+exchange)
                    x = set(df["toSymbol"].tolist())
                    x = list(x)
                    df = df.reset_index(drop=True)
                    self.trade_pair.update({symbol:x})
                    self.exchange_trade_pair.update({raw_exchange:self.trade_pair})

                    #z = self.exchange_#trade_pair
                    #print z.keys()
                #print self.exchange_trade_pair

            print 'DONE'


        except requests.exceptions.RequestException as e:
            print e
        except OverflowError:
            print 'OverflowError: '+str(symbol)
        except Exception as e:
            print e

        #self.exchange_trade_pair = self.exchange_trade_pair
        #print self.exchange_trade_pair



if __name__ == '__main__':

    cwd = '/Users/jkail/Documents/GitHub/lit_crypto/alpha/'
    df = p.read_csv(cwd+'/data/coinlist_info.csv')
    ls_has = df["Symbol"].tolist()
    ls_has = ls_has[:100]
    chunksize = 50
    trade_pair = {}
    exchange_trade_pair ={}
    symbols = ['BTC','BCH','LTC','ETH']
    exchanges = ['Bitfinex','Bitstamp','coinone','Coinbase','CCCAGG']
    #runner = GetTradePair(symbols,1,cwd,trade_pair,exchanges,exchange_trade_pair)
    runner = GetTradePair()
    runner.main()



