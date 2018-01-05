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
import socket
import traceback
import logging


class GetTradePair(object):

    def __init__(self, symbol_list,chunksize,cwd,catalog):
        self.symbol_list = symbol_list
        self.chunksize = chunksize
        self.cwd = cwd
        self.catalog = catalog


    def trading_partners(self,symbol,error_symbols):
        my_file = self.cwd+'/data/trading_pair/%s_trading_pair.csv' % symbol
        frames = []
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
                    df = df.assign(symbol = symbol, timestamp_api_call = dt.datetime.now(),hostname = socket.gethostname(),source = source )
                    frames.append(df)

                    if os.path.isfile(my_file):
                        df_resident = p.read_csv(my_file,  encoding= 'utf-8')
                        frames.append(df_resident)
                    else:
                        pass
                    df = p.concat(frames)
                    #print(frames)
                    if not df.empty:
                        df = df.drop_duplicates(['exchange','fromSymbol','toSymbol','volume24h','volume24hTo','source','timestamp_api_call'], keep='last')
                        #df = df.sort_values('timestamp_api_call')
                        df = df.reset_index(drop=True)
                        df.to_csv(my_file, index = False,  encoding= 'utf-8') #need to add this
                        s3 = savetos3.SaveS3(my_file,self.catalog)
                        s3.main()
                    else:
                        pass

                else:
                    pass
            else:
                pass
        except requests.exceptions.RequestException as e:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            error_symbols.append(symbol)
            sleep(0.2)
            pass
        except OverflowError:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            print('OverflowError: '+str(symbol))
            pass
        except Exception as e:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            pass



    def main(self):

        print('BEGIN: GetTradePair.main')

        try:
            error_symbols = []
            append_list = ['USD','JPY','USDT','EUR','KRW']
            symbols = self.symbol_list
            source = 'cryptocompare'
            for x in append_list:
                symbols.append(x)
            gtp = GetTradePair(self.symbol_list,self.chunksize,self.cwd,self.catalog)


            xsymbols = [self.symbol_list[x:x+self.chunksize] for x in range(0, len(self.symbol_list), self.chunksize )]

            for symbol_list in tqdm(xsymbols,desc='trading_partners'):

                threads = [threading.Thread(target=gtp.trading_partners, args=(symbol,error_symbols,)) for symbol in symbol_list]

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

            print('DONE')

            for x in append_list:
                self.symbol_list.remove(x)
        except requests.exceptions.RequestException as e:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            print(e)
        except OverflowError:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            print('OverflowError: ')
        except Exception as e:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            print(e)


if __name__ == '__main__':

    # cwd = '/Users/jkail/Documents/GitHub/lit_crypto_data/alpha/'
    # #df = p.read_csv(cwd+'/data/coininfo/coininfo.csv')
    # #ls_has = df["Symbol"].tolist()
    # #ls_has = ls_has[:100]
    # symbol_list = ['BTC','BCH','LTC','ETH','XRP']
    # chunksize = 50
    # catalog = 'litcryptodata'

    #runner = GetTradePair(symbol_list,chunksize,cwd,catalog)
    runner = GetTradePair()
    runner.main()



