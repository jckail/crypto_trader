#!/usr/bin/env python

__author__ = 'jkail'

import requests
import pandas as p
import datetime as dt
import os
import threading
from tqdm import tqdm
from time import sleep
import savetos3
import socket
import traceback
import logging


class GetDtlPrice(object):

    def __init__(self, symbol_list, exchanges, chunksize,cwd, catalog):
        self.catalog = catalog
        self.symbol_list = symbol_list
        self.chunksize = chunksize
        self.exchanges = exchanges
        self.cwd = cwd

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
                    if list(data.keys())[0] == "RAW":
                        df = p.DataFrame(data["RAW"][symbol])
                        df = p.DataFrame.transpose(df)
                        df = df.assign (coin = symbol, timestamp_api_call = dt.datetime.now(),hostname = socket.gethostname(), exchange = exchange)
                        frames.append(df)
                    else:
                        pass
                else:
                    pass
            except requests.exceptions.RequestException as e:
                #print(e)
                logging.info('------')
                logging.error(traceback.format_exc())
                logging.info('------')
                logging.exception(traceback.format_exc())
                logging.info('------')
                error_symbols.append(symbol)
                #.append(symbol)
                sleep(0.2)
                #print 'request error'
                pass
            except OverflowError:
                print('OverflowError: '+str(symbol))
                logging.info('------')
                logging.error(traceback.format_exc())
                logging.info('------')
                logging.exception(traceback.format_exc())
                logging.info('------')
                pass
            except Exception as e:
                logging.info('------')
                logging.error(traceback.format_exc())
                logging.info('------')
                logging.exception(traceback.format_exc())
                logging.info('------')
                #print(e)
                #print 'exception'
                pass


    def main(self):
        """

        :return:
        """
        frames = []
        error_symbols = []
        print ('BEGIN: GetDtlPrice.main')
        try:

            gdl = GetDtlPrice(self.symbol_list, self.exchanges, self.chunksize, self.cwd,self.catalog)

            xsymbols = [self.symbol_list[x:x+self.chunksize] for x in range(0, len(self.symbol_list), self.chunksize )]
            for  symbol_list in tqdm(xsymbols,desc='get_price_details_for_symbols'):

                threads = [threading.Thread(target=gdl.get_price_details_for_symbols, args=(symbol,frames,error_symbols,)) for symbol in symbol_list]

                for thread in threads:
                    thread.start()

                ##for thread in tqdm(threads,desc='Closed Threads'):
                for thread in threads:
                    thread.join()

                    if len(error_symbols) > 0:
                        xsymbols.append(error_symbols)
                        #print 'appending: errors: '+ str(error_symbols)
                        error_symbols = []
                    else:
                        pass

            my_file = self.cwd+'/data/pricedetails/price.csv'
            if os.path.isfile(my_file):
                df_resident = p.read_csv(my_file,  encoding= 'utf-8')
                frames.append(df_resident)
            else:
                pass

            if len(frames) > 0:
                df = p.concat(frames)

                if not df.empty:
                    df = df.drop_duplicates(['FROMSYMBOL','LASTUPDATE','LASTMARKET','MARKET'], keep='last')
                    df = df.sort_values('LASTUPDATE')
                    df = df.reset_index(drop=True)
                    df.to_csv(my_file, index = False,  encoding= 'utf-8') #need to add this
                    s3 = savetos3.SaveS3(my_file,self.catalog)
                    s3.main()
                else:
                    pass
            else:
                pass
            print('DONE')

        except Exception as e:
            print(e)
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            print('Error: GetDtlPrice.main')


if __name__ == '__main__':
    """

    :return:
    """
    # exchanges =['Bitfinex','Bitstamp','coinone','Coinbase','CCCAGG']
    # cwd = '/Users/jkail/Documents/GitHub/lit_crypto_data/alpha/'
    # #df = p.read_csv(cwd+'/data/coininfo/coininfo.csv')
    # #ls_has = df["Symbol"].tolist()
    # #ls_has = ls_has[:100]
    # symbol_list = ['BTC','BCH','LTC','ETH','XRP']
    # chunksize = 50
    # catalog = 'litcryptodata'
    #
    # runner = GetDtlPrice(symbol_list, exchanges, chunksize, cwd, catalog)
    runner = GetDtlPrice()
    runner.main()
    #x =  dt.datetime.now() - start_time
    #print 'Completion time: '+str(x)


    # 42 seconds 100 records