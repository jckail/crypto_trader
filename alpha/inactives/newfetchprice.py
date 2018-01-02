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


class GetDtlPrice(object):

    def __init__(self,symbol_list, chunksize, cwd, trade_pair, exchanges, exchange_trade_pair):
        self.symbol_list = symbol_list
        self.chunksize = chunksize
        self.cwd = cwd
        self.trade_pair = trade_pair
        self.exchanges = exchanges
        self.exchange_trade_pair = exchange_trade_pair

    def get_price_details_for_symbols(self,symbol,frames,error_symbols,exchange):
        try:


            xlist = self.exchange_trade_pair[exchange][symbol]

            #print symbol
            xsymbols = [xlist[x:x+50] for x in xrange(0, len(xlist), 50 )]

            for xsym in xsymbols:
                # print len(xsym)
                xsym = "'"+','.join(xsym)+"'"
                # print '----------'
                # print len(xlist)
                # print symbol
                # print '----------'
                #print xsym
                url = "https://min-api.cryptocompare.com/data/pricemultifull"

                querystring = {"fsyms":symbol,"tsyms":xsym,"e":exchange}

                headers = {
                    'cache-control': "no-cache",
                    'postman-token': "f3d54076-038b-9e2d-1ff3-593ae13aabbf"
                }
                try:
                    response = requests.request("GET", url, headers=headers, params=querystring)

                    if response.status_code == 200:
                        data = response.json()
                        if data.keys()[0] == "RAW":
                            df = p.DataFrame(data["RAW"][symbol])
                            df = p.DataFrame.transpose(df)
                            df = df.assign (coin = symbol, coin_units = 1, timestamp_api_call = dt.datetime.now(),hostname = socket.gethostname())
                            frames.append(df)

                        else:
                            pass
                    else:
                        pass
                except requests.exceptions.RequestException as e:
                    print(e)
                    error_symbols.append(symbol)
                    #.append(symbol)
                    sleep(0.2)
                    print 'request error'
                    pass
                except OverflowError:
                    print 'OverflowError: '+str(symbol)
                    pass
                except Exception as e:
                    print(e)
                    print 'exception'
                    pass
            else:
                pass
        except Exception as e:
            print(e)
            print 'exception'
            pass


    def main(self):
        """

        :return:
        """

        print 'begin: GetDtlPrice.main'
        try:

            gdl = GetDtlPrice(self.symbol_list,self.chunksize,self.cwd,self.trade_pair,self.exchanges,self.exchange_trade_pair)
            frames = []
            error_symbols = []
            keys = self.exchange_trade_pair.keys()
            dict = self.exchange_trade_pair
            keys = set(keys)
            keys = list(keys)
            symbols = []

            for key in tqdm(keys,desc='exchanges'):
                exchange = key
                #print 'x'
                #print dict[key].keys()
                #print 'x'
                # print '------'
                # print len(dict[key].keys())
                # print '------'
            xsymbols = [dict[key].keys()[x:x+self.chunksize] for x in xrange(0, len(dict[key].keys()), self.chunksize )]

            for symbol_list in tqdm(xsymbols,desc='get_price_details_for_symbols'):

                threads = [threading.Thread(target=gdl.get_price_details_for_symbols, args=(symbol,frames,error_symbols,exchange,)) for symbol in symbol_list]

                for thread in threads:
                    thread.start()

                ##for thread in tqdm(threads,desc='Closed Threads'):
                for thread in tqdm(threads,desc='closed_threads'):
                    thread.join()

                    if len(error_symbols) > 0:
                        xsymbols.append(error_symbols)
                        #print 'appending: errors: '+ str(error_symbols)
                        error_symbols = []
                    else:
                        pass

            my_file = self.cwd+'/data/%s_current_dtl_price.csv' % exchange

            if os.path.isfile(my_file):
                df_resident = p.read_csv(my_file,  encoding= 'utf-8')
                frames.append(df_resident)

            else:
                pass
            # print '--------'
            # print len(frames)
            # print '--------'
            df = p.concat(frames)

            if not df.empty:
                df = df.drop_duplicates(['FROMSYMBOL','LASTUPDATE','LASTMARKET','MARKET'], keep='last')
                df = df.sort_values('FROMSYMBOL')
                df = df.reset_index(drop=True)
                df.to_csv(my_file, index = False,  encoding= 'utf-8') #need to add this
            else:
                pass


        except Exception as e:
            print(e)
            print 'Error: GetDtlPrice.main'
        print 'DONE'



if __name__ == '__main__':
    """

    :return:
    """
    exchanges =['Bitfinex','Bitstamp','coinone','Coinbase','CCCAGG']
    cwd = '/Users/jkail/Documents/GitHub/lit_crypto/alpha'
    df = p.read_csv(cwd+'/data/coininfo/coininfo.csv')
    ls_has = df["Symbol"].tolist()
    ls_has = ls_has[:300]
    #ls_has = ['BTC','BCH','LTC','ETH']
    #print len(ls_has)
    trade_pair = {}
    ls_has, 200, exchanges
    start_time = dt.datetime.now()
    exchange_trade_pair = {u'CCCAGG': {'ETH': [u'USD', u'GBP', u'AUD', u'RUB', u'SGD', u'ZAR', u'CAD', u'BRL', u'PHP', u'HKD', u'MXN', u'TRY', u'PAB', u'USD', u'THB', u'USDT', u'TZS', u'PHP', u'AIC', u'GOLD', u'VND', u'EUR', u'UAH', u'SEK', u'VEF', u'IDR', u'NZD', u'SAR', u'CHF', u'AED', u'PKR', u'COP', u'WUSD', u'MYR', u'THB', u'NZDT', u'PLN', u'JPY', u'USDT', u'KRW', u'IDR', u'EUR', u'MYR', u'SGD', u'BRL', u'AIC', u'GBP', u'PLN', u'AUD', u'SGD', u'NOK', u'CAD', u'PKR', u'JPY', u'USDT', u'EUR', u'HKD', u'SGD', u'MXN', u'INR', u'CAD', u'DKK', u'WEUR', u'TZS', u'AUD', u'INR', u'PLN', u'GBP', u'AIC', u'RUB', u'KRW', u'USDT', u'ILS', u'MYR', u'RUB', u'KES', u'KZT', u'IRR', u'PEN', u'RON', u'NOK', u'MAD', u'JPY', u'MXN', u'RUR', u'CNY', u'EUR', u'USDT', u'USDT', u'EUR', u'KRW', u'VND', u'PLN', u'WUSD', u'NZDT', u'JPY', u'AUD', u'ZAR', u'ILS', u'UAH', u'ILS', u'CAD', u'MYR', u'VND', u'TRY', u'AIC', u'MXN', u'GOLD', u'GBP', u'IDR', u'INR', u'CNY', u'RUR', u'NGN', u'PHP', u'IRR', u'SEK', u'THB', u'VEF', u'CHF', u'SAR', u'AED', u'PKR', u'SGD', u'NGN', u'THB', u'INR', u'RUR', u'USDT', u'SGD', u'EUR', u'JPY', u'GBP', u'WEUR', u'AIC', u'AUD', u'HKD', u'BRL', u'USD', u'MXN', u'UAH', u'UAH', u'CAD', u'GOLD', u'INR', u'GHS', u'KZT', u'MYR', u'RUB', u'JPY', u'PLN', u'CNY', u'WUSD', u'MXN', u'NGN', u'NZD', u'WEUR', u'NGN', u'JPY', u'MXN', u'BRL', u'CAD', u'USD', u'TZS', u'RUR', u'RON', u'KES', u'KRW', u'USD', u'GOLD', u'WUSD', u'ZAR', u'AUD', u'PLN', u'GBP', u'AIC', u'VND', u'KRW', u'EUR', u'USDT', u'JPY', u'USD', u'DKK', u'KZT', u'WEUR', u'TZS', u'MAD', u'NOK', u'CAD', u'RON', u'HKD', u'RUB', u'GBP', u'PLN', u'USDT', u'EUR', u'KRW', u'VND', u'AIC', u'GBP', u'USD', u'NGN', u'JPY', u'THB', u'UAH', u'RUR', u'NGN', u'INR', u'MYR', u'MXN', u'PEN', u'IRR', u'KES', u'RUB', u'MXN', u'HKD', u'PHP', u'BRL', u'CAD', u'ZAR', u'SGD', u'AUD', u'PLN', u'GBP', u'AIC', u'VND', u'KRW', u'EUR', u'USDT', u'JPY', u'THB', u'IDR', u'MYR', u'INR', u'PAB', u'NZDT', u'TRY', u'WUSD', u'COP', u'PKR', u'AED', u'CHF', u'AIC', u'SAR', u'VEF', u'GOLD', u'SEK', u'UAH', u'ILS', u'RUR', u'NGN', u'CNY', u'NZD', u'EUR', u'NGN', u'VND', u'AIC', u'GBP', u'PLN', u'AUD', u'PHP', u'THB', u'RUR', u'IRR', u'RUR', u'BRL', u'USD', u'JPY', u'USDT', u'NGN', u'NGN', u'INR', u'RUB', u'KRW', u'KRW', u'ZAR', u'MXN', u'MYR', u'NGN', u'BRL', u'UAH', u'RUR', u'VND', u'AUD', u'BRL', u'VND', u'BRL', u'AIC', u'THB', u'EUR', u'IDR', u'SEK', u'KRW', u'USD', u'USD', u'VND', u'KRW', u'USD', u'CAD', u'PLN', u'WUSD', u'PKR', u'GBP', u'MXN', u'THB', u'CAD', u'BRL', u'RUB', u'IDR', u'SGD', u'MYR', u'INR', u'PLN', u'RUR', u'ZAR', u'AUD', u'THB', u'VND', u'WEUR', u'WUSD', u'INR', u'AED', u'UAH', u'UAH', u'WUSD', u'TZS', u'THB', u'AUD', u'IDR', u'SGD', u'MYR', u'INR', u'RUR', u'RUB', u'VEF', u'SAR', u'MYR', u'ILS', u'CAD', u'GHS', u'BTC', u'USD', u'KRW', u'USDT', u'EUR', u'ETH', u'JPY', u'THB', u'AUD', u'CAD', u'PLN', u'GBP', u'BTC', u'RUB', u'WAVES', u'WUSD', u'RUR', u'LTC', u'KCS', u'WEUR', u'NEO', u'DOGE', u'BNB', u'KCS', u'RUB', u'USDT', u'EUR', u'ETH', u'JPY', u'THB', u'AUD', u'CAD', u'PLN', u'BNB', u'RUB', u'WAVES', u'NZDT', u'WEUR', u'BTC', u'PLN', u'RUB', u'WAVES', u'RUR', u'SGD', u'BTC', u'PLN', u'USD', u'KRW', u'USD', u'KRW', u'USDT', u'EUR', u'NZDT', u'DOGE', u'NEO', u'WEUR', u'KCS', u'LTC', u'RUR', u'WUSD', u'WAVES', u'RUB', u'BNB', u'GBP', u'PLN', u'CAD', u'AUD', u'THB', u'JPY', u'ETH', u'EUR', u'USDT', u'KRW', u'USD', u'BNB', u'RUR', u'WUSD', u'DOGE', u'BNB', u'WAVES', u'BTC', u'USD', u'KRW', u'USDT', u'EUR', u'ETH', u'WUSD', u'ETH', u'GBP', u'LTC', u'WEUR', u'NEO', u'CAD', u'THB', u'AUD', u'JPY', u'JPY', u'AUD', u'THB', u'CAD', u'RUR', u'NZDT', u'USD', u'KRW', u'EUR', u'ETH', u'AIC', u'VND', u'AUD', u'BRL', u'CAD', u'PLN', u'USDT', u'BNB', u'RUB', u'XMR', u'ILS', u'RUR', u'GBP', u'WAVES', u'NZDT', u'NEO', u'USD', u'THB', u'BTC', u'HKD', u'WEUR', u'THB', u'USD', u'BTC', u'USDT', u'KRW', u'EUR', u'ETH', u'VND', u'AIC', u'AUD', u'BRL', u'CAD', u'PLN', u'BNB', u'THB', u'RUB', u'RUR', u'KRW', u'DOGE', u'PLN', u'DOGE', u'WAVES', u'USD', u'EUR', u'DOGE', u'NEO', u'NZDT', u'WAVES', u'GBP', u'RUR', u'ILS', u'XMR', u'RUB', u'THB', u'BNB', u'PLN', u'CAD', u'BRL', u'AUD', u'AIC', u'VND', u'ETH', u'EUR', u'KRW', u'USDT', u'BTC', u'HKD', u'WUSD', u'WEUR', u'DOGE', u'ILS', u'RUR', u'WAVES', u'NZDT', u'NEO', u'WEUR', u'RUB', u'OCL', u'BTC', u'XMR', u'VND', u'AIC', u'AUD', u'BRL', u'CAD', u'USDT', u'OCTANOX', u'BNB', u'WUSD', u'ETH', u'ZEC', u'WUSD', u'ZEC', u'BTC', u'USD', u'USDT', u'KRW', u'EUR', u'VND', u'AIC', u'JPY', u'AUD', u'CAD', u'SGD', u'HSR', u'THB', u'PLN', u'MXN', u'RUB', u'UAH', u'RUR', u'WAVES', u'HKD', u'WUSD', u'DOGE', u'NZDT', u'CHF', u'NGN', u'WEUR', u'PKR', u'KES', u'BTC', u'UAH', u'VND', u'USDT', u'KRW', u'EUR', u'VND', u'AIC', u'JPY', u'AUD', u'CAD', u'THB', u'PLN', u'MXN', u'RUB', u'LKK', u'RUR', u'WAVES', u'INR', u'RUR', u'USD', u'UAH', u'WUSD', u'CHF', u'WEUR', u'BTC', u'USD', u'USDT', u'KRW', u'EUR', u'LTC', u'AIC', u'LTC', u'PKR', u'WEUR', u'NGN', u'CHF', u'NZDT', u'DOGE', u'WUSD', u'HKD', u'WAVES', u'RUR', u'UAH', u'RUB', u'KES', u'MXN', u'THB', u'HSR', u'SGD', u'CAD', u'AUD', u'JPY', u'AIC', u'VND', u'EUR', u'KRW', u'USDT', u'USD', u'PLN', u'PKR', u'LKK', u'WTRY', u'WAVES', u'TZS', u'GBP', u'MYR', u'BTC', u'HKD', u'WUSD', u'NZDT', u'CHF', u'NGN', u'GBP', u'WEUR', u'INR', u'TZS', u'JPY', u'AUD', u'CAD', u'SGD', u'THB', u'PLN', u'MXN', u'RUB', u'LTC', u'MYR', u'DOGE', u'MYR', u'INR', u'BCH']}}


    #runner = GetDtlPrice(ls_has, 1, cwd, trade_pair, exchanges, exchange_trade_pair)
    runner = GetDtlPrice()
    runner.main()
    #x =  dt.datetime.now() - start_time
    #print 'Completion time: '+str(x)


    # 42 seconds 100 records