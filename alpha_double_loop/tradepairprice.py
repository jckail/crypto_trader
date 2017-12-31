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

    def __init__(self, symbol_list, exchanges, chunksize,cwd,trade_pair,exchange_trade_pair):
        self.symbol_list = symbol_list
        self.chunksize = chunksize
        self.exchanges = exchanges
        self.cwd = cwd
        self.trade_pair = trade_pair
        self.exchange_trade_pair = exchange_trade_pair

    def get_price_details_for_symbols(self,symbol,frames,error_symbols):
        try:
            for exchange in symbol:
                symbol = symbol[exchange]
                for x in symbol:
                    for symbol in x.keys():
                        xlist = x[symbol]
                        xsymbols = [xlist[x:x+50] for x in xrange(0, len(xlist), 50 )]
                        for xsym in xsymbols:

                            xsym = "'"+','.join(xsym)+"'"

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
                                        df = df.assign (coin = symbol, coin_units = 1, timestamp_api_call = dt.datetime.now(),computer_name = 'JordanManual')
                                        frames.append(df)

                                    else:
                                        pass
                                else:
                                    pass
                            except requests.exceptions.RequestException as e:
                                #print(e)
                                error_symbols.append(symbol)
                                #.append(symbol)
                                sleep(0.2)
                                #print 'request error'
                                pass
                            except OverflowError:
                                print 'OverflowError: '+str(symbol)
                                pass
                            except Exception as e:
                                #print(e)
                                #print 'exception'
                                pass
        except Exception as e:
            #print(e)
            #print 'exception'
            pass


    def main(self):
        """

        :return:
        """
        frames = []
        error_symbols = []
        print 'begin: GetDtlPrice.main'
        try:

            gdl = GetDtlPrice(self.symbol_list, self.exchanges, self.chunksize, self.cwd,self.trade_pair,self.exchange_trade_pair)


            xsymbols = [self.exchange_trade_pair[x:x+self.chunksize] for x in xrange(0, len(self.exchange_trade_pair), self.chunksize )]
            for symbol_list in tqdm(xsymbols,desc='get_price_details_for_symbols'):

                threads = [threading.Thread(target=gdl.get_price_details_for_symbols, args=(symbol,frames,error_symbols,)) for symbol in symbol_list]

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

            my_file = self.cwd+'/data/current_dtl_price.csv'
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
                df.to_csv(my_file, index = False,  encoding= 'utf-8') #need to add this
            else:
                pass
            print 'DONE'

        except Exception as e:
            print(e)
            print 'Error: GetDtlPrice.main'

    print 'end: GetDtlPrice.main'


if __name__ == '__main__':
    """

    :return:
    """
    exchanges =['Bitfinex','Bitstamp','coinone','Coinbase','CCCAGG']
    cwd = '/Users/jkail/Documents/GitHub/lit_crypto/alpha/'
    df = p.read_csv(cwd+'/data/coinlist_info.csv')
    ls_has = df["Symbol"].tolist()
    ls_has = ls_has[:300]
    ls_has = ['BTC','BCH','LTC','ETH']
    #print len(ls_has)
    ls_has, 200, exchanges
    start_time = dt.datetime.now()
    exchange_trade_pair = [{'CCCAGG': [{'BTC': ['THB', 'THB', 'BTC', 'KCS', 'AUD', 'THB', 'RUR', 'LTC', 'WAVES', 'SGD', 'PLN', 'BNB', 'CAD', 'WEUR', 'BTC', 'BNB', 'RUB', 'WAVES', 'WUSD', 'CAD', 'WEUR', 'EUR', 'KRW', 'ETH', 'USD', 'BNB', 'RUB', 'WUSD', 'RUR', 'THB', 'PLN', 'JPY', 'WUSD', 'WAVES', 'BTC', 'USD', 'KRW', 'USDT', 'EUR', 'DOGE', 'KCS', 'USDT', 'WUSD', 'GBP', 'ETH', 'RUR', 'LTC', 'WEUR', 'DOGE', 'KCS', 'JPY', 'USDT', 'BTC', 'USD', 'KRW', 'USDT', 'EUR', 'ETH', 'AUD', 'EUR', 'CAD', 'BNB', 'GBP', 'RUB', 'RUR', 'USD', 'PLN', 'RUB', 'WUSD', 'WAVES', 'RUB', 'GBP', 'BNB', 'PLN', 'CAD', 'LTC', 'THB', 'JPY', 'WAVES', 'EUR', 'USDT', 'KRW', 'USD', 'BTC', 'AUD', 'RUR', 'ETH', 'WEUR', 'KCS', 'JPY', 'ETH', 'JPY', 'CAD', 'NZDT', 'NZDT', 'AUD', 'KRW', 'DOGE', 'NZDT', 'KZT', 'TZS', 'CAD', 'NZD', 'VEF', 'SEK', 'UAH', 'NGN', 'CNY', 'ZAR', 'AUD', 'PLN', 'AIC', 'VND', 'KES', 'WUSD', 'PHP', 'WUSD', 'PAB', 'RUR', 'UAH', 'SAR', 'COP', 'USD', 'PAB', 'CHF', 'NZD', 'RUR', 'THB', 'BRL', 'BRL', 'HKD', 'INR', 'MXN', 'CNY', 'CAD', 'AUD', 'GBP', 'PLN', 'AIC', 'VND', 'KRW', 'EUR', 'USDT', 'JPY', 'USD', 'BRL', 'MXN', 'TRY', 'IDR', 'SAR', 'SGD', 'ZAR', 'CAD', 'PHP', 'PEN', 'KES', 'PAB', 'TZS', 'NOK', 'RON', 'DKK', 'AED', 'MAD', 'KZT', 'COP', 'WUSD', 'PKR', 'IRR', 'THB', 'HKD', 'WEUR', 'CHF', 'NZD', 'GOLD', 'VND', 'SGD', 'USDT', 'CNY', 'WEUR', 'INR', 'MYR', 'AIC', 'IDR', 'MXN', 'THB', 'EUR', 'JPY', 'PKR', 'KES', 'ILS', 'RUB', 'KRW', 'PLN', 'AUD', 'NZDT', 'VEF', 'SEK', 'UAH', 'ILS', 'NGN', 'RUR', 'GBP', 'INR', 'RUB', 'VEF', 'GOLD', 'KRW', 'IDR', 'CAD', 'TRY', 'MYR', 'EUR', 'UAH', 'JPY', 'RUR', 'NGN', 'ILS', 'UAH', 'SEK', 'VEF', 'NZDT', 'GOLD', 'CNY', 'NZD', 'AED', 'PKR', 'SAR', 'TRY', 'IRR', 'COP', 'WUSD', 'KES', 'CHF', 'INR', 'MYR', 'IDR', 'USDT', 'USD', 'JPY', 'USDT', 'EUR', 'KRW', 'VND', 'AIC', 'PLN', 'GBP', 'AUD', 'ZAR', 'CAD', 'PHP', 'HKD', 'BRL', 'THB', 'MXN', 'RUB', 'PAB', 'TZS', 'SGD', 'RON', 'MXN', 'THB', 'IDR', 'SGD', 'EUR', 'PHP', 'BRL', 'INR', 'HKD', 'TZS', 'PEN', 'NOK', 'ILS', 'GBP', 'RUR', 'SGD', 'PEN', 'USD', 'GBP', 'VND', 'RUB', 'KRW', 'NOK', 'MAD', 'WEUR', 'KZT', 'DKK', 'USD', 'NZDT', 'ZAR', 'USDT', 'JPY', 'SEK', 'MYR', 'AIC', 'NGN', 'MYR', 'PLN', 'RUB', 'AUD', 'WEUR', 'INR', 'WAVES', 'MXN', 'MYR', 'LTC', 'VND', 'NZDT', 'DOGE', 'CHF', 'PLN', 'GBP', 'WEUR', 'PKR', 'WUSD', 'NGN', 'LKK', 'INR', 'BCH', 'RUR', 'KES', 'CHF', 'GBP', 'PLN', 'BCH', 'TZS', 'MXN', 'WAVES', 'UAH', 'LTC', 'NZDT', 'NGN', 'LTC', 'LKK', 'KRW', 'RUB', 'VND', 'THB', 'JPY', 'AIC', 'RUB', 'MXN', 'EUR', 'PLN', 'THB', 'HSR', 'AUD', 'DOGE', 'SGD', 'CAD', 'RUR', 'WTRY', 'USDT', 'BTC', 'AIC', 'VND', 'EUR', 'KRW', 'USDT', 'USD', 'USD', 'HKD', 'NGN', 'LTC', 'RUR', 'SGD', 'CAD', 'AUD', 'UAH', 'UAH', 'RUB', 'HKD', 'WUSD', 'CAD', 'UAH', 'SGD', 'CAD', 'JPY', 'AIC', 'MYR', 'RUR', 'BTC', 'JPY', 'USD', 'RUB', 'USDT', 'EUR', 'AUD', 'WAVES', 'THB', 'KRW', 'AIC', 'USDT', 'MYR', 'PLN', 'WUSD', 'WEUR', 'WEUR', 'EUR', 'MYR', 'INR', 'KRW', 'BTC', 'AUD', 'VND', 'BTC', 'JPY', 'HKD', 'USD', 'DOGE', 'THB', 'MXN', 'CHF', 'USD', 'USD', 'GBP', 'USD', 'USD', 'RUB', 'THB', 'VND', 'PLN', 'BNB', 'CAD', 'BRL', 'AUD', 'AIC', 'ETH', 'USDT', 'KRW', 'WAVES', 'BRL', 'VND', 'AIC', 'ETH', 'EUR', 'EUR', 'KRW', 'USD', 'KRW', 'CAD', 'DOGE', 'OCL', 'USDT', 'BTC', 'XMR', 'WUSD', 'ILS', 'WAVES', 'OCL', 'WEUR', 'NEO', 'ILS', 'AIC', 'BRL', 'ETH', 'ZEC', 'EUR', 'VND', 'AIC', 'RUR', 'BNB', 'EUR', 'ETH', 'USD', 'XMR', 'BTC', 'WUSD', 'NEO', 'WEUR', 'HKD', 'NZDT', 'GBP', 'WAVES', 'THB', 'RUR', 'BNB', 'USD', 'THB', 'ZEC', 'DOGE', 'WUSD', 'NEO', 'WEUR', 'HKD', 'NZDT', 'GBP', 'WAVES', 'PLN', 'RUR', 'XMR', 'NZDT', 'RUB', 'PLN', 'BNB', 'CAD', 'BRL', 'AUD', 'AIC', 'VND', 'ETH', 'EUR', 'KRW', 'USDT', 'BTC', 'THB', 'PLN', 'ILS', 'AUD', 'RUB', 'BRL', 'USD', 'BNB', 'USDT', 'XMR', 'ILS', 'OCTANOX', 'BTC', 'RUB', 'PLN', 'DOGE', 'WUSD', 'WEUR', 'AUD', 'CAD', 'RUR', 'THB', 'BTC', 'USD', 'KRW', 'AUD', 'DOGE', 'VND', 'CAD', 'WAVES', 'RUB', 'USDT', 'RUR', 'CAD', 'RUR', 'CHF', 'JPY', 'SGD', 'CAD', 'SGD', 'RUB', 'RUR', 'JPY', 'RUB', 'RUB', 'CAD', 'SGD', 'CHF', 'RUB', 'RUR', 'JPY', 'RUB', 'RUB', 'RUB', 'USD', 'USD', 'USD', 'USD']}]}]


    runner = GetDtlPrice(ls_has,exchanges,100,cwd,exchange_trade_pair,exchange_trade_pair)
    #runner = GetDtlPrice()
    runner.main()
    #x =  dt.datetime.now() - start_time
    #print 'Completion time: '+str(x)


    # 42 seconds 100 records