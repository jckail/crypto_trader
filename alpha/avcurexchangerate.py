import requests
import pandas as p
import datetime as dt
import os
import threading
#import urllib2
import time
from time import sleep
from tqdm import tqdm
import savetos3
import socket
import traceback
import logging
import time


class AvCurExRate(object):

    def __init__(self, avcurs, catalog, chunksize, cwd, top_currencies):
        self.avcurs = avcurs
        self.catalog = catalog
        self.chunksize = chunksize
        self.cwd = cwd
        self.top_currencies = top_currencies

    def get_rate(self,symbol,error_symbols):
        frames = []
        local = []
        for x in self.top_currencies:
            local.append(x)
        for to_currency in local:
            #print(local)
            sleep(2)
            url = "https://www.alphavantage.co/query"

            querystring = {"function":"CURRENCY_EXCHANGE_RATE","from_currency":symbol,"to_currency":to_currency,"apikey":"6258AGUENRIIG1MH"} #6258AGUENRIIG1MH #DEMO

            headers = {
                'cache-control': "no-cache",
                'postman-token': "4e1fbe35-c6a5-ecae-4766-b31f6e63f985"
            }

            try:
                response = requests.request("GET", url, headers=headers, params=querystring)
                #print(response.status_code)

                if response.status_code == 200:
                    data = response.json()
                    keys = list(data.keys())
                    # print(list(data.keys()))
                    # print(data)
                    if 'Information' not in keys and len(keys) > 0 and 'Error Message' not in keys:
                        if data["Realtime Currency Exchange Rate"] != [] :
                            df = p.DataFrame(data["Realtime Currency Exchange Rate"], index=[0])
                            df = df.assign(symbol = symbol,  utc = time.time(), hostname = socket.gethostname(),source = 'alphavantage' )
                            frames.append(df)
                            #print(symbol+' '+str(len(frames))+'/'+str(len(local)))
                        else:
                            pass
                    else:
                        local.append(to_currency)
                        #error_symbols.append(symbol)
                        sleep(5)
                        pass
                else:
                    pass
            except requests.exceptions.RequestException as e:
                error_symbols.append(symbol)
                sleep(0.2)
                logging.info('------')
                logging.error(traceback.format_exc())
                logging.info('------')
                logging.exception(traceback.format_exc())
                logging.info('------')
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

        try:
            if len(frames) > 0:
                my_file = self.cwd+'/data/currency_exchange_rates/'+symbol+'_curexrate.csv'
                if os.path.isfile(my_file):
                    df_resident = p.read_csv(my_file,  encoding= 'utf-8')
                    frames.append(df_resident)
                else:
                    pass
                df = p.concat(frames)
                if not df.empty:
                    df = df.drop_duplicates(['1. From_Currency Code','2. From_Currency Name','6. Last Refreshed'], keep='last')
                    df = df.sort_values('6. Last Refreshed')
                    df = df.reset_index(drop=True)
                    df.to_csv(my_file, index = False,  encoding= 'utf-8') #need to add this
                    s3 = savetos3.SaveS3(my_file,self.catalog)
                    s3.main()
                else:
                    pass
            else:
                pass
        except Exception as e:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            print(e)


    def main(self):
        error_symbols = []
        aver = AvCurExRate(self.avcurs, self.catalog, self.chunksize, self.cwd, self.top_currencies)

        # for symbol in self.top_currencies:
        #     print(self.top_currencies)
        #     aver.get_rate(symbol,error_symbols)


        xsymbols = [self.avcurs[x:x+self.chunksize] for x in range(0, len(self.avcurs), self.chunksize )]

        print('Begin: get_rate')

        for symbol_list in tqdm(xsymbols,desc='get_rate'):

            threads = [threading.Thread(target=aver.get_rate, args=(symbol,error_symbols,)) for symbol in symbol_list]

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





if __name__ == '__main__':
    # cwd = '/Users/jkail/Documents/GitHub/lit_crypto_data/alpha'
    # df = p.read_csv(cwd+'/data/avinfo/avcurrencies.csv')
    # avcurs = df["currency code"].tolist()
    # #avcurs = avcurs[:10000]
    # catalog = 'litcryptodata'
    # chunksize = 200
    #
    # top_currencies = ['USD','EUR','JPY','GBP','KRW','CNY','CAD','HKD','INR']
    # avcurs = top_currencies
    # runner = AvCurExRate(avcurs, catalog, chunksize, cwd ,top_currencies )

    runner = AvCurExRate()
    runner.main()
