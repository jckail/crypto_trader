#!/usr/bin/env python

__author__ = 'jkail'

import requests
import pandas as p
import datetime as dt
import os
from tqdm import tqdm
from time import sleep
from io import StringIO
import boto3
from os.path import basename
import traceback
import logging
import savetos3
import socket
import time
import csv
import zipfile
from io import BytesIO
import quandl
import threading
from quandl.errors.quandl_error import LimitExceededError


class GetStockData(object):

    def __init__(self, cwd,catalog,chunksize):

        self.cwd = cwd
        self.catalog = catalog
        self.chunksize =  25
        self.stocklist = []
        self.og_chunk = chunksize


    def getstocklist(self):

        try:
            frames = []
            my_file = self.cwd+'/data/quandlstocks/dim_stocks/'+'WIKI-datasets-codes.csv'
            if os.path.isfile(my_file):
                df_resident = p.read_csv(my_file,  encoding= 'utf-8')
                frames.append(df_resident)
            else:
                pass

            url = "https://www.quandl.com/api/v3/databases/WIKI/codes?api_key=kzmH8ENEsNUc5GkS9bum"

            request = requests.get(url)
            data = zipfile.ZipFile(BytesIO(request.content))

            x = data.namelist()
            for y in x:
                #print(y)
                my_file = data.extract(y,self.cwd+'/data/quandlstocks/dim_stocks/')

            column_names = ['pattern','description']
            df = p.read_csv(my_file, header = None, names = column_names)
            df = df.reset_index(drop=True)
            descriptionlist =df["description"].tolist()

            mitem = []
            mlocation = []

            for z in descriptionlist:
                #d = z
                z = z.replace('- All T', 'All T')
                x = z.split(" - ")
                mitem.append(x[0])
                if len(x) == 2:
                    mlocation.append(x[1])
                else:
                    mlocation.append(None)

            dl = []
            dl.append(['item',mitem])
            dl.append(['location',mlocation])
            df_desc = p.DataFrame.from_items(dl)

            df = p.concat([df, df_desc], axis=1, join_axes=[df.index])
            df = df.assign (utc = time.time(),hostname = socket.gethostname(),source = 'quandl' )
            frames.append(df)

            df = p.concat(frames)

            if not df.empty:
                df = df.drop_duplicates(['pattern','description'], keep='last')
                df = df.reset_index(drop=True)

                df.to_csv(my_file, index = False,  encoding= 'utf-8') #need to add this
                s3 = savetos3.SaveS3(my_file,self.catalog)
                s3.main()
                x = df.query("(location != location ) or (location == 'All Areas')")
                self.stocklist =x["pattern"].tolist()
                return self.stocklist


                #return stocklist
        except requests.exceptions.RequestException as e:
            print (e)
        except Exception as e:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')

            print(e)

    def get_stocks(self,stock,error_symbols):
        try:
            df = quandl.get(stock, authtoken="kzmH8ENEsNUc5GkS9bum")
            pattern = stock
            stock = stock.replace('WIKI/','')
            df = df.assign (utc = time.time(),hostname = socket.gethostname(),source = 'quandl',pattern = pattern, stock = stock, Date = ''  )

            df['Date'] = df.index



            my_file = self.cwd+'/data/quandlstocks/fact_stocks/'+stock+'.csv'
            frames = []
            frames.append(df)

            if os.path.isfile(my_file):
                df_resident = p.read_csv(my_file, encoding= 'utf-8')
                frames.append(df_resident)
            else:
                pass
            df = p.concat(frames)

            if not df.empty:
                df = df.drop_duplicates(['pattern','Date'], keep='last')
                df = df.reset_index(drop=True)
                df.to_csv(my_file, index = False,  encoding= 'utf-8') #need to add this
                s3 = savetos3.SaveS3(my_file,self.catalog)
                s3.main()

                #print(df)
                pass


        except LimitExceededError as e:
            error_symbols.append(stock)
            #sleep(1)
            #logging.exception(traceback.format_exc())
            # logging.info('------')
            # logging.error(traceback.format_exc())
            # logging.info('------')
            # logging.exception(traceback.format_exc())
            # logging.info('------')
            pass

        except Exception:
            pass

    def main(self):
        print('begin: GetStockData.main')

        try:
            gcl = GetStockData(self.cwd,self.catalog,self.chunksize)
            self.stocklist = gcl.getstocklist()
            error_symbols = []
            xstockist = [self.stocklist[x:x+self.chunksize] for x in range(0, len(self.stocklist), self.chunksize )]

            for stocklist in tqdm(xstockist,desc='Get Stock Info'):

                threads = [threading.Thread(target=gcl.get_stocks, args=(stock,error_symbols,)) for stock in stocklist]

                for thread in threads:
                    #sleep(.3)
                    thread.start()

                for thread in threads:
                    thread.join()

                    if len(error_symbols) > 0:
                        xstockist.append(error_symbols)
                        error_symbols = []
                    else:
                        pass


            print ('DONE')

        except Exception as e:
            logging.error(traceback.format_exc())
            print (e)

if __name__ == '__main__':
    """

    :return:
    """
    cwd = os.getcwd()
    cwd = '/Users/jkail/Documents/GitHub/lit_crypto_data/alpha'
    catalog = 'litcryptodata'
    chunksize = 500
    #runner = GetStockData(cwd,catalog,chunksize)

    runner = GetStockData()
    runner.main()


