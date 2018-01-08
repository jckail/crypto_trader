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

class GetAVCurrencyList(object):

    def __init__(self, cwd,catalog):

        self.cwd = cwd
        self.catalog = catalog
        self.avcurs = []


    def getcurrencylist(self):

        try:
            frames = []
            url = "https://www.alphavantage.co/physical_currency_list/"

            headers = {
                'cache-control': "no-cache",
                'postman-token': "f7f0c0c6-c707-e39e-76ed-f61d9046b7f8"
            }

            response = requests.request("GET", url, headers=headers)

            data = response.content.decode('utf-8')
            cr = csv.reader(data.splitlines(), delimiter=',')
            my_list = list(cr)
            df = p.DataFrame(my_list[1:],columns=my_list[0])

            df = df.assign (utc = time.time(),hostname = socket.gethostname(),source = 'alphavantage' )
            df = df.reset_index(drop=True)
            frames.append(df)


            my_file = self.cwd+'/data/avinfo/avcurrencies.csv'
            if os.path.isfile(my_file):
                df_resident = p.read_csv(my_file,  encoding= 'utf-8')
                frames.append(df_resident)

            else:
                pass

            df = p.concat(frames)

            if not df.empty:
                df = df.drop_duplicates(['currency code','currency name'], keep='last')
                df = df.reset_index(drop=True)
                df.to_csv(my_file, index = False,  encoding= 'utf-8') #need to add this
                s3 = savetos3.SaveS3(my_file,self.catalog)
                s3.main()
                self.avcurs = df["currency code"].tolist()
                return self.avcurs

        except requests.exceptions.RequestException as e:
            print (e)
        except Exception as e:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')

            print(e)

    def main(self):
        print('begin: GetCurrencyList.main')

        try:
            gcl = GetAVCurrencyList(self.cwd,self.catalog)
            self.avcurs = gcl.getcurrencylist()
            print ('DONE')
            return self.avcurs

        except Exception as e:
            logging.error(traceback.format_exc())
            print (e)





if __name__ == '__main__':
    """

    :return:
    """
    # cwd = os.getcwd()
    # cwd = '/Users/jkail/Documents/GitHub/lit_crypto_data/alpha'
    # catalog = 'litcryptodata'
    # runner = GetAVCurrencyList(cwd,catalog)

    runner = GetAVCurrencyList()
    runner.main()


