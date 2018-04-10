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
import socket
import savetos3
import traceback
import logging
import time

class CoinMarketCap(object):

    def __init__(self, cwd,catalog):

        self.cwd = cwd
        self.catalog = catalog

    def ticker(self):

        frames = []
        url = "https://api.coinmarketcap.com/v1/ticker/"

        querystring = {"limit":"0"}

        headers = {
            'cache-control': "no-cache",
            'postman-token': "a9ed8f5a-9fa5-b77b-6d59-ab97bb499b5e"
        }
        try:
            response = requests.request("GET", url, headers=headers, params=querystring)

            if response.status_code == 200:
                data = response.json()
                df = p.DataFrame(data)
                df = df.assign( utc = time.time(),hostname = socket.gethostname(),source = 'coinmarketcap')
                frames.append(df)

                my_file = self.cwd+'/data/coinmarketcap/ticker.csv'

                if os.path.isfile(my_file):
                    df_resident = p.read_csv(my_file,  encoding= 'utf-8')
                    frames.append(df_resident)
                else:
                    pass
                df = p.concat(frames)
                if not df.empty:
                    df = df.drop_duplicates(['symbol','last_updated'], keep='last')
                    df = df.sort_values('symbol')
                    df = df.reset_index(drop=True)
                    df.to_csv(my_file, index = False,  encoding= 'utf-8') #need to add this
                    s3 = savetos3.SaveS3(my_file,self.catalog)
                    s3.main()
                else:
                    pass
            else:
                pass
        except requests.exceptions.RequestException as e:
            sleep(0.2)
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            pass
        except OverflowError:
            print('OverflowError: ')
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
            pass

    def main(self):
        print ('begin: CoinMarketCap.main')

        try:
            gcl = CoinMarketCap(self.cwd,self.catalog)
            gcl.ticker()

        except Exception as e:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            print (e)


        print ('DONE')


if __name__ == '__main__':
    """

    :return:
    """
    #cwd = os.getcwd()
    #cwd = '/Users/jkail/Documents/GitHub/lit_crypto_data/alpha'
    #catalog = 'litcrypto'
    runner = CoinMarketCap()

    runner.main()


