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
import time


class GetMineData(object):

    def __init__(self,cwd,catalog):
        self.catalog = catalog
        self.cwd = cwd
        url = "https://www.cryptocompare.com/api/data/miningequipment/"
        headers = {
            'cache-control': "no-cache",
            'postman-token': "f2ac0486-84d0-6e4c-a3e7-ba91f5f35897"
        }
        response = requests.request("GET", url, headers=headers)
        data = response.json()

        self.data = data


    def coin_miner_data(self):
        try:
            data = self.data
            
            source = 'cryptocompare'

            if data["CoinData"]:
                keys = list(data['CoinData'].keys())

                #for key in tqdm(keys,desc='coin_miner_data'):
                for key in keys:
                    frames = []
                    sub =  data['CoinData'][key]

                    df = p.DataFrame.from_dict(sub,orient='Index', dtype=None)
                    df = p.DataFrame.transpose(df)
                    df = df.assign (utc = time.time(),hostname = socket.gethostname(),source = source,symbol = key )
                    frames.append(df)

                    my_file = self.cwd+'/data/mining_data/coin_miner_data/%s_mining.csv' % key
                    #print(my_file)
                    if os.path.isfile(my_file):
                        df_resident = p.read_csv(my_file,  encoding= 'utf-8')
                        frames.append(df_resident)
                    else:
                        pass
                    df = p.concat(frames)
                    df = df.drop_duplicates(['Symbol','TotalCoinsMined','BlockReward','DifficultyAdjustment','BlockRewardReduction','BlockNumber','PreviousTotalCoinsMined'],  keep='last')
                    df = df.sort_values('symbol')
                    df = df.reset_index(drop=True)
                    if not df.empty:
                        df.to_csv(my_file, index = False,  encoding= 'utf-8')
                        s3 = savetos3.SaveS3(my_file,self.catalog)
                        s3.main()
                    else:
                        print ('No '+str(key)+' data: '+key)

            else:
                print ('No coin_miner_data')
        except Exception as e:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            print (e)
        #print('x')


    def miner_data(self):
        try:
            data = self.data
            
            source = 'cryptocompare'
            if data["MiningData"]:
                keys = list(data['MiningData'].keys())
                frames = []

                #for key in tqdm(keys,desc='miner_data'):
                for key in keys:
                    #print '----------'
                    #print key
                    sub =  data['MiningData'][key]
                    #print '----------'


                    df = p.DataFrame.from_dict(sub,orient='Index', dtype=None)
                    df = p.DataFrame.transpose(df)
                    df = df.assign (utc = time.time(),hostname = socket.gethostname(),source = source,symbol = key )
                    frames.append(df)

                my_file = self.cwd+'/data/mining_data/miner_data/mining_equipment.csv'
                #print(my_file)
                if os.path.isfile(my_file):
                    df_resident = p.read_csv(my_file,  encoding= 'utf-8')
                    frames.append(df_resident)
                else:
                    pass

                df = p.concat(frames)
                df = df.drop_duplicates(['Company','Cost','CurrenciesAvailable','HashesPerSecond','Name'],  keep='last')
                df = df.sort_values('CurrenciesAvailable')
                df = df.reset_index(drop=True)

                if not df.empty:
                    df.to_csv(my_file, index = False,  encoding= 'utf-8' ) #need to add this

                    s3 = savetos3.SaveS3(my_file,self.catalog)
                    s3.main()

                    pass #print 'Updated: '+str(my_file)
                else:
                    print ('No data: ')

            else:
                print ('no miner_data')
        except Exception as e:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            print(e)
        #print('y')

    def main(self):
        """

        :return:
        """
        print ('BEGIN: GetMineData.main')
        try:
            gmd = GetMineData(self.cwd,self.catalog)
            gmd.coin_miner_data()
            gmd.miner_data()

        except Exception as e:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            print(e)
        print('DONE')

        #print 'end: GetMineData.main'


if __name__ == '__main__':
    #symbols = ['BTC','BCH','LTC','ETH']
    """

    :return:
    """
    # cwd = '/Users/jkail/Documents/GitHub/lit_crypto_data/alpha'
    # runner = GetMineData(cwd,'litcryptodata') #pass symbols to run test in place
    runner = GetMineData()
    runner.main()


