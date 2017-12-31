#!/usr/bin/env python

__author__ = 'jkail'

import requests
import pandas as p
import datetime as dt
import os
import threading
from tqdm import tqdm
from time import sleep


class GetMineData(object):

    def __init__(self,cwd):
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
                keys = data['CoinData'].keys()

                for key in tqdm(keys,desc='coin_miner_data'):
                    frames = []
                    #print key
                    sub =  data['CoinData'][key]
                    #print sub
                    #print '----------'

                    df = p.DataFrame.from_dict(sub,orient='Index', dtype=None)
                    df = p.DataFrame.transpose(df)
                    df = df.assign (timestamp_api_call = dt.datetime.now(),source = source,key = key )
                    frames.append(df)

                    my_file = self.cwd+'/data/mining_data/%s_mining.csv' % key

                    if os.path.isfile(my_file):
                        df_resident = p.read_csv(my_file,  encoding= 'utf-8')
                        frames.append(df_resident)
                    else:
                        pass
                    #print df
                    df = p.concat(frames)
                    df = df.drop_duplicates(['CoinName','Points','Type','Points','key'],  keep='last')
                    df = df.sort_values('key')
                    df = df.reset_index(drop=True)
                    #print '--------'
                    #print df
                    if not df.empty:
                        #print my_file
                        df.to_csv(my_file, index = False,  encoding= 'utf-8')
                        pass #print 'Updated: '+str(my_file)
                    else:
                        print 'No '+str(key)+' data: '+key
                print 'DONE'
            else:
                print 'No coin_miner_data'
        except ValueError:
            print ValueError


    def miner_data(self):
        try:
            data = self.data
            
            source = 'cryptocompare'
            if data["MiningData"]:
                keys = data['MiningData'].keys()
                frames = []

                for key in tqdm(keys,desc='miner_data'):
                    #print '----------'
                    #print key
                    sub =  data['MiningData'][key]
                    #print '----------'

                    df = p.DataFrame.from_dict(sub,orient='Index', dtype=None)
                    df = p.DataFrame.transpose(df)
                    df = df.assign (timestamp_api_call = dt.datetime.now(),source = source,key = key )
                    frames.append(df)

                my_file = self.cwd+'/data/mining_data/mining_equipment.csv'

                if os.path.isfile(my_file):
                    df_resident = p.read_csv(my_file,  encoding= 'utf-8')
                    frames.append(df_resident)
                else:
                    pass

                df = p.concat(frames)
                df = df.drop_duplicates(['Company','Cost','CurrenciesAvailable','HashesPerSecond','Name'],  keep='last')
                df = df.sort_values('key')
                df = df.reset_index(drop=True)

                if not df.empty:
                    df.to_csv(my_file, index = False,  encoding= 'utf-8' ) #need to add this
                    pass #print 'Updated: '+str(my_file)
                else:
                    print 'No data: '
                print 'DONE'
            else:
                print 'no miner_data'
        except Exception as e:
            print(e)

    def main(self):
        """

        :return:
        """
        print 'begin: GetMineData.main'
        try:
            gmd = GetMineData(self.cwd)
            gmd.coin_miner_data()
            gmd.miner_data()

        except Exception as e:
            print(e)

        #print 'end: GetMineData.main'


if __name__ == '__main__':
    symbols = ['BTC','BCH','LTC','ETH']
    """

    :return:
    """
    runner = GetMineData() #pass symbols to run test in place
    runner.main()


