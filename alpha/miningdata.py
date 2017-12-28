#!/usr/bin/env python

__author__ = 'jkail'

import requests
import pandas as p
import datetime as dt
import os


class GetMineData(object):

    def __init__(self):

        url = "https://www.cryptocompare.com/api/data/miningequipment/"
        headers = {
            'cache-control': "no-cache",
            'postman-token': "f2ac0486-84d0-6e4c-a3e7-ba91f5f35897"
        }
        response = requests.request("GET", url, headers=headers)
        data = response.json()
        self.data = data
        #print data

    def coin_miner_data(self):

        data = self.data
        cwd = os.getcwd()
        source = 'cryptocompare'

        if data["CoinData"]:
            keys = data['CoinData'].keys()

            for key in keys:
                frames = []
                #print key
                sub =  data['CoinData'][key]
                #print sub
                #print '----------'

                df = p.DataFrame.from_dict(sub,orient='Index', dtype=None)
                df = p.DataFrame.transpose(df)
                df = df.assign (timestamp_api_call = dt.datetime.now(),source = source,key = key )
                frames.append(df)

                my_file = cwd+'/data/mining_data/%s_mining.csv' % key

                if os.path.isfile(my_file):
                    df_resident = p.DataFrame.from_csv(my_file)
                    frames.append(df_resident)
                else:
                    print ''
                #print df
                df = p.concat(frames)
                df = df.drop_duplicates(['CoinName','Points','Type','Points','key'],  keep='last')
                df = df.sort_values('key')
                df = df.reset_index(drop=True)
                #print '--------'
                #print df
                if not df.empty:
                    #print my_file
                    df.to_csv(my_file, index_label='Id')
                    print 'Updated: '+str(my_file)
                else:
                    print 'No '+str(key)+' data: '+key
        else:
            print 'No coin_miner_data'

    def miner_data(self):
        data = self.data
        cwd = os.getcwd()
        source = 'cryptocompare'
        if data["MiningData"]:
            keys = data['MiningData'].keys()
            frames = []

            for key in keys:
                #print '----------'
                #print key
                sub =  data['MiningData'][key]
                #print '----------'

                df = p.DataFrame.from_dict(sub,orient='Index', dtype=None)
                df = p.DataFrame.transpose(df)
                df = df.assign (timestamp_api_call = dt.datetime.now(),source = source,key = key )
                frames.append(df)

            my_file = cwd+'/data/mining_data/mining_equipment.csv'

            if os.path.isfile(my_file):
                df_resident = p.DataFrame.from_csv(my_file)
                frames.append(df_resident)
            else:
                print ''

            df = p.concat(frames)
            df = df.drop_duplicates(['Company','Cost','CurrenciesAvailable','HashesPerSecond','Name'],  keep='last')
            df = df.sort_values('key')
            df = df.reset_index(drop=True)

            if not df.empty:
                df.to_csv(my_file,index_label='Sequence',  encoding= 'utf-8' ) #need to add this
                print 'Updated: '+str(my_file)
            else:
                print 'No data: '
        else:
            print 'no miner_data'

    def main(self):
        """

        :return:
        """
        print 'begin: GetMineData.main'
        try:
            gmd = GetMineData()
            gmd.coin_miner_data()
            gmd.miner_data()


        except:
            print 'Error: GetMineData.main'

        print 'end: GetMineData.main'


if __name__ == '__main__':
    symbols = ['BTC','BCH','LTC','ETH']
    """

    :return:
    """
    runner = GetMineData() #pass symbols to run test in place
    runner.main()


