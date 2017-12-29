#!/usr/bin/env python

__author__ = 'jkail'

import requests
import pandas as p
import datetime as dt
import os
from tqdm import tqdm
from time import sleep


class GetCoinLists(object):

    def __init__(self, runfocus_symbols_only, focus_symbols,cwd):
        self.runfocus_symbols_only = runfocus_symbols_only
        self.focus_symbols = focus_symbols
        self.cwd = cwd

    def func_get_coin_list(self):

        print "Running func_get_coin_list"
        source = "cryptocompare"
        url = "https://min-api.cryptocompare.com/data/all/coinlist"

        headers = {
            'cache-control': "no-cache",
            'postman-token': "a1299df4-9db1-44cc-376e-0357176b776f"
        }

        response = requests.request("GET", url, headers=headers)
        data = response.json()


        df = p.DataFrame.from_dict(data["Data"],orient='index', dtype=None)

        df = df.assign (timestamp_api_call = dt.datetime.now(),source = source )
        df = df.reset_index(drop=True)
        df = df.sort_values('Id')
        df.to_csv(self.cwd+'/data/coinlist_info.csv',encoding='utf-8', index_label='sequence')

        return df

    def main(self):
        """

        :return:
        """
        #y = [1]
        #for x in tqdm(y, desc='get_price_details_for_symbols'):
        print 'begin: GetCoinLists.main'
        #x += 1
        try:
            gcl = GetCoinLists(self.runfocus_symbols_only,self.focus_symbols,self.cwd)
            df = gcl.func_get_coin_list()

        except:
            print 'Error: GetCoinLists.main'

        print 'end: GetCoinLists.main'
        return df


if __name__ == '__main__':
    """

    :return:
    """
    runner = GetCoinLists()

    runner.main()


