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


class GetMinuteHist(object):

    def __init__(self, symbol_list, exchanges, chunksize, cwd,catalog):
        self.catalog = catalog
        self.symbol_list = symbol_list
        self.exchanges = exchanges
        self.chunksize = chunksize
        self.cwd = cwd

    def get_minute_hist(self,symbol,error_symbols):
        pass

    def main(self):
        pass





if __name__ == '__main__':
    # exchanges =['Bitfinex','Bitstamp','coinone','Coinbase','CCCAGG']
    # cwd = '/Users/jkail/Documents/GitHub/lit_crypto_data/alpha'
    # df = p.read_csv(cwd+'/data/coininfo/coininfo.csv')
    # symbol_list = df["Symbol"].tolist()
    # symbol_list = symbol_list[:100]
    # catalog = 'litcryptodata'
    # chunksize = 50
    # runner = GetMinuteHist(symbol_list, exchanges, chunksize, cwd, catalog)
    runner = GetMinuteHist()
    runner.main()
    #print '--------------------------------------------------------------------------'
# x =  dt.datetime.now() - start_time
#print 'Completion time: '+str(x)

#7 seconds 100 records