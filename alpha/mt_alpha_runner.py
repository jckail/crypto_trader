#!/usr/bin/env python

#mods
import argparse
import os
import pandas as p
#from multiprocessing import Pool, TimeoutError
#import timeurllib2urllib2
import threading
#add arg focus symbols only
import datetime as dt
import multiprocessing
import socket
import traceback
from common import *
import logging
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('boto3').setLevel(logging.CRITICAL)
import datetime as dt


# classes
import coinlist
import day_hist
import setup
import fetchprice
import hour_hist
import minute_hist
import miningdata
import tradepair
import fetchprice
import mtsocial
import savetos3
import gluemaintenance
import s3maintenance
import awscatalogcreate
import coinmarketcaptest
import logtos3




class AlphaRunner(object):

    """
    This is the main runner
    """

    def __init__(self):
        self.args = self.get_args()
        self.run = self.args.run
        self.runfocus_symbols_only = self.args.runfocus_symbols_only
        self.runcrawler = self.args.runcrawler
        self.minute_run_param = self.args.minute_run_param

        self.cwd = os.getcwd()
        self.focus_symbols = ['BTC','BCH','LTC','ETH','XRP']
        self.symbol_list = []
        # FULL LIST exchanges = ['Cryptsy', 'BTCChina', 'Bitstamp', 'BTER', 'OKCoin', 'Coinbase', 'Poloniex', 'Cexio', 'BTCE', 'BitTrex', 'Kraken', 'Bitfinex', 'Yacuna', 'LocalBitcoins', 'Yunbi', 'itBit', 'HitBTC', 'btcXchange', 'BTC38', 'Coinfloor', 'Huobi', 'CCCAGG', 'LakeBTC', 'ANXBTC', 'Bit2C', 'Coinsetter', 'CCEX', 'Coinse', 'MonetaGo', 'Gatecoin', 'Gemini', 'CCEDK', 'Cryptopia', 'Exmo', 'Yobit', 'Korbit', 'BitBay', 'BTCMarkets', 'Coincheck', 'QuadrigaCX', 'BitSquare', 'Vaultoro', 'MercadoBitcoin', 'Bitso', 'Unocoin', 'BTCXIndia', 'Paymium', 'TheRockTrading', 'bitFlyer', 'Quoine', 'Luno', 'EtherDelta', 'bitFlyerFX', 'TuxExchange', 'CryptoX', 'Liqui', 'MtGox', 'BitMarket', 'LiveCoin', 'Coinone', 'Tidex', 'Bleutrade', 'EthexIndia', 'Bithumb', 'CHBTC', 'ViaBTC', 'Jubi', 'Zaif', 'Novaexchange', 'WavesDEX', 'Binance', 'Lykke', 'Remitano', 'Coinroom', 'Abucoins', 'BXinth', 'Gateio', 'HuobiPro', 'OKEX']
        self.exchanges = ['Bitfinex','Bitstamp','coinone','Coinbase','CCCAGG']
        #self.exchanges = ['Coinbase']
        self.chunksize = 200  #~~#thread limit 199
        #self.org_params = json.load(open("config/cti_config.dict"))
        self.reddit_ls = []
        self.coderepository_ls = []
        self.twitter_ls = []
        self.cryptocompare_ls = []
        self.general_ls = []
        self.facebook_ls = []
        self.trade_pair = {}
        self.exchange_trade_pair = {}
        self.catalog = ''


        machine = str(socket.gethostname())
        self.logfile = 'litcrypto_'+machine+'.log'
        cwd_split = self.cwd.split('/')
        target_ibdex = cwd_split.index('alpha') # project name
        self.logger_path = '/'.join(cwd_split[:target_ibdex-1])+'/lit_crypto_data/alpha/logs/'

        self.s3_log_file = 'alpha/logs/'+self.logfile
        self.logfile = self.logger_path+self.logfile
        if not os.path.exists(self.logger_path):
            os.makedirs(self.logger_path)
        self.logging = self.intialize_logger()



    def get_args(self):
        """
        :return:
        """
        try:
            parser = argparse.ArgumentParser(usage='alpha_runner.py --run <run> --runfocus_symbols_only <runfocus_symbols_only> --runcrawler <runcrawler> --minute_run_param <minute_run_param>')
            parser.add_argument('--run', required=False, default= 'Y', dest='run', choices=['Y', 'N'], help='want to run this y')
            parser.add_argument('--runfocus_symbols_only', required=False, default= 'N', dest='runfocus_symbols_only', choices=['Y', 'N'], help='runfocus_symbols_only run')
            parser.add_argument('--runcrawler', required= False, default= 'N',dest='runcrawler', choices=['Y', 'N'], help='runfocus_symbols_only run')
            parser.add_argument('--minute_run_param', required= False, default= 'N',dest='minute_run_param', choices=['Y', 'N'], help='minute_run_param run')
            args = parser.parse_args()
            return args
        except Exception as e:
            print(e)


    def intialize_logger(self):
        try:
            logging.basicConfig(filename=self.logfile, level=logging.INFO)
            logging.info ('--------------------------------------------------------------------------')
            logging.info('')
            logging.info('------ logger-----')
            logging.info(dt.datetime.now())
            logging.info('')
            logging.info ('--------------------------------------------------------------------------')
            return logging

        except Exception as e:
            print(e)

    def alpha_runner(self,l):
        if self.run == 'Y':
            print ("Begin alpha_runner")
            try:
                try:
                    if self.runfocus_symbols_only == 'N':
                        cl = coinlist.GetCoinLists(self.cwd,self.catalog)
                        cl.main()
                        df = p.read_csv(self.cwd+'/data/coininfo/coininfo.csv', encoding= 'utf-8')
                        self.symbol_list = df["Symbol"].tolist()

                    elif self.runfocus_symbols_only == 'Y':
                         self.symbol_list = self.focus_symbols
                except Exception as e:
                    logging.info('------')
                    logging.error(traceback.format_exc())
                    logging.info('------')
                    logging.exception(traceback.format_exc())
                    logging.info('------')
                    print(e)
                    print ('error getting self.symbol_list')

                try:

                    #add processing queue
                    #self.symbol_list = self.symbol_list[:10]
                    #
                    x = len(self.symbol_list)
                    # #self.symbol_list.append('SMT')
                    print ('--------------------------------------------------------------------------')
                    print ('Evaluating: '+str(x)+' Coins')
                    print ('--------------------------------------------------------------------------')
                    #helps limit #threads open etc

                    logging.info ('miningdata.GetMineData')
                    md = miningdata.GetMineData(self.cwd,self.catalog)
                    md.main()
                    # #
                    print('--------------------------------------------------------------------------')
                    logging.info ('coinmarketcaptest.CoinMarketCap')
                    cmc = coinmarketcaptest.CoinMarketCap(self.cwd, self.catalog)
                    cmc.main()
                    # # #thread1 = threading.Thread(target=md.main(), args=())
                    # #
                    print('--------------------------------------------------------------------------')
                    logging.info ('fetchprice.GetDtlPrice')
                    mfp = fetchprice.GetDtlPrice(self.symbol_list, self.exchanges, self.chunksize,self.cwd,self.catalog) #chunk size not used here just broken up into 50 strings due to api list constraint
                    mfp.main()
                    # # #thread2 = threading.Thread(target=mfp.main(), args=())
                    print('--------------------------------------------------------------------------')
                    #
                    logging.info ('tradepair.GetTradePair')
                    tp = tradepair.GetTradePair(self.symbol_list,self.chunksize,self.cwd,self.catalog)
                    tp.main()
                    # # #thread3 = threading.Thread(target=tp.main(), args=())
                    print('--------------------------------------------------------------------------')
                    #
                    logging.info ('minute_hist.GetMinuteHist')
                    mh = minute_hist.GetMinuteHist(self.symbol_list,self.exchanges,self.chunksize,self.cwd,self.catalog)
                    mh.main()
                    # thread4 = threading.Thread(target=mh.main(), args=())
                    #
                    logging.info ('hour_hist.GetHourHist')
                    hh = hour_hist.GetHourHist(self.symbol_list,self.exchanges,self.chunksize,self.cwd,self.catalog)
                    hh.main()
                    # thread5 = threading.Thread(target=hh.main(), args=())
                    #
                    logging.info ('day_hist.GetDayHist')
                    dh = day_hist.GetDayHist(self.symbol_list,self.exchanges,self.chunksize,self.cwd,self.catalog)
                    dh.main()
                    # thread6 = threading.Thread(target=dh.main(), args=())
                    print('--------------------------------------------------------------------------')
                    # #
                    logging.info ('mtsocial.GetSocialData')
                    gsd = mtsocial.GetSocialData(self.symbol_list,self.exchanges,self.chunksize,self.cwd,self.catalog,\
                    self.reddit_ls,\
                    self.coderepository_ls,\
                    self.twitter_ls,\
                    self.cryptocompare_ls,\
                    self.general_ls,\
                    self.facebook_ls)
                    gsd.main()


                    #
                    # tp = newtradepair.GetTradePair(self.symbol_list,self.chunksize,self.cwd,self.trade_pair,self.exchanges,self.exchange_trade_pair)
                    # tp.main()
                    #print self.exchange_trade_pair.keys
                    # nfp = newfetchprice.GetDtlPrice(self.symbol_list,self.chunksize,self.cwd,self.trade_pair,self.exchanges,self.exchange_trade_pair)
                    # nfp.main()
                    # print '---------'
                    #fl = forloopfetchprice.GetDtlPrice(self.symbol_list,self.exchanges,self.chunksize,self.cwd)
                    #fl.main()
                        # non 0:00:35.364493
                        #multi#thread  0:00:21.039896
                        #full run mutli #threadCompletion time: 0:16:40.115999


                except Exception as e:
                    logging.info('------')
                    logging.error(traceback.format_exc())
                    logging.info('------')
                    logging.exception(traceback.format_exc())
                    logging.info('------')
                    print(e)
                    print ('error on processing dtl, hist')
                #askcurrentprice from has price/ if focus_symbols passed

            except Exception as e:
                logging.info('------')
                logging.error(traceback.format_exc())
                logging.info('------')
                logging.exception(traceback.format_exc())
                logging.info('------')
                print(e)
                print ("ERROR: alpha_runner")

        else:
            print ('invalid args')

    def minute_run(self,l):

        logging.info ('Minute_run')
        logging.info ('fetchprice.GetDtlPrice')
        mfp = fetchprice.GetDtlPrice(self.focus_symbols, self.exchanges, self.chunksize,self.cwd,self.catalog) #chunk size not used here just broken up into 50 strings due to api list constraint
        mfp.main()
        logging.info ('minute_hist.GetMinuteHist')
        mh = minute_hist.GetMinuteHist(self.focus_symbols,self.exchanges,self.chunksize,self.cwd,self.catalog)
        mh.main()




    def main(self):
        try:

            logging.info ('')
            logging.info ('')
            logging.info ('')
            logging.info ('')
            self.logging.info(socket.gethostname())
            start_time = dt.datetime.now()
            #logging.basicConfig(filename='myapp.log', level=logging.INFO)
            self.logging.info('Started')

            print('')
            print ('-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')
            print('')
            print ('-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')

            print (self.args)
            s = setup.Setup(self.cwd)
            self.cwd = s.main()
            print(self.cwd)

            cat = awscatalogcreate.CreateAwsCatalog(self.cwd)
            self.catalog = cat.main()

            s3 = s3maintenance.GetS3Bucket(self.catalog)
            s3.main()


            print('chunksize: '+str(self.chunksize))

            print ('---------------------------------------------------------------------------------------BEGIN---------------------------------------------------------------------------------------')


            if self.minute_run_param == 'N':
                self.alpha_runner(self.logging)
            elif self.minute_run_param == 'Y':
                self.minute_run(self.logging)


            if self.runcrawler == 'Y':
                print ('---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')
                glue = gluemaintenance.RunGlue(self.catalog)
                glue.main()

            print ('---------------------------------------------------------------------------------------END---------------------------------------------------------------------------------------')
            x = dt.datetime.now() - start_time
            print ('Completion time: '+str(x))
            lts = logtos3.LogToS3(self.catalog,self.logfile, self.s3_log_file)
            lts.main()
            print ('---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')
            self.logging.info('Finished')
            self.logging.info('Completion time: '+str(x))
            print('')
            self.logging.info(socket.gethostname())

        except Exception as e:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            print(e)


if __name__ == '__main__':

    ar = AlphaRunner()
    ar.main()
