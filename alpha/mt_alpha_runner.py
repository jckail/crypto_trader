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
import postrunmtc


class AlphaRunner(object):

    """
    This is the main runner
    """

    def __init__(self):
        self.args = self.get_args()
        self.run = self.args.run
        self.runfocus_symbols_only = self.args.runfocus_symbols_only
        self.runisprice = self.args.runisprice
        self.cwd = os.getcwd()
        self.focus_symbols = ['BTC','BCH','LTC','ETH']
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

    def get_args(self):
        """
        :return:
        """
        parser = argparse.ArgumentParser(usage='alpha_runner.py --run <run> --runfocus_symbols_only <runfocus_symbols_only> --runisprice <runisprice>')
        parser.add_argument('--run', required=True, dest='run', choices=['Y', 'N'], help='want to run this y')
        parser.add_argument('--runfocus_symbols_only', required=True, dest='runfocus_symbols_only', choices=['Y', 'N'], help='runfocus_symbols_only run')
        parser.add_argument('--runisprice', required= False, default= 'N',dest='runisprice', choices=['Y', 'N'], help='runfocus_symbols_only run')
        args = parser.parse_args()

        return args

    def alpha_runner(self):
        print ('Chunk size: '+str(self.chunksize))
        if self.run == 'Y':
            print ("Begin alpha_runner")
            try:
                try:
                    if self.runfocus_symbols_only == 'N':
                        cl = coinlist.GetCoinLists(self.cwd)
                        cl.main()
                        df = p.read_csv(self.cwd+'/data/coininfo/coininfo.csv', encoding= 'utf-8')
                        self.symbol_list = df["Symbol"].tolist()

                    elif self.runfocus_symbols_only == 'Y':
                         self.symbol_list = self.focus_symbols
                except Exception as e:
                    print(e)
                    print ('error getting self.symbol_list')

                try:

                    #add processing queue

                    self.symbol_list = self.symbol_list[:10]
                    #
                    x = len(self.symbol_list)
                    # #self.symbol_list.append('SMT')
                    print ('--------------------------------------------------------------------------')
                    print ('Evaluating: '+str(x)+' Coins')
                    print ('--------------------------------------------------------------------------')
                    # #helps limit #threads open etc
                    #
                    md = miningdata.GetMineData(self.cwd)
                    md.main()
                    # # #thread1 = threading.Thread(target=md.main(), args=())
                    # #
                    print('--------------------------------------------------------------------------')
                    mfp = fetchprice.GetDtlPrice(self.symbol_list, self.exchanges, self.chunksize,self.cwd) #chunk size not used here just broken up into 50 strings due to api list constraint
                    mfp.main()
                    # # #thread2 = threading.Thread(target=mfp.main(), args=())
                    print('--------------------------------------------------------------------------')
                    #
                    tp = tradepair.GetTradePair(self.symbol_list,self.chunksize,self.cwd)
                    tp.main()
                    # # #thread3 = threading.Thread(target=tp.main(), args=())
                    print('--------------------------------------------------------------------------')
                    #
                    mh = minute_hist.GetMinuteHist(self.symbol_list,self.exchanges,self.chunksize,self.cwd)
                    mh.main()
                    # thread4 = threading.Thread(target=mh.main(), args=())
                    #
                    hh = hour_hist.GetHourHist(self.symbol_list,self.exchanges,self.chunksize,self.cwd)
                    hh.main()
                    # thread5 = threading.Thread(target=hh.main(), args=())
                    #
                    dh = day_hist.GetDayHist(self.symbol_list,self.exchanges,self.chunksize,self.cwd)
                    dh.main()
                    # thread6 = threading.Thread(target=dh.main(), args=())
                    print('--------------------------------------------------------------------------')
                    # #
                    gsd = mtsocial.GetSocialData(self.symbol_list,self.exchanges,self.chunksize,self.cwd,\
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
                    print(e)
                    print ('error on processing dtl, hist')
                #askcurrentprice from has price/ if focus_symbols passed

            except Exception as e:
                print(e)
                print ("ERROR: alpha_runner")

        else:
            print ('invalid args')

    def main(self):
        start_time = dt.datetime.now()
        print ('-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')
        print (self.args)
        print (self.cwd)
        print ('---------------------------------------------------------------------------------------BEGIN---------------------------------------------------------------------------------------')
        s = setup.Setup(self.cwd)
        self.cwd = s.main()
        self.alpha_runner()
        rg = postrunmtc.RunGlue()
        rg.main()
        print ('---------------------------------------------------------------------------------------END---------------------------------------------------------------------------------------')
        x = dt.datetime.now() - start_time
        print ('Completion time: '+str(x))
        print ('---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')




if __name__ == '__main__':
    ar = AlphaRunner()
    ar.main()
