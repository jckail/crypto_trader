#!/usr/bin/env python
#mods
import argparse
import os
import pandas as p
from multiprocessing import Pool, TimeoutError
import time
# classes
import buildcoinlist
import day_hist
import test
import threading
#add arg focus symbols only
import datetime as dt


import fetchprice
import haspricing
import hour_hist
import minute_hist
import social
import miningdata
import tradepair
import fetchprice





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
        self.exchanges = ['Bitfinex','Bitstamp','coinone','Coinbase','CCCAGG']
        #self.exchanges = ['Coinbase']
        self.chunksize = 199  #~~#thread limit
        #self.org_params = json.load(open("config/cti_config.dict"))

    def get_args(self):
        """
        :return:
        """
        parser = argparse.ArgumentParser(usage='alpha_runner.py --run <run> --runfocus_symbols_only <runfocus_symbols_only> --runisprice <runisprice>')
        parser.add_argument('--run', required=True, dest='run', choices=['Y', 'N'], help='want to run this y')
        parser.add_argument('--runfocus_symbols_only', required=True, dest='runfocus_symbols_only', choices=['Y', 'N'], help='runfocus_symbols_only run')
        parser.add_argument('--runisprice', required= False, default= 'N',dest='runisprice', choices=['Y', 'N'], help='runfocus_symbols_only run')
        args = parser.parse_args()
        print '------'+str(args)
        return args

    def validate_coin_get_price(self):
        print 'Chunk size: '+str(self.chunksize)
        if self.run == 'Y':
            print "Begin validate_coin_get_price"
            try:
                try:
                    if self.runfocus_symbols_only == 'N':
                        #get list of coins
                        coin_df = buildcoinlist.GetCoinLists(self.runfocus_symbols_only,self.focus_symbols)
                        gcl_output = coin_df.main()
                        symbol_list = gcl_output["Symbol"].tolist()
                        #symbol_list = symbol_list # for testing
                        if self.runisprice == 'Y':
                            has_pricing = []
                            hpc = haspricing.HasPricingCheck(symbol_list,has_pricing,self.chunksize)
                            hpc.main()
                            df = p.read_csv(self.cwd+'/data/has_pricing.csv')
                            df_has = df.query('has_pricing == 1')
                            ls_has = df_has["symbol"].tolist()

                        elif self.runisprice == 'N' and os.path.isfile(self.cwd+'/data/has_pricing.csv') == True:
                            df = p.read_csv(self.cwd+'/data/coinlist_info.csv')
                            #df_has = df.query('has_pricing == 1')
                            ls_has = df["Symbol"].tolist()

                        else:
                            has_pricing = []
                            hpc = haspricing.HasPricingCheck(symbol_list,has_pricing,self.chunksize)
                            hpc.main()
                            df = p.read_csv(self.cwd+'/data/has_pricing.csv')
                            df_has = df.query('has_pricing == 1')
                            ls_has = df_has["symbol"].tolist()

                    elif self.runfocus_symbols_only == 'Y':
                        ls_has = self.focus_symbols
                except Exception as e:
                    print(e)
                    print 'error getting symbol_list'

                try:
                    x = len(ls_has)
                    ls_has = ls_has
                    print '--------------------------------------------------------------------------'
                    print 'Evaluating: '+str(x)
                    print '--------------------------------------------------------------------------'
                    #helps limit #threads open etc

                    md = miningdata.GetMineData()
                    md.main()
                    #thread1 = #threading.Thread(target=md.main(), args=())

                    print'--------------------------------------------------------------------------'
                    mfp = fetchprice.GetDtlPrice(ls_has, self.exchanges, self.chunksize) #chunk size not used here just broken up into 50 strings due to api list constraint
                    mfp.main()
                    #thread2 = #threading.Thread(target=mfp.main(), args=())
                    print'--------------------------------------------------------------------------'

                    tp = tradepair.GetTradePair(ls_has)
                    #tp.main()
                    ##thread3 = #threading.Thread(target=tp.main(), args=())
                    print'--------------------------------------------------------------------------'

                    mh = minute_hist.GetMinuteHist(ls_has,self.exchanges,self.chunksize)
                    mh.main()
                    #thread4 = #threading.Thread(target=mh.main(), args=())

                    hh = hour_hist.GetHourHist(ls_has,self.exchanges,self.chunksize)
                    hh.main()
                    #thread5 = #threading.Thread(target=hh.main(), args=())

                    dh = day_hist.GetDayHist(ls_has,self.exchanges,self.chunksize)
                    dh.main()
                    #thread6 = #threading.Thread(target=dh.main(), args=())
                    print'--------------------------------------------------------------------------'

                    gsd = social.GetSocialData(ls_has)
                    #gsd.main()
                    ##thread7 = #threading.Thread(target=mfp.main(), args=())


                    #thread1.start()
                    #thread2.start()
                    ##thread3.start()
                    #thread4.start()
                    #thread5.start()
                    #thread6.start()

                    #thread1.join()
                    #thread2.join()
                    ##thread3.join()
                    #thread4.join()
                    #thread5.join()
                    #thread6.join()
                    ##thread7.start()

                        # non 0:00:35.364493
                        #multi#thread  0:00:21.039896
                        #full run mutli #thread

                except Exception as e:
                    print(e)
                    print 'error on processing dtl, hist'
                #askcurrentprice from has price/ if focus_symbols passed

            except Exception as e:
                print(e)
                print "ERROR: validate_coin_get_price"

        else:
            print 'invalid args'

    def main(self):
        start_time = dt.datetime.now()

        print '----------------------------BEGIN----------------------------'
        self.validate_coin_get_price()
        print '----------------------------END----------------------------'
        x =  dt.datetime.now() - start_time
        print 'Completion time: '+str(x)




if __name__ == '__main__':
    ar = AlphaRunner()

    ar.main()






