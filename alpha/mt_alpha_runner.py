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
import fetchprice
import haspricing
import hour_hist
import minute_hist
import social
import miningdata
import tradepair
import test
import threading
#add arg focus symbols only
import datetime as dt


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
        self.chunksize = 100

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
                        #ask if price info exists (update has price csv)
                        ### add [:50] ### to symbol_list for testing
                            xsymbols = [symbol_list[x:x+self.chunksize] for x in xrange(0, len(symbol_list), self.chunksize)]
                            for symbol_list in xsymbols:
                                    has_pricing = []
                                    hpc = haspricing.HasPricingCheck(symbol_list,has_pricing)
                                    hpc.main()


                            df = p.DataFrame.from_csv(self.cwd+'/data/has_pricing.csv')
                            df_has = df.query('has_pricing == 1')
                            ls_has = df_has["symbol"].tolist()

                        elif self.runisprice == 'N' and os.path.isfile(self.cwd+'/data/has_pricing.csv') == True:

                            df = p.DataFrame.from_csv(self.cwd+'/data/has_pricing.csv')
                            df_has = df.query('has_pricing == 1')
                            ls_has = df_has["symbol"].tolist()

                    elif self.runfocus_symbols_only == 'Y':
                        ls_has = self.focus_symbols
                except ValueError:
                    print 'error getting symbol_list'

                try:
                    x = len(ls_has)
                    print 'Evaluating: '+str(x)

                    #helps limit threads open etc
                    xsymbols = [ls_has[x:x+self.chunksize] for x in xrange(0, len(ls_has), self.chunksize)]
                    for ls_has in xsymbols:
                        md = miningdata.GetMineData()
                        #md.main()


                        tp = tradepair.GetTradePair(ls_has)
                        #tp.main()


                        fp = fetchprice.GetDtlPrice(ls_has)
                        #fp.main()

                        mh = minute_hist.GetMinuteHist(ls_has,self.exchanges)
                        mh.main()

                        hh = hour_hist.GetHourHist(ls_has,self.exchanges)
                        hh.main()

                        dh = day_hist.GetDayHist(ls_has,self.exchanges)
                        dh.main()

                        gsd = social.GetSocialData(ls_has)
                        #gsd.main()



                        # non 0:00:35.364493
                        #multithread  0:00:21.039896
                        #full run mutli thread


                except ValueError:
                    print 'error on processing dtl, hist'


                #askcurrentprice from has price/ if focus_symbols passed

            except ValueError:
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



    """print __name__
    pool = Pool(processes=4)              # start 4 worker processes


    # evaluate "f(20)" asynchronously
    res = pool.apply_async(t.f)      # runs in *only* one process
    print res.get(timeout=1)              # prints "400"
    
    """



