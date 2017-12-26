#!/usr/bin/env python
import argparse
import buildcoinlist
import haspricing
import pandas



class alpha_runner(object):
    """
    This is the main runner
    """

    def __init__(self):
        self.args = self.get_args()
        self.run = self.args.run
        self.focus_symbols =  ['BTC','BCH','LTC','ETH']


        if self.run == 'Y':
            print'evoked'
        else:
            print 'invalid args'

        #self.org_params = json.load(open("config/cti_config.dict"))

    def get_args(self):
        """
        :return:
        """
        parser = argparse.ArgumentParser(usage='alpha_runner.py --run <run>')
        parser.add_argument('--run', required=True, dest='run', choices=['Y', 'N'], help='want to run this y')
        args = parser.parse_args()
        return args

    def validate_coin_get_price(self):

        if 1 == 1:
            try:
                #get list of coins
                coin_df = buildcoinlist.GetCoinLists()
                gcl_output = coin_df.main()

                #ask if price info exists (update has price csv)
                hpc = haspricing.HasPricingCheck(gcl_output,self.focus_symbols)
                hpc.main()


            except:
                print "ERROR: validate_coin_get_price"

    def main(self):
        self.validate_coin_get_price()


if __name__ == '__main__':
    ar = alpha_runner()
    ar.main()
