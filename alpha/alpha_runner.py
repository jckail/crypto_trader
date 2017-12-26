#!/usr/bin/env python
import argparse
import buildcoinlist
import pandas

class alpha_runner(object):
    """
    This is the main runner
    """

    def __init__(self):
        self.args = self.get_args()
        self.run = self.args.run

        if self.run == 'Y':
            print'hey you envoked this'
        else:
            print 'foff asdfasdf'

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

                print 'try trigger'
                coin_df = buildcoinlist.GetCoinLists()
                output = coin_df.main()
                print output

            except:
                print "not going into envoiking function "

    def main(self):
        self.validate_coin_get_price()


if __name__ == '__main__':
    ar = alpha_runner()
    ar.main()
