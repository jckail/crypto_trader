import os

cwd = os.getcwd()
from time import sleep
from tqdm import tqdm
import socket
import boto3
import awscatalogcreate
import traceback
import logging


class Setup(object):

    def __init__(self, cwd):
        self.cwd = cwd

    def build_data_path(self):

        cwd_split = self.cwd.split('/')

        target_ibdex = cwd_split.index('alpha')  # project name
        self.cwd = '/'.join(cwd_split[:target_ibdex - 1]) + '/lit_crypto_data/alpha'

        if not os.path.exists(self.cwd):
            os.makedirs(self.cwd)
            print('Created: ' + self.cwd)
        return self.cwd

    def validate_directories(self):
        try:
            create_list = ['/data/' \
                , '/data/coinmarketcap/' \
                , '/data/day_data/' \
                , '/data/hour_data/' \
                , '/data/mining_data/' \
                , '/data/minute_data/' \
                , '/data/social/' \
                , '/data/social/reddit/' \
                , '/data/social/twitter/' \
                , '/data/social/coderepository/' \
                , '/data/social/facebook/' \
                , '/data/social/cryptocompare/' \
                , '/data/social/general/' \
                , '/data/trading_pair/' \
                , '/data/coininfo/' \
                , '/data/pricedetails/' \
                , '/data/mining_data/coin_miner_data/' \
                , '/data/mining_data/miner_data/'
                , '/data/avinfo/'
                , '/data/currency_exchange_rates/'
                           ]

            # for y in tqdm(create_list,desc='validate_directories'): #progressbar
            for y in create_list:
                directory = self.cwd + y
                gdirectory = self.cwd + y + "gzip_files/"
                if not os.path.exists(directory):
                    os.makedirs(directory)
                    print('Created: ' + directory)
                if not os.path.exists(gdirectory):
                    os.makedirs(gdirectory)
                    print('Created: ' + gdirectory)
        except Exception as e:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            print(e)

    def main(self):
        try:
            iam = boto3.resource('iam')
            current_user = iam.CurrentUser()
            print('Current User: ' + current_user.user_name)
            s = Setup(self.cwd)
            self.cwd = s.build_data_path()
            s.validate_directories()
            # print(self.cwd)
            print ('---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')
            return self.cwd

        except Exception as e:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            print(e)


if __name__ == '__main__':
    # cwd = os.getcwd()

    s = Setup()
    s.main()
