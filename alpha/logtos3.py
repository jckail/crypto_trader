#!/usr/bin/env python

__author__ = 'jkail'

import requests
import pandas as p
import datetime as dt
import os
import threading
from tqdm import tqdm
from time import sleep
import boto3
from os.path import basename
import json
import csv
import gzip
import shutil
import socket
import botocore
import traceback
import logging




class LogToS3(object):

    def __init__(self, catalog, local_log_file, s3_log_file):

        self.catalog = catalog
        self.local_log_file = local_log_file
        self.s3_log_file = s3_log_file
        self.s3_resource = boto3.resource('s3')

    def log_to_s3(self):
        try:
            self.s3_resource.meta.client.upload_file(self.local_log_file, self.catalog, self.s3_log_file )

        except Exception as e:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            print(e)
            pass

    def main(self):
        #print ('BEGIN: LogToS3')
        try:
            s3 = LogToS3(self.catalog, self.local_log_file, self.s3_log_file)
            s3.log_to_s3()
            print('Log Saved to S3')

        except Exception as e:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            print(e)



if __name__ == '__main__':

    # catalog = 'litcryptodata'
    # s3_log_file = 'alpha/logs/litcrypto.log'
    # local_log_file = '/Users/jkail/Documents/GitHub/lit_crypto_data/alpha/logs/litcrypto.log'
    #runner = LogToS3(catalog, local_log_file, s3_log_file)
    runner = LogToS3()
    runner.main()
#
# /Users/jkail/Documents/GitHub/lit_crypto_data/alpha/data/coininfo/gzip_files/coininfo.json.gz
# litcryptodata
# alpha/data/coininfo/coininfo.json.gz

    # 42 seconds 100 records