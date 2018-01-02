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
import validatedatabase

class RunGlue:
    def __init__(self, catalog):
        self.client = boto3.client('glue')
        self.catalog = catalog

        self.s3_path = 's3://%s/' % self.catalog
        self.cron = 'cron(15 12 * * ? *)'


        self.database = self.catalog

        self.crawler_name = self.database+'crawler'

    def check_running(self):
        rg = RunGlue(self.catalog)
        #'RUNNING'
        try:
            response = self.client .get_crawlers()
            print(response)
            crawlers = response['Crawlers']
            for crawler in crawlers:
                if crawler['Name'] == self.crawler_name:
                    if print(crawler['State']) == 'READY':
                        rg.
                    else:
                        sleep(5)
                        rg.check_running()

                else:
                    pass




        except Exception as e:
            pass
            print(e)


    def main(self):
        try:


            rg1 = RunGlue(self.catalog)
            rg1.check_running()

        except Exception as e:
            pass
            print(e)

if __name__ == '__main__':

    x = 'litcryptodata'
    rg = RunGlue(x)
    rg.main()