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




class SaveS3(object):

    def __init__(self, file,catalog):
        self.catalog = catalog
        self.file = file
        self.s3_client = boto3.client('s3')
        self.directory = file.replace(basename(file),'')
        self.s3_resource = boto3.resource('s3')


        self.basename = basename(file)
        self.filename, self.file_extension = os.path.splitext(self.basename)



        self.local_gz_name = self.directory+"gzip_files/"+self.filename+'.json'+'.gz'
        self.aws_gz_file = self.directory+"gzip_files/"+'aws_'+self.filename+'.json'+'.gz'

        self.s3_basename = basename(self.local_gz_name)
        cwd_split = self.directory.split('/')
        target_ibdex = cwd_split.index('alpha') # project name

        self.s3_directory = '/'.join(cwd_split[target_ibdex:])
        self.s3_file = self.s3_directory+self.s3_basename


    def to_json(self):
        frames = []
        #df.to_json(self.local_gz_name,compression = 'gzip')
        if os.path.isfile(self.file):
            try:
                df = p.read_csv(self.file)
                df = df.reset_index(drop=True)
                if not df.empty:
                    frames.append(df)
                else:
                    pass
            except Exception as e:
                print(e)

        if os.path.isfile(self.aws_gz_file):
            try:
                df = p.read_json(self.aws_gz_file,compression = 'gzip')
                df = df.reset_index(drop=True)
                if not df.empty:
                    frames.append(df)
                else:
                    pass
            except Exception as e:
                print(e)

        if len(frames) > 0:
            df = p.concat(frames)
            df = df.reset_index(drop=True)
            #print (df)
            df.to_json(self.local_gz_name,orient = 'records',compression = 'gzip',lines = True)


    def save_to_s3(self):
        try:

            self.s3_resource.meta.client.upload_file(self.local_gz_name,self.catalog,self.s3_file )

        except Exception as e:
            print(e)
            pass

    def get_s3_file(self):
        try:
            self.s3_resource.Bucket(self.catalog).download_file(self.s3_file, self.aws_gz_file)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                pass
            else:
                raise

    def main(self):
        try:
            s3 = SaveS3(self.file,self.catalog)
            s3.get_s3_file()
            s3.to_json()
            s3.save_to_s3()

        except Exception as e:
            print(e)



if __name__ == '__main__':
    """

    :return:
    """
    '/Users/jkail/Documents/GitHub/lit_crypto_data/alpha'
    file = '/Users/jkail/Documents/GitHub/lit_crypto_data/alpha/data/coininfo/coininfo.csv'
    catalog = 'litcryptodata'

    #runner = SaveS3(file,catalog)
    runner = SaveS3()
    runner.main()



    # 42 seconds 100 records