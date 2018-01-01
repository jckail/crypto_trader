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



class SaveS3(object):

    def __init__(self, file):

        self.file = file
        self.s3_client = boto3.client('s3')
        self.directory = file.replace(basename(file),'')
        self.s3 = boto3.resource('s3')

        cwd_split = self.directory.split('/')
        target_ibdex = cwd_split.index('alpha') # project name

        self.s3_directory = '/'.join(cwd_split[target_ibdex:])
        self.basename = basename(file)
        self.filename, self.file_extension = os.path.splitext(self.basename)

    def to_json(self):
        df = p.read_csv(self.file)
        df.to_json(self.directory+self.filename+'.json')
        self.basename = self.filename+'.json'

    def gzip_jsons(self):
        zipped_file = self.directory +"gzip_files/"+self.basename+'.gz'
        with open(self.file, 'rb') as f_in, gzip.open(zipped_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        self.file = zipped_file
        #print (self.file)


    def save_to_s3(self):
        s3 = boto3.resource('s3')
        self.basename = basename(self.file)
        #print(self.file,'litcrypto',self.s3_directory+self.basename)
        s3.meta.client.upload_file(self.file,'litcrypto',self.s3_directory+self.basename )

        #multipart_upload_part = self.s3.MultipartUploadPart('litcrypto',self.s3_directory+self.basename,'multipart_upload_id','part_number')
        #s3.upload_fileobj(x,'litcrypto','data/coinlist_info')

    def main(self):
        s3 = SaveS3(self.file)
        #create a csv to hive datastore command
        #run athena query to validate datastore was created successfully
        s3.to_json()
        s3.gzip_jsons()
        s3.save_to_s3()



if __name__ == '__main__':
    """

    :return:
    """
    file ='/Users/jckail13/lit_crypto/alpha/data/coinlist_info.csv'
    cwd = '/Users/jckail13/lit_crypto/alpha/data/'


    runner = SaveS3()
    runner.main()



    # 42 seconds 100 records