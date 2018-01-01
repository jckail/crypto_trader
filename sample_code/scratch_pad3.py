# import boto3
#
# s3 = boto3.resource('s3')
#
# for bucket in s3.buckets.all():
#     print (bucket.name)
my_file = '/Users/jckail13/lit_crypto/alpha/data/coinlist_info.csv'
import boto3
import pandas as p

session = boto3.Session()
s3_client = session.client('s3')
try:
    print("uploading file"),my_file

    tc = boto3.s3.transfer.TransferConfig()
    t = boto3.s3.transfer.S3Transfer(client = s3_client,
                                     config = tc)
    t.upload_file(my_file,'litcrypto','data/coinlist_info.csv')

except:
    print ('ef')

