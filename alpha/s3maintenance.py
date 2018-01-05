import boto3
import traceback
import logging

class GetS3Bucket:
    def __init__(self,bucket):

        self.s3_client = boto3.client('s3')
        self.s3_bucket = bucket
    def create_s3_bucket(self):
        try:
            self.s3_client.create_bucket(Bucket=self.s3_bucket)

        except Exception as e:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            print(e)

    def validate_s3(self):

        s3 = GetS3Bucket(self.s3_bucket)
        try:
            response = self.s3_client.list_buckets()
            buckets = [bucket['Name'] for bucket in response['Buckets']]
            #print(buckets)
            if self.s3_bucket in buckets:
                print('Validated Bucket: '+self.s3_bucket)
            else:
                s3.create_s3_bucket()
                print('Creating Bucket: '+self.s3_bucket)
                s3.validate_s3()

        except Exception as e:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            print(e)

    def main(self):
        try:

            s3 = GetS3Bucket(self.s3_bucket)
            s3.validate_s3()
            return self.s3_bucket

        except Exception as e:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            pass
                #print(e)




if __name__ == '__main__':
    #cwd = '/Users/jckail13/lit_crypto_data/alpha/data/coininfo/coininfo.csv'
    s3 = GetS3Bucket()
    #rg = RunGlue()
    s3.main()