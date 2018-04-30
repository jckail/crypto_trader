import boto3
import traceback
import logging

class GetS3Bucket:
    def __init__(self,bucket):
        self.tests3 = boto3.resource('s3')
        self.s3_client = boto3.client('s3')
        self.s3_bucket = bucket
        self.loops = 0

    def validate_s3(self):
        sa3 = boto3.resource('s3')
        #s3 = GetS3Bucket(self.s3_bucket)
        import botocore
        exists = True



        try:
            a = sa3.meta.client.head_bucket(Bucket=(self.s3_bucket))
            print('Validated: '+self.s3_bucket)
            #print(a)
        except botocore.exceptions.ClientError as e:

            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            #print(error_code)

            #print(self.loops)
            print(e)
            if error_code == 404:
                exists = False

                print('Creating Bucket: '+self.s3_bucket)
                sa3.create_bucket(Bucket=self.s3_bucket)
                logging.info('------')
                logging.error(traceback.format_exc())
                logging.info('------')
                logging.exception(traceback.format_exc())
                logging.info('------')

                if self.loops <= 2:
                    self.loops += 1
                    #print(self.loops)
                    s3.validate_s3()




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




if __name__ == '__main__':
    #cwd = '/Users/jckail13/lit_crypto_data/alpha/data/coininfo/coininfo.csv'
    #b = 'litcryptodata'
    #print(b)
    s3 = GetS3Bucket('litcryptodata')
    #rg = RunGlue()
    s3.main()