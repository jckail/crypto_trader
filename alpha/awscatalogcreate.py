import traceback
import logging


class CreateAwsCatalog:

    def __init__(self,cwd):
        self.cwd = cwd
        self.catalog = ''

    def create_catalog(self):

        cwd_split = self.cwd.split('/')
        target_ibdex = cwd_split.index('alpha') # project name

        self.catalog = cwd_split[target_ibdex-1]
        self.catalog = self.catalog.replace('_','')
        self.catalog = self.catalog.lower()
        print (self.catalog)
        return self.catalog

    def main(self):
        try:
            cat = CreateAwsCatalog(self.cwd)
            self.catalog = cat.create_catalog()
            print('Working AWS Catalog:'+self.catalog)
            return self.catalog
        except Exception as e:
            print(e)
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')


if __name__ == '__main__':
    #cwd = '/Users/jkail/Documents/GitHub/lit_crypto_data/alpha'
    cat = CreateAwsCatalog()
    catalog = cat.main()