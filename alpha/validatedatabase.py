import boto3
import datetime as dt


class CheckDatabase:

    def __init__(self, catalog):

        self.catalogid = '462455771080'
        self.glue_client = boto3.client('glue')
        self.catalog = catalog
        self.database_name = catalog


    def validate_database(self):
        try:
            cd = CheckDatabase(self.catalog)
            response = self.glue_client.get_databases(
                CatalogId=self.catalogid,
                MaxResults=123
            )
            #'spectrumdb'
            #print(response['DatabaseList'])
            #test =  response['DatabaseList']
            #print(type(test))
            #print(list(response['DatabaseList']))

            db_list = []

            for x in response['DatabaseList']:
                db_list.append(x['Name'])

            db_list = set(db_list)
            db_list = list(db_list)

            if self.database_name in (db_list):
                print('Validated Database: '+self.database_name)
            else:
                print('Creating Database: '+self.database_name)
                cd.create_database()
                cd.validate_database()
        except Exception as e:
            print(e)

    def create_database(self):
        try:
            response = self.glue_client.create_database(
                CatalogId=self.catalogid,
                DatabaseInput={
                    'Name': self.database_name,
                    'Description': 'created by runner on '+str(dt.datetime.now()),
                    'Parameters': {}
                }
            )
        except Exception as e:
            print(e)

    def main(self):
        try:
            cd = CheckDatabase(self.catalog)
            cd.validate_database()
            return self.database_name

        except Exception as e:
            print(e)
if __name__ == '__main__':
    # my_file = '/Users/jkail/Documents/GitHub/lit_crypto_data/alpha/data/social/reddit/reddit.json'#.gz
    # cwd = my_file
    cd = CheckDatabase()
    output = cd.main()
    #print(output)
    #cd.validate_database()



#462455771080