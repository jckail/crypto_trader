import boto3
client = boto3.client('glue')

class CheckDatabase:

    def __init__(self):
        self.catalogid = '462455771080'

response = client.get_databases(
    CatalogId='462455771080',

    MaxResults=123
)
print(response)

response = client.create_database(
    CatalogId='string',
    DatabaseInput={
        'Name': 'string',
        'Description': 'string',
        'LocationUri': 'string',
        'Parameters': {
            'string': 'string'
        }
    }
)

if __name__ == '__main__':
    my_file = '/Users/jkail/Documents/GitHub/lit_crypto_data/alpha/data/social/reddit/reddit.json'#.gz
    cwd = my_file
    #rg = RunGlue(my_file)
    rg = RunGlue()
    rg.main()

print(response)

#462455771080