import requests
import pandas as p
import os
import datetime as dt
cwd = os.getcwd()
source = 'cryptocompare'


url = "https://www.cryptocompare.com/api/data/miningequipment/"

headers = {
    'cache-control': "no-cache",
    'postman-token': "f2ac0486-84d0-6e4c-a3e7-ba91f5f35897"
}

response = requests.request("GET", url, headers=headers)

data = response.json()

def miner_data(data):
    keys = data['MiningData'].keys()
    frames = []

    for key in keys:
        print '----------'

        print key
        sub =  data['MiningData'][key]
        print '----------'

        df = p.DataFrame.from_dict(sub,orient='Index', dtype=None)
        df = p.DataFrame.transpose(df)
        df = df.assign (timestamp_api_call = dt.datetime.now(),source = source,key = key )
        frames.append(df)

    my_file = cwd+'/data/mining_equipment.csv'

    if os.path.isfile(my_file):
        df_resident = p.DataFrame.from_csv(my_file)
        print 'appending new data: '
        frames.append(df_resident)
    else:
        print ''

    df = p.concat(frames)
    df = df.drop_duplicates(['Company','Cost','CurrenciesAvailable','HashesPerSecond','Name'],  keep='last')
    df = df.sort_values('key')
    df = df.reset_index(drop=True)
    print df
    if not df.empty:
        df.to_csv(my_file,index_label='Sequence',  encoding= 'utf-8' ) #need to add this
        print 'Updated: '+str(my_file)
    else:
        print 'No data: '

miner_data(data)