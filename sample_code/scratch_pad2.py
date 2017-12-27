import requests
import pandas as p
import datetime as dt
import os

#symbol =  "'LTC'"
symbols = ['BTC','BCH','LTC','ETH']
source = 'cryptocompare'
cwd = os.getcwd()

for symbol in symbols:
    frames = []
    raw_symbol = symbol
    symbol = "'"+symbol+"'" #must add for df query
    df_get_id = p.DataFrame.from_csv('/Users/jkail/Documents/GitHub/lit_crypto/alpha/data/coinlist_info.csv')
    a = df_get_id.query("Symbol == "+symbol)
    b = a["Id"].tolist()
    symbol_id = b[0]


    url = "https://www.cryptocompare.com/api/data/socialstats/"

    querystring = {"id":symbol_id}

    headers = {
        'cache-control': "no-cache",
        'postman-token': "557b5e23-a352-f2d3-938d-fe17d490eee9"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)
    data = response.json()
    sub_data = data["Data"]
    keys = sub_data.keys()

    print 'data_keys: ' + str(keys)
    for key in keys:
        frames =[]
        ############################################################################################################################################################
        if key == 'CodeRepository':
            code_repository = sub_data[key]
            keys = code_repository.keys()
            sub = code_repository['List']
            df = p.DataFrame(sub)
            df = df.assign (socialsource = key,timestamp_api_call = dt.datetime.now(),source = source,symbol = raw_symbol,symbol_id = symbol_id )
            frames.append(df)

            my_file = cwd+'/data/social/%s.csv' % key

            if os.path.isfile(my_file):
                df_resident = p.DataFrame.from_csv(my_file)
                print 'appending new'+str(key)+' data: '+symbol
                frames.append(df_resident)
            else:
                print ''

            df = p.concat(frames)
            df = df.drop_duplicates(['closed_issues','closed_pull_issues','language','last_update','subscribers','stars','symbol'], keep='last')
            df = df.sort_values('symbol')
            df = df.reset_index(drop=True)

            if not df.empty:
                df.to_csv(my_file, index_label='Id')
                print 'Updated: '+str(my_file)
            else:
                print 'No '+str(key)+' data: '+symbol
                ############################################################################################################################################################
        elif key == 'Reddit':
            sub = sub_data[key]
            df = p.DataFrame.from_dict(sub,orient='index', dtype=None)
            df = p.DataFrame.transpose(df)
            df = df.assign (socialsource = key,timestamp_api_call = dt.datetime.now(),source = source,symbol = raw_symbol,symbol_id = symbol_id )
            frames.append(df)

            my_file = cwd+'/data/social/%s.csv' % key

            if os.path.isfile(my_file):
                df_resident = p.DataFrame.from_csv(my_file)
                print 'appending new'+str(key)+' data: '+symbol
                frames.append(df_resident)
            else:
                print ''

            df = p.concat(frames)
            df = df.drop_duplicates(['name','comments_per_hour','posts_per_day','Points','subscribers','symbol'], keep='last')
            df = df.sort_values('symbol')
            df = df.reset_index(drop=True)

            if not df.empty:
                df.to_csv(my_file, index_label='Id')
                print 'Updated: '+str(my_file)
            else:
                print 'No '+str(key)+' data: '+symbol

                ############################################################################################################################################################
        elif key == 'Twitter':
            sub = sub_data[key]
            df = p.DataFrame.from_dict(sub,orient='index', dtype=None)
            df = p.DataFrame.transpose(df)
            df = df.assign (socialsource = key,timestamp_api_call = dt.datetime.now(),source = source,symbol = raw_symbol,symbol_id = symbol_id )
            frames.append(df)

            my_file = cwd+'/data/social/%s.csv' % key

            if os.path.isfile(my_file):
                df_resident = p.DataFrame.from_csv(my_file)
                print 'appending new'+str(key)+' data: '+symbol
                frames.append(df_resident)
            else:
                print ''

            df = p.concat(frames)
            df = df.drop_duplicates(['account_creation','name','Points','followers','statuses','symbol'],  keep='last')
            df = df.sort_values('symbol')
            df = df.reset_index(drop=True)

            if not df.empty:
                df.to_csv(my_file, index_label='Id')
                print 'Updated: '+str(my_file)
            else:
                print 'No '+str(key)+' data: '+symbol

                ############################################################################################################################################################
        elif key == 'Facebook':
            sub = sub_data[key]
            df = p.DataFrame.from_dict(sub,orient='index', dtype=None)
            df = p.DataFrame.transpose(df)
            df = df.assign (socialsource = key,timestamp_api_call = dt.datetime.now(),source = source,symbol = raw_symbol,symbol_id = symbol_id )
            frames.append(df)

            my_file = cwd+'/data/social/%s.csv' % key

            if os.path.isfile(my_file):
                df_resident = p.DataFrame.from_csv(my_file)
                print 'appending new'+str(key)+' data: '+symbol
                frames.append(df_resident)
            else:
                print ''

            df = p.concat(frames)
            df = df.drop_duplicates(['name','talking_about','Points','likes','symbol'],  keep='last')
            df = df.sort_values('symbol')
            df = df.reset_index(drop=True)

            if not df.empty:
                df.to_csv(my_file, index_label='Id')
                print 'Updated: '+str(my_file)
            else:
                print 'No '+str(key)+' data: '+symbol

                ############################################################################################################################################################

        elif key == 'CryptoCompare':
            sub = sub_data[key]
            a = sub.pop('CryptopianFollowers', None)
            a = sub.pop('SimilarItems', None)
            a = sub.pop('PageViewsSplit', None)
            del a
            df = p.DataFrame.from_dict(sub,orient='index', dtype=None)
            df = p.DataFrame.transpose(df)
            df = df.assign (socialsource = key,timestamp_api_call = dt.datetime.now(),source = source,symbol = raw_symbol,symbol_id = symbol_id )
            frames.append(df)
            my_file = cwd+'/data/social/%s.csv' % key

            if os.path.isfile(my_file):
                df_resident = p.DataFrame.from_csv(my_file)
                print 'appending new'+str(key)+' data: '+symbol
                frames.append(df_resident)
            else:
                print ''

            df = p.concat(frames)
            df = df.drop_duplicates(['PageViews','Posts','Comments','Points','Followers','symbol'],  keep='last')
            df = df.sort_values('symbol')
            df = df.reset_index(drop=True)

            if not df.empty:
                df.to_csv(my_file, index_label='Id')
                print 'Updated: '+str(my_file)
            else:
                print 'No '+str(key)+' data: '+symbol

                ############################################################################################################################################################
        elif key == 'General':
            sub = sub_data[key]
            df = p.DataFrame.from_dict(sub,orient='index', dtype=None)
            df = p.DataFrame.transpose(df)
            df = df.assign (socialsource = key,timestamp_api_call = dt.datetime.now(),source = source,symbol = raw_symbol,symbol_id = symbol_id )
            frames.append(df)

            my_file = cwd+'/data/social/%s.csv' % key

            if os.path.isfile(my_file):
                df_resident = p.DataFrame.from_csv(my_file)
                print 'appending new'+str(key)+' data: '+symbol
                frames.append(df_resident)
            else:
                print ''

            df = p.concat(frames)
            df = df.drop_duplicates(['CoinName','Points','Type','Points','symbol'],  keep='last')
            df = df.sort_values('symbol')
            df = df.reset_index(drop=True)

            if not df.empty:
                df.to_csv(my_file, index_label='Id')
                print 'Updated: '+str(my_file)
            else:
                print 'No '+str(key)+' data: '+symbol







