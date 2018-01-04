import requests
import pandas as p
import datetime as dt
import socket
import os


    def ticker():
    cwd = '/Users/jkail/Documents/GitHub/lit_crypto_data/alpha'
    frames = []

    url = "https://api.coinmarketcap.com/v1/ticker/"

    querystring = {"limit":"0"}

    headers = {
        'cache-control': "no-cache",
        'postman-token': "a9ed8f5a-9fa5-b77b-6d59-ab97bb499b5e"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)


    data = response.json()

    df = p.DataFrame(data)
    df = df.assign( timestamp_api_call = dt.datetime.now(),hostname = socket.gethostname())
    frames.append(df)
    print(df)

    my_file = cwd+'/data/day_data/ticker.csv'
    if os.path.isfile(my_file):
        df_resident = p.read_csv(my_file,  encoding= 'utf-8')
        frames.append(df_resident)
    else:
        pass
    df = p.concat(frames)
    if not df.empty:
        df = df.drop_duplicates(['symbol','last_updated'], keep='last')
        df = df.sort_values('time')
        df = df.reset_index(drop=True)
        df.to_csv(my_file, index = False,  encoding= 'utf-8') #need to add this
        s3 = savetos3.SaveS3(my_file,catalog)
        s3.main()

ticker()