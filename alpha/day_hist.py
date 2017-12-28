import requests
import pandas as p
import datetime as dt
import os
import threading
import urllib2
import time


class GetDayHist(object):

    def __init__(self, symbol_list, exchanges):
        self.symbol_list = symbol_list
        self.exchanges = exchanges


    def get_day_hist(self,symbol):
        currentts = str(int(time.time()))
        cwd = os.getcwd()
        frames = []
        for exchange in self.exchanges:
            url = "https://min-api.cryptocompare.com/data/histoday"

            querystring = {"fsym":symbol,"tsym":"USD","limit":"2000","aggregate":"3","e":exchange,"toTs":currentts}

            headers = {
                'cache-control': "no-cache",
                'postman-token': "e00df90c-b8b6-cb28-54ff-88c19b883e0a"
            }

            response = requests.request("GET", url, headers=headers, params=querystring)
            data = response.json()

            if data["Data"] :
                df = p.DataFrame(data["Data"])
                df = df.assign(symbol = symbol, coin_units = 1, timestamp_api_call = dt.datetime.now(),computer_name = 'JordanManual',exchange = exchange )
                frames.append(df)
                my_file = cwd+'/data/day_data/'+symbol+'_day.csv'
                if os.path.isfile(my_file):
                    df_resident = p.DataFrame.from_csv(my_file)
                    frames.append(df_resident)
                else:
                    pass
                df = p.concat(frames)
                if not df.empty:
                    df = df.drop_duplicates(['time','exchange','coin'], keep='last')
                    df = df.sort_values('time')
                    df = df.reset_index(drop=True)
                    df.to_csv(my_file, index_label='Id') #need to add this
                    #print 'Updated trade pair: '+str(my_file)
                else:
                    pass

            else:
                pass #print 'Invalid: '+currentts+'   '+exchange+'  '+symbol



    def main(self):


        gmt = GetDayHist(self.symbol_list,self.exchanges)

        threads = [threading.Thread(target=gmt.get_day_hist, args=(symbol,)) for symbol in self.symbol_list]
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()






if __name__ == '__main__':
    """df_get_id = p.DataFrame.from_csv('/Users/jkail/Documents/GitHub/lit_crypto/alpha/data/coinlist_info.csv')
    b = df_get_id["Symbol"].tolist()
    symbols = b[:50]
    #print symbols
    exchanges = ['CCCAGG','Coinbase', 'Bitfinex']
    """
    runner = GetDayHist()
    runner.main()
