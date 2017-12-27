#!/usr/bin/env python

__author__ = 'jkail'

import requests
import pandas as p
import datetime as dt
import os


class GetDtlPrice(object):

    def __init__(self, symbols):
        self.symbols = symbols

    def get_get_details_for_symbols(self):
        symbols = self.symbols
        frames = []
        cwd = os.getcwd()
        xsymbols = [symbols[x:x+50] for x in xrange(0, len(symbols), 50)]
        #print xsymbols

        for symbols in xsymbols:

            api_call_symbols = ','.join(symbols)
            #print api_call_symbols

            url = "https://min-api.cryptocompare.com/data/pricemultifull"

            querystring = {"fsyms":api_call_symbols,"tsyms":'USD',"e":"CCCAGG"}

            headers = {
                'cache-control': "no-cache",
                'postman-token': "f3d54076-038b-9e2d-1ff3-593ae13aabbf"
            }

            response = requests.request("GET", url, headers=headers, params=querystring)
            data = response.json()
            #print data

            for key in data['RAW'].keys():
                test_df = p.DataFrame.from_dict(data['RAW'][key],orient='Columns', dtype=None)
                test_df = p.DataFrame.transpose(test_df)
                test_df = test_df.assign (coin = key, coin_units = 1, timestamp_api_call = dt.datetime.now(),computer_name = 'JordanManual') ##replace with ec2ip/region
                frames.append(test_df)

        my_file = cwd+'/data/current_dtl_price.csv'

        if os.path.isfile(my_file):
            df_resident = p.DataFrame.from_csv(my_file)
            print 'appending new data price dtl '
            frames.append(df_resident)
        else:
            print 'no new data to append'

        df = p.concat(frames)
        df = df.sort_values('LASTUPDATE')
        df = df.drop_duplicates(['FROMSYMBOL','LASTUPDATE','LASTMARKET'], keep='last')
        df = df.sort_values('LASTUPDATE')
        df = df.reset_index(drop=True)
        df.to_csv(cwd+'/data/current_dtl_price.csv',encoding='utf-8', index_label='Id')
        return df

    def main(self):
        """

        :return:
        """
        print 'begin: GetDtlPrice.main'
        try:
            gdl = GetDtlPrice(self.symbols)
            df = gdl.get_get_details_for_symbols()
            return df

        except:
            print 'Error: GetDtlPrice.main'

        print 'end: GetDtlPrice.main'


if __name__ == '__main__':
    """

    :return:
    """
    runner = GetDtlPrice()
    runner.main()


