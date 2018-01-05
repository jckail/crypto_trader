#!/usr/bin/env python

__author__ = 'jkail'

import requests
import pandas as p
import datetime as dt
import os
import threading
from time import sleep
from tqdm import tqdm
import coinlist
import savetos3
import socket
import traceback
import logging
import time


class GetSocialData(object):

    def __init__(self, symbol_list,exchanges,chunksize,cwd,catalog,reddit_ls,coderepository_ls,twitter_ls,cryptocompare_ls,general_ls,facebook_ls):

        self.symbol_list = symbol_list
        self.exchanges = exchanges
        self.chunksize = chunksize
        self.cwd = cwd
        self.catalog = catalog
        self.reddit_ls = reddit_ls
        self.coderepository_ls = coderepository_ls
        self.twitter_ls = twitter_ls
        self.cryptocompare_ls = cryptocompare_ls
        self.general_ls = general_ls
        self.facebook_ls = facebook_ls

    def get_socials(self,symbol,error_symbols):
        symbol_list = self.symbol_list
        symbol_count = 0
        len_symbol_list = len(symbol_list)

        symbol_count += 1
        source = 'cryptocompare'
        raw_symbol = symbol
        symbol = "'"+symbol+"'" #must add for df query
        df_get_id = p.read_csv(self.cwd+'/data/coininfo/coininfo.csv')
        a = df_get_id.query("Symbol == "+symbol)
        b = a["Id"].tolist()
        if len(b) > 0:

            symbol_id = b[0]
            url = "https://www.cryptocompare.com/api/data/socialstats/"

            querystring = {"id":symbol_id}

            headers = {
                'cache-control': "no-cache",
                'postman-token': "557b5e23-a352-f2d3-938d-fe17d490eee9"
            }

            try:
                response = requests.request("GET", url, headers=headers, params=querystring)

                if response.status_code == 200:
                    data = response.json()
                    if data["Data"]:
                        sub_data = data["Data"]
                        keys = list(sub_data.keys())
                        for key in keys:
                            frames =[]
                            ############################################################################################################################################################
                            if key == 'CodeRepository':
                                code_repository = sub_data[key]
                                sub = code_repository['List']
                                df = p.DataFrame(sub)
                                if not df.empty:
                                    df = df.assign (socialsource = key,utc = time.time(),hostname = socket.gethostname(),source = source,symbol = raw_symbol,symbol_id = symbol_id )
                                    self.coderepository_ls.append(df)

                                    ############################################################################################################################################################

                            elif key == 'Reddit':
                                sub = sub_data[key]
                                df = p.DataFrame.from_dict(sub,orient='index', dtype=None)
                                df = p.DataFrame.transpose(df)
                                df = df.query("Points > 0 ")
                                if not df.empty:
                                    df = df.assign (socialsource = key,utc = time.time(),hostname = socket.gethostname(),source = source,symbol = raw_symbol,symbol_id = symbol_id )
                                    self.reddit_ls.append(df)

                                    ############################################################################################################################################################
                            elif key == 'Twitter':
                                sub = sub_data[key]
                                df = p.DataFrame.from_dict(sub,orient='index', dtype=None)
                                df = p.DataFrame.transpose(df)
                                df = df.query("Points > 0 ")
                                if not df.empty:
                                    df = df.assign (socialsource = key,utc = time.time(),hostname = socket.gethostname(),source = source,symbol = raw_symbol,symbol_id = symbol_id )
                                    self.twitter_ls.append(df)

                                    ############################################################################################################################################################
                            elif key == 'Facebook':
                                sub = sub_data[key]
                                df = p.DataFrame.from_dict(sub,orient='index', dtype=None)
                                df = p.DataFrame.transpose(df)
                                df = df.query("Points > 0 ")
                                if not df.empty:
                                    df = df.assign (socialsource = key,utc = time.time(),hostname = socket.gethostname(),source = source,symbol = raw_symbol,symbol_id = symbol_id )
                                    self.facebook_ls.append(df)

                                    ############################################################################################################################################################

                            elif key == 'CryptoCompare':
                                sub = sub_data[key]
                                a = sub.pop('CryptopianFollowers', None)
                                a = sub.pop('SimilarItems', None)
                                a = sub.pop('PageViewsSplit', None)
                                del a
                                if not df.empty:
                                    df = p.DataFrame.from_dict(sub,orient='index', dtype=None)
                                    df = p.DataFrame.transpose(df)
                                    df = df.assign (socialsource = key,utc = time.time(),hostname = socket.gethostname(),source = source,symbol = raw_symbol,symbol_id = symbol_id )
                                    #print df
                                    self.cryptocompare_ls.append(df)

                                    ############################################################################################################################################################
                            elif key == 'General':
                                sub = sub_data[key]
                                df = p.DataFrame.from_dict(sub,orient='index', dtype=None)
                                if not df.empty:
                                    df = p.DataFrame.transpose(df)
                                    df = df.assign (socialsource = key,utc = time.time(),hostname = socket.gethostname(),source = source,symbol = raw_symbol,symbol_id = symbol_id )
                                    #print self.general_ls
                                    self.general_ls.append(df)
                                    #print self.general_ls

                    else:
                        print('No Social Data'+str(symbol)+' '+str(symbol_count)+'/'+str(len_symbol_list))
                else:
                    print('Invald coin'+str(symbol)+' '+str(symbol_count)+'/'+str(len_symbol_list))

            except requests.exceptions.RequestException as e:
                logging.info('------')
                logging.error(traceback.format_exc())
                logging.info('------')
                logging.exception(traceback.format_exc())
                logging.info('------')
                error_symbols.append(symbol)
                sleep(0.2)
                pass
            except OverflowError:
                logging.info('------')
                logging.error(traceback.format_exc())
                logging.info('------')
                logging.exception(traceback.format_exc())
                logging.info('------')
                print('OverflowError: '+str(symbol))
                pass
            except Exception as e:
                logging.info('------')
                logging.error(traceback.format_exc())
                logging.info('------')
                logging.exception(traceback.format_exc())
                logging.info('------')
                pass

    def create_social_files(self,social_dict,drop_dupe_dict,key):

        try:

            #print key
            my_file = self.cwd+'/data/social/'+key+'/'+key+'.csv'
            #print(my_file)
            #print(social_dict[key])
            workinglist = list(social_dict[key])

            #print workinglist
            if os.path.isfile(my_file):
                df_resident = p.read_csv(my_file,  encoding= 'utf-8')
                workinglist.append(df_resident)
            else:
                pass
            df = p.concat(workinglist)
            if not df.empty:
                if key in list(drop_dupe_dict.keys()):
                    #print(key)
                    #print(drop_dupe_dict[key])
                    df = df.drop_duplicates(drop_dupe_dict[key], keep='last')
                    pass

                else:
                    print('drop dupe no key')
                    #df = df
                if key in ['reddit','twitter','facebook']:
                    df = df.query("Points > 0 ")

                df = df.sort_values('symbol')
                df = df.reset_index(drop=True)
                if not df.empty:
                    df.to_csv(my_file, index = False,  encoding= 'utf-8')
                    #print(self.catalog)
                    s3 = savetos3.SaveS3(my_file,self.catalog)
                    s3.main()
                else:
                    pass
        except Exception as e:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            print(e)
            print(key)


    def main_run(self):

        """

        :return:
        """

        print('begin: GetSocialData.main')
        try:
            error_symbols = []
            gsd = GetSocialData(self.symbol_list,self.exchanges,self.chunksize,self.cwd,self.catalog, \
                                self.reddit_ls, \
                                self.coderepository_ls, \
                                self.twitter_ls, \
                                self.cryptocompare_ls, \
                                self.general_ls, \
                                self.facebook_ls)
            #gsd.get_socials()

            xsymbols = [self.symbol_list[x:x+self.chunksize] for x in range(0, len(self.symbol_list), self.chunksize )]


            for symbol_list in tqdm(xsymbols,desc='get_socials'):

                threads = [threading.Thread(target=gsd.get_socials, args=(symbol,error_symbols,)) for symbol in symbol_list]

                for thread in threads:
                    thread.start()

                #for thread in tqdm(threads,desc='Closed Threads'):
                for thread in threads:
                    thread.join()

                    if len(error_symbols) > 0:
                        xsymbols.append(error_symbols)
                        error_symbols = []
                    else:
                        pass


            social_dict = {"reddit":self.reddit_ls,"coderepository":self.coderepository_ls,"twitter":self.twitter_ls,"cryptocompare":self.cryptocompare_ls,"general":self.general_ls,"facebook":self.facebook_ls}
            #
            drop_dupe_dict ={"reddit":['name','comments_per_hour','posts_per_day','Points','subscribers','symbol']\
                             ,"coderepository":['closed_issues','closed_pull_issues','language','last_update','subscribers','stars','symbol'] \
                             ,"twitter":['account_creation','name','Points','followers','statuses','symbol']\
                             ,"cryptocompare":['PageViews','Posts','Comments','Points','Followers','symbol']\
                             ,"general":['CoinName','Points','Type','Points','symbol']\
                             ,"facebook":['name','talking_about','Points','likes','symbol']\
                             }
            #
            #print social_dict
            keys = list(social_dict.keys())
            #print(keys)
            #gsd.create_social_files(key)


            threads = [threading.Thread(target=gsd.create_social_files, args=(social_dict,drop_dupe_dict,key,)) for key in tqdm(keys,desc='save_socials')]

            for thread in threads:
                thread.start()

            #for thread in tqdm(threads,desc='Closed Threads'):
            for thread in threads:
                thread.join()

                if len(error_symbols) > 0:
                    xsymbols.append(error_symbols)
                    error_symbols = []
                else:
                    pass

        except requests.exceptions.RequestException as e:
            print(e)
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
        except OverflowError:
            print('OverflowError: ')
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
        except Exception as e:
            logging.info('------')
            logging.error(traceback.format_exc())
            logging.info('------')
            logging.exception(traceback.format_exc())
            logging.info('------')
            print(e)

        #print 'end: GetSocialData.main'
        print('DONE')

    def main(self):
        gsd = GetSocialData(self.symbol_list,self.exchanges,self.chunksize,self.cwd,self.catalog, \
                            self.reddit_ls, \
                            self.coderepository_ls, \
                            self.twitter_ls, \
                            self.cryptocompare_ls, \
                            self.general_ls, \
                            self.facebook_ls)
        if os.path.isfile(self.cwd+'/data/coininfo/coininfo.csv'):
            gsd.main_run()

        else:
            print("repopulating coinlist")
            cl = coinlist.GetCoinLists(self.cwd,self.catalog)
            cl.main()
            gsd.main_run()


if __name__ == '__main__':
    """

    :return:
    """

    # cwd = '/Users/jkail/Documents/GitHub/lit_crypto_data/alpha'
    # catalog = 'litcryptodata'
    # reddit_ls = []
    # coderepository_ls = []
    # twitter_ls = []
    # cryptocompare_ls = []
    # general_ls = []
    # facebook_ls = []
    # df = p.read_csv(cwd+'/data/coininfo/coininfo.csv')
    # symbol_list = df["Symbol"].tolist()
    # #symbol_list = symbol_list [:1000]
    # #symbol_list = ['BRIT']
    # exchanges = ['Bitfinex','Bitstamp','coinone','Coinbase','CCCAGG']
    # chunksize = 200
    #
    #
    # runner = GetSocialData(symbol_list,exchanges,chunksize,cwd,catalog, \
    #                        reddit_ls, \
    #                        coderepository_ls, \
    #                        twitter_ls, \
    #                        cryptocompare_ls, \
    #                        general_ls, \
    #                        facebook_ls)
    runner = GetSocialData()
    runner.main()

"""
            drop_dupe_dict ={"reddit":['name','comments_per_hour','posts_per_day','Points','subscribers','symbol']\
                             ,"coderepository":['closed_issues','closed_pull_issues','language','last_update','subscribers','stars','symbol'] \
                             ,"twitter":['account_creation','name','Points','followers','statuses','symbol']\
                             ,"cryptocompare":['PageViews','Posts','Comments','Points','Followers','symbol']\
                             ,"general":['CoinName','Points','Type','Points','symbol']\
                             ,"facebook":['name','talking_about','Points','likes','symbol']\
                             }

"""

