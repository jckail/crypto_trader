import os
cwd = os.getcwd()
from time import sleep
from tqdm import tqdm


def setup_alpha():
    create_list = ['/data/','/data/day_data/','/data/hour_data/','/data/mining_data/','/data/minute_data/','/data/social/','/data/trading_pair/']

    for y in tqdm(create_list,desc='validate_directories'):
        directory = cwd+y
        gdirectory = cwd+y+"gzip_files/"
        if not os.path.exists(directory):
            os.makedirs(directory)
            print ('Created: '+directory)
        if not os.path.exists(gdirectory):
            os.makedirs(gdirectory)
            print ('Created: '+gdirectory)



