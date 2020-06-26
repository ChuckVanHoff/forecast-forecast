import os
import time
import json

from pymongo import MongoClient

import request_and_load
from request_and_load import read_list_from_file
import weather
import db_ops
import make_instants
import config
from config import OWM_API_key_loohoo as loohoo_key
from config import OWM_API_key_masta as masta_key
from config import port, host #, user, password, socket_path

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def get_and_make(codes):
    ''' Request weather data from the OWM api. Transform and load that data
    into a database.

    :param codes: a list of zipcodes
    :type codes: list of five-digit valid strings of US zip codes
    '''

    # Begin a timer for the process and run the request and load process.
    start_start = time.time()
    print(f'task began at {start_start}')
    i, n = 0, 0 # i for counting zipcodes processed and n for counting API
                # calls made; API calls limited to a maximum 60/minute/apikey.
    start_time = time.time()
    for code in codes:
        try:
            current = weather.get_current_weather(code)
#             print(current)
#             exit()
            ### If the data was not recieved a -1 should have been returned.
            ### When this happens it should be handled some way other than
            ### just skipping over it. And the try block is not necessary here.
        except AttributeError as e:
            print(e)
            print(f'AttributeError for {code}. Continuing to next code.')
#             exit()
            continue
        n+=1
        coords = current.loc ###['coordinates']
        try:
            forecasts = weather.five_day(coords)
        except AttributeError:
            print(f'got AttributeError for {coords}. Continuing to next code.')
            continue
        n+=1
        
        ### This should load to database the old way if the data comes in as
        ### the old way (that is, as strait up dict or list) but switch to the
        ### new way if the data was collected and returned as Weather objects.
        try:
            db_ops.load(current.as_dict, client, config.database, 'obs_temp')
            for cast in forecasts:
                db_ops.load(cast.as_dict, client, config.database, 'cast_temp')
        except:
            request_and_load.load_weather(current, client, config.database, 'obs_temp')
            request_and_load.load_weather(forecasts, client, config.database, 'cast_temp')

        # if the api request rate is greater than 60 just keep going. Otherwise
        # check how many requests have been made and if it's more than 120
        # start make_instants.
        if n/2 / (time.time()-start_time) <= 1:
            i+=1
            continue
        else:
            i+=1
            if n>=120:
                make_instants.make_instants(client)
                if time.time() - start_time < 60:
                    print(f'Waiting {start_time+60 - time.time()} seconds before resuming API calls.')
                    time.sleep(start_time - time.time() + 60)
                    start_time = time.time()
                n = 0

    # sort the last of the documents in temp collections
    try:
        make_instants.make_instants(client)
    except:
        print('No more documents to sort into instants')
    print(f'task took {time.time() - start_start}sec and processed {i} codes')

if __name__ == '__main__':
    # This try block is to deal with the switching back and forth between
    # computers with different directory names.
    try:
        directory = os.path.join(os.environ['HOME'], 'data', 'forecast-forecast')
        filename = os.path.join(directory, 'ETL', 'Extract', 'resources', 'success_zipsNC.csv')
        codes = read_list_from_file(filename)
    except FileNotFoundError:
        directory = BASE_DIR
        filename = os.path.join(directory, 'ETL', 'Extract', 'resources', 'success_zipsNC.csv')
        codes = read_list_from_file(filename)
    client = MongoClient(host=host, port=port)
    get_and_make(codes)
#     get_and_make(codes[:61])
    client.close()
