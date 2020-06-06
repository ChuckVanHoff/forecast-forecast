import os
import time
import json

from pymongo import MongoClient

from request_and_load import get_current_weather, five_day, load_weather, read_list_from_file
from make_instants import make_instants

from config import OWM_API_key_loohoo as loohoo_key, OWM_API_key_masta as masta_key
from config import port, host, uri


def get_and_make(codes):
    ''' Request weather data from the OWM api. Transform and load that data into a database.
    
    :param codes: a list of zipcodes
    :type codes: list of five-digit valid strings of US zip codes
    '''
    
    # Begin a timer for the process and run the request and load process.
    start_start = time.time()
    print('task began at {start_start}')
    i, n = 0, 0 # i for counting zipcodes processed and n for counting API calls made; API calls limited to a maximum of 60/minute/apikey.
    start_time = time.time()
    for code in codes:
        try:
            current = get_current_weather(code)
        except AttributeError:
            print('got AttributeError while collecting current weather for {code}. Continuing to next code.')
            continue
        n+=1
        coords = current['coordinates']         
        try:
            forecasts = five_day(coords, code=code)
        except AttributeError:
            print('got AttributeError while collecting forecasts for {code}. Continuing to next code.')
            continue
        n+=1
        load_weather(current, client, 'owmap', 'obs_temp')
        load_weather(forecasts, client, 'owmap', 'cast_temp')
        
        # if the api request rate is greater than 60 just keep going. Otherwise check how many requests have been made
        # and if it's more than 120 start make_instants.
        if n/2 / (time.time()-start_time) <= 1:
            i+=1
            continue
        else:
            i+=1
            if n>=120:
                make_instants(client)
                if time.time() - start_time < 60:
                    print('Waiting {start_time+60 - time.time()} seconds before resuming API calls.')
                    time.sleep(start_time - time.time() + 60)
                    start_time = time.time()
                n = 0

    # sort the last of the documents in temp collections
    try:
        make_instants(client)
    except:
        print('No more documents to sort into instants')
    print('task took {time.time() - start_start} seconds and processed {i} zipcodes')

if __name__ == '__main__':

    from make_instants import Client
    
    # this try block is to deal with the switching back and forth between computers with different directory names
    try:
        directory = os.path.join(os.environ['HOME'], 'forecast')
        filename = os.path.join(directory, 'success_zipsNC.csv')
        codes = read_list_from_file(filename)
    except FileNotFoundError:
        print('file not found')
        # directory = os.path.join(os.environ['HOME'], 'data', 'forecast-forecast')
        # filename = os.path.join(directory, 'ETL', 'Extract', 'resources', 'success_zipsNC.csv')
        # codes = read_list_from_file(filename)
    client = Client(uri=uri)
    if type(client) == None:
        print('at line 80 in get_and_make, and client is NoneType again! I will try to reestablish the client')
        client = Client(uri=uri)
    get_and_make(codes)
    client.close()