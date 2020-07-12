''' A single function to request weather data from the OpenWeatherMaps API
service, transform the returned data into larger, structured documents, load
the requested data and incomplete documents to the local MongoDB database, and
finally load any complete documents to a separate database. '''

import os
import time
import json

from pymongo import MongoClient
import geohash

import request_and_load
import weather
import db_ops
import make_instants
import config
from config import OWM_API_key_loohoo as loohoo_key
from config import OWM_API_key_masta as masta_key

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def read_list_from_file(filename):
    """ Read the zip codes list from the csv file.
        
    :param filename: the name of the file
    :type filename: sting
    """
    with open(filename, "r") as z_list:
        return z_list.read().strip().split(',')

def get_and_make(codes):
    ''' Request weather data from the OWM api. Transform and load that data
    into a database.

    :param codes: a list of zipcodes or geo coordinate pairs
    :type codes: list of five-digit valid strings of US zip codes
    '''

    # Begin a timer for the process and run the request and load process.
    start_start = time.time()
    print(f'Weather ETL process began at {time.ctime()}.')
    i, n = 0, 0 # i for counting zipcodes processed and n for counting API
                # calls made; API calls limited to a maximum 60/minute/apikey.
    start_time = time.time()
    for code in codes:
        try:
            current = weather.get_current_weather(code)
            n+=1 
            forecasts = weather.five_day(current.loc) # location=current_coords 
            n+=1
        except AttributeError as e:
            print(f'AttributeError for {code}. Continuing to next code.')
            continue

        # Try to load the data in the weather.Weather objects. If it can't, do
        # load it the old way in case current and forecasts are dict and list.
        try:
            db_ops.load(
                current.as_dict,
                config.database,
                config.observation_collection
            )
            for cast in forecasts:
                db_ops.load(
                    cast.as_dict,
                    config.database,
                    config.forecast_collection
                )
        except:
            print(f'''There was an error while get_and_make.get_and_make() was
            attempting to load to {client}. Now trying to use request_and_load.
            load_weather() to do the same thing.''')
            request_and_load.load_weather(
                current,
                client,
                config.database,
                'obs_temp'
                )
            request_and_load.load_weather(
                forecasts,
                client,
                config.database,
                'cast_temp'
                )

        # If the api request rate is greater than 60 just keep requesting.
        # Otherwise check how many requests have been made, and if it's more
        # than 120 start sorting the data to instants while you reduce the
        # request rate by waiting.
        if n/2 / (time.time()-start_time) <= 1:
            i+=1
            continue
        else:
            i+=1
            if n>=120:
                make_instants.make_instants(
                    client,
                    config.forecast_collection, 
                    config.observation_collection,
                    config.instants_collection
                )
                if time.time() - start_time < 60:
                    print(f'Waiting {start_time+60 - time.time()} seconds before resuming API calls.')
                    time.sleep(abs(start_time - time.time() + 60))
                    start_time = time.time()
                n = 0

    # Sort the last of the documents in temp collections
    try:
        make_instants.make_instants()
    except:
        print('No more documents to sort into instants')
    print(f'''Weather ETL process has concluded.
    It took {time.time() - start_start} seconds and processed {i} locations''')

if __name__ == '__main__':
    ### Commented after update to get current by geocoord was made. ###
#     directory = os.path.join(os.environ['HOME'], 'data', 'forecast-forecast')
#     filename = os.path.join(directory, 'ETL', 'resources', 'success_zipsNC.csv')
#     codes = read_list_from_file(filename)

    client = MongoClient(config.host, config.port)

    # Create a geohash list and convert it to a list of coordinate locations
    # from a geohash list.
    b32 = '0123456789bcdefghjkmnpqrstuvwxyz'
    hl = [f'dn{p3}{p4}{p5}' for p3 in b32[16:24] for p4 in b32 for p5 in b32]
    locations = []  # Coordinate list
    for row in hl:
        cd = {}  # Coordinate dict
        cd['lon'] = geohash.decode(row)[0]
        cd['lat'] = geohash.decode(row)[1]
        locations.append(cd)

    limit = 61
    print(f'The number of locations is limited to {limit}.')
    get_and_make(locations[:limit])
