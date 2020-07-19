''' Carry out the ETL process for OpenWeatherMaps data:

    -request the data through the OWM api
    -transform it to an instance of Weather
    -sort it out into instances of Instants
    -load it to the database: local if not legit, remote if yes, legit
'''


import time
import pymongo
from pymongo import MongoClient

import owm_get
import geo_hash
import config


client = MongoClient()


def favor(value, floor=10800, trans=False):
    ''' Find the nearest floor multiple to the value.
    
    :param value: The number that you want to transform.
    :type value: Any numerical value that can be python floor divided.
    :param floor: The number that defines which multiples to use.
    :type floor: It might be best to keep it an int until more testing is done.
    :param trans: Determines if value will be transformed or not. If trans is 
    True, then value will be changed and nothing will be returned, otherwise
    value stays the same and the nearest floor value will be returned.
    :type trans: bool --Default is True
    '''

    this_floor = floor * (value//floor)
    next_floor = floor * (value//floor + 1)

    if abs(next_floor - value) <= abs(this_floor - value):
        temp = next_floor
    else:
        temp = this_floor
    if trans:
        value = temp
        return
    return temp

def party(locations, breaks=True, batch=60):
    ''' Get data for the locations given in the argument. 
    
    :param locations: the locations to gather data for
    :type locations: list of geocoordinate dictionaries
    :param breaks: run the code at reduced speed (written to handle API call
    limits). The default is True.
    :type breaks: bool
    :param batch: the number of API calls to be made before hitting the breaks
    :type batch: int. Default is 60, as that is the requests/minute rate for
    free API keys.
    '''
    
    num = len(locations)
    obs = []
    casts = []
    db = client[config.database]
    obs_col = db[config.observation_collection]
    cast_col = db[config.forecast_collection]
    start_start = time.time()  # This is for timing the WHOLE process.
    if breaks:
        i = 0
        # If the number of locations is less than batch, then the while loop
        # will not trigger and nothing will happen. Check for this condition
        if num < batch:
            batch = num

        start_time = time.time()  # This is for timing the SUB-process.
        while num - i > 0:
            # This should ensure that the rest of the list is requested and
            # that there is no IndexError caused when [i:i+batch] goes beyond
            # the indexes of locations.
            if num - i < batch:
                batch = num

            # Reset these with each pass of the while loop. #
            n = 0       
            obs = []
            casts = []

            for loc in locations[i:i+batch]:
                # Get the current observations and forecasts and add them to
                # obs and casts lists
                obs.append(owm_get.current(loc))
                n += 1
                forecast = owm_get.forecast(loc)
                n += 1
                for cast in forecast['list']:
                    casts.append(cast)

            # At this point you may have requested more than 60 times per
            # API key. Check the number, then check the requests/min rate;
            # if the rate is over 1request/second start loading the data
            # to the database.
            if n >= batch * 2:
                if n/2 / (time.time()-start_time) > 1: 
                    if isinstance(obs, dict):
                        obs_col.insert_many(obs)
                        cast_col.insert_many(casts)
                    elif isinstance(obs, list):
                        obs_col.insert_many(obs)
                        cast_col.insert_many(casts)
                    else:
                        print(type(obs), 'doing nothing')
                
                # Check the API request rate and wait a lil bit if it's high
                if n/2 / (time.time() - start_time) > 1:
                    print(f'waiting {start_time - time.time() + 60} seconds.')
                    time.sleep(start_time - time.time() + 60)
                    start_time = time.time()
            i += int(n/2)
    else:  # if there are no breaks on the process
        for loc in locations:
            # Get the forecasts and observations.
            obs.append(owm_get.current(loc))
            forecast = owm_get.forecast(loc)
            for cast in forecast['list']:
                casts.append(cast)
        # Load it to the database
        if isinstance(obs, dict):
            obs_col.insert_many(obs)
            cast_col.insert_many(casts)
        elif isinstance(obs, list):
            obs_col.insert_many(obs)
            cast_col.insert_many(casts)
        else:
            print(type(obs), 'doing nothing')
    return f'Completed pinky party and requested for {i} locations. \
        It all took {int(time.time() - start_start)} seconds'
