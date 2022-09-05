import time
import json
import pymongo
from pymongo import MongoClient

import owm_get
import geo_hash
import make_instants
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

def party(locations, breaks=True, batch=60, e_r=True, client=config.client, load_raw=False):
    ''' Carry out the ETL process for OpenWeatherMaps data:

    -request the data through the OWM api
    -transform it to an instance of Weather
    -sort it out into instances of Instants
    -load it to the database: local if not legit, remote if yes, legit

    :param locations: the locations to gather data for
    :type locations: list of geocoordinate dictionaries
    :param breaks: run the code at reduced speed (written to handle API call
    limits). The default is True.
    :type breaks: bool
    :param batch: the number of API calls to be made before hitting the breaks
    :type batch: int. Default is 60, as that is the requests/minute rate for
    free API keys.
    :param e_r: give error reports or pass over them
    :type e_r: bool
    :param load_raw: load data raw or edit it before loading
    :type load_raw: bool
    '''

    num = len(locations)
    good_grabs = []
    error_reports = []
    
    db = client[config.database]
    weathers_col = db[config.weathers_collection]
    
    print('Lets get this party started!!')
    start_start = time.time()  # This is for timing the WHOLE process.
    if breaks:
        i = 0
        # If the number of locations is less than batch, then the while loop
        # will not trigger and nothing will happen. Check for this condition
        if num < batch:
            batch = num

        start_time = time.time()  # This is for timing the SUB-process.
        while num - i > 0 and (time.time()-start_time) < 9000:
            # Stop the collection process to make way for the dump and restore.
            if (time.time()-start_time) > 9000:
                print(f'''Data collection timeout has occured after
                {time.time()-start_time} seconds of operation.''')
                break
            # This should ensure that the rest of the list is requested and
            # that there is no IndexError caused when [i:i+batch] goes beyond
            # the indexes of locations.
            if num - i < batch:
                batch = num

            # Reset these with each pass of the while loop. #
            n = 0
            data = []
            for loc in locations[i:i+batch]:
                # Get the current observations and forecasts and keep count
                # of api requests with n.
                data.append(owm_get.current(loc))
                n += 1
                if load_raw:
                    data.append(owm_get.forecast(loc))
                    n += 1
                else:
                    forecast = owm_get.forecast(loc)
                    n += 1
                    for cast in forecast['list']:
                        data.append(cast)
                good_grabs.append(loc)

            # At this point you may have requested more than 60 times per
            # API key. Check the number, then check the requests/min rate;
            # if the rate is over 1request/second start loading the data
            # to the database.
            if n >= batch * 2:
                if n/2 / (time.time()-start_time) > 1: 
                    if isinstance(data, list):
                        try:
                            result = weathers_col.insert_many(data, ordered=False)
                        except pymongo.errors.BulkWriteError as e:
                            for item in e.details['writeErrors']:
                                report = {item['errmsg'], item['op']}
                                error_reports.append(report)
                            print('Added bulkWriteError reports.')
                    else:
                        print(type(data), 'pinky.party() doing nothing')

                # Check the API request rate and wait a lil bit if it's high,
                # otherwise take advantage of the wait time to make_instants.
                if n/2 / (time.time() - start_time) > 1:
                    print(f'waiting {start_time - time.time() + 60} seconds.')
                    time.sleep(start_time - time.time() + 60)
                    start_time = time.time()
                else:
                    print(f'been waiting for something like {time.time() - start_time - 60} seconds.')
                    start_time = time.time()
            i += int(n/2)
            # Now that the data grab was good and the data load was also good,
            # record the timeplaces into the progress log.
            with open('progress_log.txt', 'a') as pl:
                for loc in good_grabs:
                    pl.write(str(loc) + '\n')
    else:  # if there are no breaks on the process...
        for loc in locations:
            # Get the forecasts and observations.
            data.append(owm_get.current(loc))
            forecast = owm_get.forecast(loc)
            for cast in forecast['list']:
                data.append(cast)
        # Load it to the database
        if isinstance(data, list):
            weathers_col.insert_many(data)
        else:
            print(type(weathers), 'doing nothing')
    
    if e_r:
        filename = 'error_reports.txt'
        with open(filename, 'a+') as f:
            for row in error_reports:
                f.write(row)    
    print(f'''Completed pinky party and requested for {i} locations. 
    It all took {int(time.time() - start_start)} seconds.''')
    return
