''' This program collects weather data from OpenWeatherMaps API and stores it in a database.
the data collection happens every 3 hours (the granularity of the weather data available).
'''

import json
import os
import time
import urllib
# import dns
# import pandas as pd

from pyowm import OWM
from pyowm.weatherapi25.forecast import Forecast
from pyowm.exceptions.api_response_error import NotFoundError
from pyowm.exceptions.api_call_error import APICallTimeoutError, APIInvalidSSLCertificateError
from pymongo import MongoClient
from pymongo.collection import Collection, ReturnDocument
from pymongo.errors import ConnectionFailure, InvalidDocument, DuplicateKeyError, OperationFailure
from urllib.parse import quote
from config import OWM_API_key as key, connection_port, user, password, socket_path

zipcode = str
zlon = float
zlat = float
client = MongoClient()

API_key = key
# loc_host = loc_host
# rem_host = remo_host
port = connection_port
owm = OWM(API_key)    # the OWM object
password = quote(password)    # url encode the password for the mongodb uri
uri = "mongodb+srv://%s:%s@%s" % (user, password, socket_path)
print(uri)

def make_zip_list(state):
    ''' Make a list of zip codes in the specified state.
        Read zip codes from downloadable zip codes csv file available at https://www.unitedstateszipcodes.org/
        
        :param state: the two-letter abreviation of the state whose zip codes you'd like listed
        :type state: string
        
        :returns success_zips: list of zip codes that OWM has records for
        :type success_zips: list
    '''
    
    global owm
    
    successes = 0
    exceptions = 0
    success_zips = []
    fail_zips = []
    
    df = pd.read_csv("resources/zip_code_database.csv")
    # Make a datafram from the rows with the specified state and write the 'zip' column to a list 
    zip_list = df.loc[lambda df: df['state'] == f'{state.upper()}']['zip'].tolist()
    for zipp in zip_list:
        if int(zipp) > 10000:
            try:
                set_location(zipp)
                success_zips.append(zipp)
                successes+=1
                time.sleep(.9)
            except APIInvalidSSLCertificateError:
                print(f'except first try: APIInvalidSSLCertificateError with zipcode {zipp}...trying again')
                set_location(zipp)
                success_zips.append(zipp)
                successes+=1
                print('this time it worked')
                time.sleep(.9)
            except APIInvalidSSLCertificateError:
                print('except on second try: APIInvalidSSLCertificateError - reestablishing the OWM object and trying again.')
                owm = OWM(API_key)    # the OWM object
                set_location(zipp)
                success_zips.append(zipp)
                successes+=1
                print('this time it worked')
                time.sleep(.9)
            except APIInvalidSSLCertificateError:
                print('....and again... this time I am just gonna pass.')
                pass
            except NotFoundError:
                print("except", f'NotFoundError with zipcode {zipp}')
                fail_zips.append(zipp)
                exceptions+=1
                pass
    write_list_to_file(success_zips, f'resources/success_zips{state}.csv')
    write_list_to_file(fail_zips, f'resources/fail_zips{state}.csv')
    print(f'successes = {successes}; exceptions = {exceptions}, all written to files')
    return(success_zips)

def write_list_to_file(zip_list, filename):
    """ Write the zip codes to csv file.
        
        :param zip_list: the list created from the zip codes dataframe
        :type zip_list: list stings
        :param filename: the name of the file
        :type filename: sting
    """
    with open(filename, "w") as z_list:
        z_list.write(",".join(str(z) for z in zip_list))
    return
        
def read_list_from_file(filename):
    """ Read the zip codes list from the csv file.
        
        :param filename: the name of the file
        :type filename: sting
    """
    with open(filename, "r") as z_list:
        return z_list.read().strip().split(',')

def set_location_and_get_current(code):
    ''' Get the latitude and longitude corrosponding to the zip code.
        
        :param code: the zip code to find weather data about
        :type code: string
    '''
    global owm
    
    try:
        obs = owm.weather_at_zip_code(f'{code}', 'us')
    except APIInvalidSSLCertificateError:
        print(f'except first try in set_location(): APIInvalidSSLCertificateError with zipcode {code}...trying again')
        try:
            obs = owm.weather_at_zip_code(f'{code}', 'us')        
            print('this time it worked')
        except APIInvalidSSLCertificateError:
            print('except on second try in set_location(): APIInvalidSSLCertificateError - reestablishing the OWM object and trying again.')
            try:
                owm = OWM(API_key)    # the OWM object
                obs = owm.weather_at_zip_code(f'{code}', 'us')
                print('this time it worked')
            except APIInvalidSSLCertificateError:
                print('....and again... this time I am just gonna return.')
                return(f'the time is {time.time()}')
    except APICallTimeoutError:
        print('caught APICallTimeoutError on first try in set_location()...trying again')
        time.sleep(5)
        try:
            obs = owm.weather_at_zip_code(f'{code}', 'us')
            print('this time it worked')
        except APICallTimeoutError:
            time.sleep(5)
            print('caught APICallTimeoutError on second try in set_location()...trying again')
            try:
                obs = owm.weather_at_zip_code(f'{code}', 'us')
                print('this time it worked')
            except APICallTimeoutError:
                print(f'could not get past the goddamn api call for {code}! Returning with nothing but shame this time.')
                return(f'the time is {time.time()}')
    current = json.loads(obs.to_JSON())
    # update the 'current' object with the fields needed for making the processing data
    current['instant'] = 10800*(current['Weather']['reference_time']//10800 + 1)
    current['location'] = current['Location']['coordinates']
    current['zipcode'] = code
    current.pop('Location')
    current['Weather'].pop('sunset_time')
    current['Weather'].pop('sunrise_time')
    current['Weather']['temperature'].pop('temp_kf')
    current['Weather'].pop('weather_icon_name')
    current['Weather'].pop('visibility_distance')
    current['Weather'].pop('dewpoint')
    current['Weather'].pop('humidex')
    current['Weather'].pop('heat_index')

#     print(current)
    return(current)

def five_day(zlat, zlon):
    ''' Get each weather forecast for the corrosponding zip code. 
    
        :return five_day: the five day, every three hours, forecast for the zip code
        :type five_day: dict
    '''
    
    global owm
    
    try:
        forecaster = owm.three_hours_forecast_at_coords(zlat, zlon)
    except APIInvalidSSLCertificateError:
        print(f'except on first try in five_day(): APIInvalidSSLCertificateError ...trying again')
        try:
            forecaster = owm.three_hours_forecast_at_coords(zlat, zlon)
            print('this time it worked')
        except APIInvalidSSLCertificateError:
            print('except on second try five_day(): APIInvalidSSLCertificateReeor... reestablish the OWM object and try again.')
            try:
                owm = OWM(API_key)    # the OWM object
                forecaster = owm.three_hours_forecast_at_coords(zlat, zlon)
                print('this time it worked')
            except APIInvalidSSLCertificateError:
                print('....and again... this time I am just gonna return.')
                return(f'the time is {time.time()}')
    except APICallTimeoutError:
        print('caught APICallTimeoutError on fritst try in five_day(). trying again...')
        time.sleep(.5)
        try:
            forecaster = owm.three_hours_forecast_at_coords(zlat, zlon)
        except APICallTimeoutError:
            print('caught APICallTimeoutError on second try in five_day(). trying again...')
            time.sleep(.5)
            try:
                forecaster = owm.three_hours_forecast_at_coords(zlat, zlon)
            except APICallTimeoutError:
                print('caught APICallTimeoutError on third try in five_day()...returning without another try.')
                return(f'the time is {time.time()}')
    # Get the actual forecasts and add them to a list to be passed on while changing the key 'reference_time' to 'instant'
    forecast = forecaster.get_forecast()
    f = json.loads(forecast.to_JSON())
    forecasts = f['weathers']
    for forecast in forecasts:
        # update the 'current' object with the fields needed for making the processing data
        forecast['instant'] = forecast.pop('reference_time')
        forecast['location'] = f['Location']['coordinates']
        forecast['time_to_instant'] = forecast['instant'] - f['reception_time']
        # remove all the fields that will give null values
        ### You should figure out how to handle the null values ###
        forecast.pop('sunset_time')
        forecast.pop('sunrise_time')
        forecast['temperature'].pop('temp_kf')
        forecast.pop('weather_icon_name')
        forecast.pop('visibility_distance')
        forecast.pop('dewpoint')
        forecast.pop('humidex')
        forecast.pop('heat_index')
    return(forecasts)

def sort_casts(forecasts, code, client):
    ''' Take the array of forecasts from the five day forecast and sort them into the documents of the instants collection.
        
        :param forecasts: the forecasts from five_day()-  They come in a list of 40, one for each of every thrid hour over five days
        :type forecasts: list- expecting a list of forecasts
        :param code: the zipcode
        :type code: string
        :param client: the mongodb client
        :type client: MongoClient
    '''
    db = client.test
    col = db.instant
    # update each forecast and insert it to the instant document with the matching instant_time and zipcode
    for forecast in forecasts:
        # now find the document that has that code and that ref_time
        # This should find a single instant specified by zip and the forecast ref_time and append the forecast to the forecasts object
        filter_by_zip_and_inst = {'zipcode': code, 'instant': forecast['instant']}
        filters = filter_by_zip_and_inst
        add_forecast_to_instant = {'$push': {'forecasts': forecast}} # append the forecast object to the forecasts list
        updates = add_forecast_to_instant
        updated = col.find_one_and_update(filters, updates, upsert=True, return_document=ReturnDocument.AFTER)
    return


# def check_db_access(loc_host, port):
def check_db_access(uri):
    ''' Check the database connection and return the client
    
        :param host: the database host
        :type host: string
        :param port: the database connection port
        :type port: int
        :param uri: the conneciton uri for the remote mongo database
        :type uri: sting
    '''
#     client = MongoClient(host=host, port=port)
    client = MongoClient(uri)    
    try:
        client.admin.command('ismaster')
    except ConnectionFailure:
        print("Server not available")
        return
    return(client)

def to_json(data, code):
    ''' Store the collected data as a json file in the case that the database
        is not connected or otherwise unavailable.
        
        :param data: the dictionary created from the api calls
        :type data: dict
        :param code: the zip code associated with the data from the list codes
        :type code: sting
    '''
    collection = data
    directory = os.getcwd()
    save_path = os.path.join(directory, 'Data', f'{code}.json')
    Data = open(save_path, 'a+')
    Data.write(collection)
    Data.close()
    return

def load(data, client, name):
    ''' Load the data to the database if possible, otherwise write to json file. 
        
        :param data: the dictionary created from the api calls
        :type data: dict
        :param client: the pymongo client object
        :type client: MongoClient
        :param name: the database collection to be used
        :type name: 
    '''
    database = client.test
    col = Collection(database, name)
    if type(data) == dict:
        filters = {'zipcode':data['zipcode'], 'instant':data['instant']}
        updates = {'$setOnInsert': data}
        try:
            # check to see if there is a document that fits the parameters. If there is, update it, if there isn't, upsert it
            update = col.find_one_and_update(filters, updates,  upsert=True, return_document=ReturnDocument.BEFORE)
            if update == None:
                print(f'upserted document to {name} according to the filter {filters}')
                return
            else:
                print(f'updated document in {name} according to the filter {filters}')
        except DuplicateKeyError:
            return(f'DuplicateKeyError, could not insert {data} into {name}')
    else:
        print('Do something about the data coming into load() not as a dict')
        return(data, client, name)


if __name__ == '__main__':
    # Run everything. Read the zip codes from the zipcode list and call the weather and forecasts and load them to the database
#     directory = os.path.join(os.environ['HOME'], 'data', 'forecast-forecast')  # for macbook pro
    directory = os.path.join(os.environ['HOME'], 'data', 'forcast-forcast')  # for macbook air
    filename = os.path.join(directory, 'ETL', 'Extract', 'resources', 'success_zipsNC.csv')
    codes = read_list_from_file(filename)
    print(f'task began at {time.localtime()}')
    client = MongoClient(uri)
    for code in codes[0]:
        current = set_location_and_get_current(code)
        zlat = current['location']['lat']
        zlon = current['location']['lon']
        load(current, client, 'instant')
        forecasts = five_day(zlat, zlon)
        sort_casts(forecasts, code, client)
    client.close()
    print(f'task ended at {time.localtime()}')