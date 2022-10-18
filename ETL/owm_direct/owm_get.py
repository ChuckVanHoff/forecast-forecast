import time
import json
import requests
import pandas as pd

import api_handles
import geo_hash
import pinky
import config

cast_key = config.OWM_API_key_masta
obs_key = config.OWM_API_key_loohoo

def forecast(location, as_df=False):
    ''' Get raw data from OWM api for current observations at the given
    location.
    
    :param location: The location around which to search for forecasts.
    :type location: dict-- {'lat': <[-90,90]>, 'lon': <(-180,180)>}
    :param as_df: Flags result to be returned as a dataframe; default is False.
    :type as_df: bool
    '''
    
    lat = location['lat']
    lon = location['lon']
    url = f'http://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={cast_key}'

    result = api_handles.retry(requests.get, url).json()
    if result == -1:
        result = {'loc': location, 'time': time.ctime()}
        return result
    # Add these fields to the data to be used in analysis.
    result['location'] = location
    result['dt'] = int(time.time())
    result['type'] = 'cast'
    if as_df:
        result = pd.json_normalize(result)
    return result

def current(location, as_df=False):
    ''' Get raw data from OWM api for current observations at the given
    location.
    
    :param location: The location around which to search for weather.
    :type location: dict-- {'lat': <(-90,90)>, 'lon': <(-180,180)>}
    :param as_df: Flags result to be returned as a dataframe; default is False.
    :type as_df: bool
    '''
    
    lat = location['lat']
    lon = location['lon']
    url = f'http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={obs_key}'
  
    result = api_handles.retry(requests.get, url).json()
    if result == -1:
        result = {'loc': location, 'time': time.ctime()}
        return result
    # Add some fields to the data to be used in analysis.
    result['location'] = location
    result['type'] = 'obs'
    if as_df:
        result = pd.json_normalize(result)
    return result
    
