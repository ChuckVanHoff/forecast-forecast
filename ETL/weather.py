''' Defines the Weather class and related functions. '''

import time
import json

from pyowm import OWM
from pyowm.weatherapi25.forecast import Forecast
from pyowm.exceptions.api_response_error import NotFoundError
from pyowm.exceptions.api_call_error import APICallTimeoutError
from pyowm.exceptions.api_call_error import APIInvalidSSLCertificateError

from config import OWM_API_key_loohoo as loohoo_key
from config import OWM_API_key_masta as masta_key
from instant import Instant
# from config import client

# from Extract.make_instants import find_data


class Weather:
    ''' A dictionary of weather variables and their observed/forecasted values
    for a given instant in time at a specified location.
    '''
    
### commented original so I could work on alterations ###
#     def __init__(self, location, _type, data=None):
#         '''
#         :param location: can be either valid US zipcode or coordinate dictionary
#         :type location: If this param is a zipcode, it should be str, otherwise
#         dict
#         :param _type: Indicates whether its data is observational or forecasted
#         :type _type: string  It must be either 'observation' or 'forecast'
#         '''

#         self.type = _type
#         self.loc = location
#         self.weather = data
#         # make the _id for each weather according to its reference time
#         if _type == 'forecast' and 'reference_time' in data:
#             self._id = f'{str(location)}{str(data["reference_time"])}'
#         elif _type == 'observation': #and 'Weather' in data:
#             self._id = f'{str(location)}{str(10800 * (data["reference_time"]//10800 + 1))}' #["Weather"]["reference_time"]//10800 + 1))}'
#         self.as_dict = {'_id': self._id,
#                        '_type': self.type,
#                         'weather': self.weather
#                        }
    
    ### COPY adding deafult weather object ###
    def __init__(self, location, _type, data={}):
        '''
        :param location: can be either valid US zipcode or coordinate dictionary
        :type location: If this param is a zipcode, it should be str, otherwise
        dict
        :param _type: Indicates whether its data is observational or forecasted
        :type _type: string  It must be either 'observation' or 'forecast'
        '''

        # Create a default weather dict and update it with data.
        weather = {
            '_id': 'DEFAULT',
            'clouds': 'DEFAULT',
            'rain': {'1h': 0,
                    '3h': 0
                    },
            'snow': {'1h': 0,
                    '3h': 0
                    },
            'wind': {'speed': 0,
                    'deg': 0
                    },
            'humidity': 'DEFAULT',
            'pressure': {'press': 'DEFAULT',
                        'sea_level': 'DEFAULT'
                        },
            'temperature': {'temp': 'DEFAULT',
                           'temp_max': 'DEFAULT',
                           'temp_min': 'DEFAULT'
                           },
            'status': 'DEFAULT',
            'detailed_status': 'DEFAULT',
            'weather_code': 'DEFAULT',
            'visibility_distance': 0,
            'dewpoint': 'DEFAULT',
            'humidex': 'DEFAULT',
            'heat_index': 'DEFAULT',
            'time_to_instant': 'DEFAULT'
        }
        weather.update(data)
        
        self.type = _type
        self.loc = location
        # Define the weather data structure
        self.weather = weather#.update(data)
        # make the _id for each weather according to its reference time
        if _type == 'forecast' and 'reference_time' in data:
            self._id = f'{str(location)}{str(data["reference_time"])}'
        elif _type == 'observation' and 'Weather' in data:
            self._id = f'{str(location)}{str(10800 * (data["reference_time"]//10800 + 1))}' #["Weather"]["reference_time"]//10800 + 1))}'
        else:
            self._id = weather['_id']    
        self.as_dict = {'_id': self._id,
                       '_type': self.type,
                        'weather': self.weather
                       }

    def to_inst(self, instants):
        ''' This will find the id'd Instant and add the Weather to it according 
        to its type.
        
        :param instants: a collection of instants
        :type instnats: dict
        
        *** NOTE: the object instants must be in the function's namespace ***
        '''

        if not instants:
            instants = {'init': 'true'}
        if self.type == 'observation':
#             print('setting something as observation')
            instants.setdefault(self._id, Instant(self._id, observations=self.weather))
            return
        if self.type == 'forecast':
#             print('setting something as forecast')
            instants.setdefault(self._id, Instant(self._id)).casts.append(self.weather)
#             instants.setdefault(self._id, Instant(self._id))['forecasts'].append(self.weather)
#             instants[self._id]['forecasts'].append(weather)
            return


def get_data_from_weather_api(owm, location, current=False):
    ''' Makes api calls for observations and forecasts and handles the API call
    errors.

    :param owm: the OWM API object
    :type owm: pyowm.OWM
    :param location: the coordinates or zipcode reference for the API call.
    :type location: if location is a zipcode, then type is a string;
    if location is a coordinates, then tuple or dict.
    :param current: This determines if the coordinate location should be used
    to get current or forecasted weather. The default is forecasted.
    :type current: bool

    returns: the API data
    '''
        
    result = None
    tries = 1
    while result is None and tries < 4:
        try:
            if type(location) == dict:
                if current:
                    result = owm.weather_at_coords(**location)
                    return result
                else:
                    result = owm.three_hours_forecast_at_coords(**location)
                    return result
            elif type(location) == str:
                result = owm.weather_at_zip_code(location, 'us')
                return result
        except APIInvalidSSLCertificateError as e:
            print(str(e))
            if type(location) == dict:
                loc = 'lat: {}, lon: {}'.format(location['lat'], location['lon'])
                owm_loohoo = OWM(loohoo_key)
                owm = owm_loohoo
            elif type(location) == str:
                loc = location
                owm_masta = OWM(masta_key)
                owm = owm_masta
            print(f'SSL error with {loc} on attempt {tries} ...trying again')
        except APICallTimeoutError:
            loc = location or 'lat: {}, lon: {}'.format(location['lat'],
                                                           location['lon'])
            print(f'''Timeout error with {loc} on attempt {tries}... waiting 1
                  second then trying again''')
            time.sleep(1)
        tries += 1
    if tries == 4:
        print('''tried 3 times without response; breaking out and causing an
        error that will crash your current colleciton process...fix that!''')
        return -1  ### sometime write something to keep track of the zip and
                ### instant that isn't collected ###

def get_current_weather(location):
    ''' Get the current weather for the given zipcode or coordinates.

    :param location: the coordinates or zipcode reference for the API call.
    :type location: if location is a zipcode, then type is a string;
    if location is a coordinates, then tuple or dict.

    :return: the raw weather object
    :type: json
    '''
    owm = OWM(loohoo_key)

    m = 0
    # Try several times to get complete the API request
    while m < 4:
        try:
            # get the raw data from the OWM and make a Weather from it
            if type(location) == dict:
                result = get_data_from_weather_api(owm, location, coords=location)
            result = get_data_from_weather_api(owm, location)
            if result == -1:
                print(f'Did not get current weather for {location} and reset owm')
                return result
            result = json.loads(result.to_JSON())  # the current weather for the given zipcode
            coordinates = result['Location'].pop('coordinates')
            result['Weather']['location'] = coordinates
            result.pop('reception_time')
            result.pop('Location')
            weather = Weather(coordinates, 'observation', result['Weather'])
            return weather
        except APICallTimeoutError:
            owm = owm_loohoo
            m += 1
    
def five_day(location):
    ''' Get each weather forecast for the corrosponding coordinates
    
    :param coords: the latitude and longitude for which that that weather is
    being forecasted
    :type coords: tuple containing the latitude and logitude for the forecast

    :return casts: the five day, every three hours, forecast for the zip code
    :type casts: list of Weather objects
    '''

    owm = OWM(masta_key)

    Forecast = get_data_from_weather_api(owm, location).get_forecast()
    forecast = json.loads(Forecast.to_JSON())
    casts = [] # This is for the weather objects created in the for loop below.
    for data in forecast['weathers']:
        # Make an _id for the next Weather to be created, create the weather, 
        # append it to the casts list.
        instant = data['reference_time']
        casts.append(Weather(location, 'forecast', data))
    return casts


# from Extract.make_instants import find_data
# # set database and collection for testing
# database = 'test'
# collection = 'instant_temp'
# # create a dict to hold the instants pulled from the database
# instants = {}
# data = find_data(client, database, collection)
# # add each doc to instants and set its key and _id to the same values
# for item in data:
#     instants[f'{item["_id"]}'] = item['_id']