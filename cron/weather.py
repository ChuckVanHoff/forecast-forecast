''' Defines the Weather class and related functions. '''

import time
import json

from benedict import benedict
from pyowm import OWM
from pyowm.weatherapi25.forecast import Forecast
from pyowm.exceptions.api_response_error import NotFoundError
from pyowm.exceptions.api_call_error import APICallTimeoutError
from pyowm.exceptions.api_call_error import APIInvalidSSLCertificateError

from config import OWM_API_key_loohoo as loohoo_key
from config import OWM_API_key_masta as masta_key
from instant import Instant

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
#         # make the timeplace for each weather according to its reference time
#         if _type == 'forecast' and 'reference_time' in data:
#             self.timeplace = f'{str(location)}{str(data["reference_time"])}'
#         elif _type == 'observation': #and 'Weather' in data:
#             self.timeplace = f'{str(location)}{str(10800 * (data["reference_time"]//10800 + 1))}'
#         self.as_dict = {'timeplace': self.timeplace,
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
            'timeplace': 'DEFAULT',
            'clouds': 'DEFAULT',
            'rain': {'1h': 0,
                    '3h': 0
                    },
            'snow': {'1h': 0,
                    '3h': 0
                    },
            'wind': {'speed': 0,
                    'deg': 0,
                     'gust': 0
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
        weather = benedict(weather)
        weather.merge(data)
        
        self.type = _type
        self.loc = location
        self.weather = weather
        # make the timeplace for each weather according to its type
        if _type == 'forecast':# and 'reference_time' in data:
            self.timeplace = f'{str(location)}{str(data["reference_time"])}'
        elif _type == 'observation':# and 'Weather' in data:
            self.timeplace = f'{str(data["location"])}{str(10800 * (data["reference_time"]//10800 + 1))}'
        else:
            self.timeplace = weather['timeplace']
        self.as_dict = {'timeplace': self.timeplace, \
                        '_type': self.type, \
                        'weather': self.weather \
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
            instants.setdefault(self.timeplace, Instant(self.timeplace, observations=self.weather))
            return
        if self.type == 'forecast':
#             print('setting something as forecast')
            instants.setdefault(self.timeplace, Instant(self.timeplace)).casts.append(self.weather)
#             instants.setdefault(self.timeplace, Instant(self.timeplace))['forecasts'].append(self.weather)
#             instants[self.timeplace]['forecasts'].append(weather)
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
            # Choose the weather type that should be returned by the function:
            # current or forecast?
            if type(location) == dict:
                if current:
                    result = owm.weather_at_coords(**location)
                    return result
                else:
                    result = owm.three_hours_forecast_at_coords(**location)
                    return result
            elif type(location) == str:
                result = owm.weather_at_zip_code(location, 'us')
#                 print(json.loads(result.to_JSON()))
#                 exit()
                return result
        except APIInvalidSSLCertificateError as e:
#             print('Error from get_data_from_weather_api() in weather.py', e)
            if type(location) == dict:
                loc = 'lat: {}, lon: {}'.format(location['lat'], \
                                                location['lon'])
                owm_loohoo = OWM(loohoo_key)
                owm = owm_loohoo
            elif type(location) == str:
                loc = location
                owm_masta = OWM(masta_key)
                owm = owm_masta
            print(f'''SSL error in get_data_from_weather_api() for {loc} on
                  attempt {tries} ...trying again''')
        except APICallTimeoutError:
            loc = location or 'lat: {}, lon: {}'.format(location['lat'], \
                                                        location['lon'])
            print(f'''Timeout error in get_data_from_weather_api() for {loc} on
            attempt {tries}... I'll wait 1 second then try again.''')
            time.sleep(1)
        tries += 1
    if tries == 4:
        print('''Tried 3 times without response; moving to next location.''')
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
        # Get the raw data from the OWM and make a weather.Weather from it.
        try:
            # The data type of the location argument will determine how to ask
            # the API for the data.
            if type(location) == dict:
                result = get_data_from_weather_api(owm, location, current=True)
            else:
                result = get_data_from_weather_api(owm, location)
            # Return a flag to indicate the result was not recieved.
            if result == -1:
                print(f'''Did not get current weather for {location}
                and reset owm.''')
                return result
            # Transform the data before returning it.
            result = json.loads(result.to_JSON())
            coordinates = result['Location']['coordinates']
            timeplace = str(coordinates) \
                + str(10800 * (result['Weather']['reference_time']//10800 + 1))
            result['Weather']['location'] = coordinates
            result['Weather']['timeplace'] = timeplace
            result['time_to_instant'] = result['Weather']['reference_time'] \
                                    - result['reception_time']            
#             weather = Weather(location, 'observation', result['Weather'])
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
        # Make an timeplace for the next Weather to be created, create the Weather,
        # append it to the casts list.
        data['timeplace'] = str(location) + str(data['reference_time'])
        data['time_to_instant'] = data['reference_time'] \
                                - forecast['reception_time']
        casts.append(Weather(location, 'forecast', data))
    return casts
