### Important note:
This project is still in the development stage. Some parts work, some do not. 

# forecast-forecast
Weather collection

This is intended to create a model to predict the errors in the weather prediction models.

## Description

## How to use it

### What you will need

You will also need write access to a MongoDB database. With the database setup, it's best
to set the database name from the config.py file, which you must add yourself.
Here is an example:
from urllib.parse import quote
OWM_API_key_1 = '<your api key>'
OWM_API_key_2 = '<your other api key>'
>>> for local
host = 'localhost'
port = 27017
<<< for local
>>> for remote
socket_path = '<socket path to db>'
username = '<your username>'
password = quote('<your password>')
uri = "mongodb+srv://%s:%s@%s" % (user, password, socket_path)
<<< for remote
database = '<database name>'

To run by zipcodes:
You will need a file that is a comma separated list of valid, five-digit, US zipcodes
as strings. Ex: '70107'

To run by geo-coordinate pairs:
In ETL/get_and_make.py, modify the 'hl' list comprehension in the __name__==__main__ block.

### What you will do
Open the get_and_make.py file to find the variable 'filename'. Edit it with the path to the
list of zipcodes. If you're want to use geo-coords, just be sure that the area you intend
to collect data over and the granularity of the collected data is represented by the geohash.

Now run get_and_make.py. That's it! Now you are collecting data from the OWM api and
loading it to the local database you have setup.

----------------------------------------------------------------------------------------

There is more to come as the project continues in its development.
