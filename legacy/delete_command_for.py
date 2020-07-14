def delete_command_for(data):
    ### THIS DOES NOT WORK ###
    ''' the 'delete command' is the MongoDB command that is used to update data
    should be a weather type object. it will have its filter and update set
    according to the entry content. It returns a command to update in a pymongo
    database.

    :param data: the dictionary created from the api calls
    :type data: dict
    :return: the command that will be used to find and update documents
    ''' 
    from pymongo import DeleteOne

    # catch the error if this is processing data entered by another module or
    # version of this one, but otherwise expect there to be ..... come back
    if "Weather" in data:
        try:
            filters = {'zipcode': data['Weather'].pop('zipcode'),\
                        'instant': data['Weather'].pop('instant')}
            updates = {'$set': {'weather': data['Weather']}}
        except KeyError:
            # this if for processing data in OWM.forecasted and OWM.observed.
            data['Weather']['time_to_instant'] \
                    = data['Weather'].pop('reference_time')\
                    - data['reception_time']
            filters = {'zipcode': data.pop('zipcode'),\
                        'instant': data.pop('instant')}
            updates = {'$set': {'weather': data['Weather']}}
        except KeyError:
            print('caught KeyError')
    else:
        try:
            filters = {'zipcode': data.pop('zipcode'),\
                        'instant': data.pop('instant')}
            updates = {'$push': {'forecasts': data}} # append to forecasts list
        except KeyError:
            print('caught keyerror')
    return DeleteOne(filters, updates,  upsert=True)

