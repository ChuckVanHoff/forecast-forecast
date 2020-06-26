def update_command_for(data):
    ''' the 'update command' is the MongoDB command that is used to update data
    should be a weather type object. it will have its filter and update set
    according to the entry content. It returns a command to update in a pymongo
    database.

    :param data: the data dictionary created from the api calls
    :type data: dict
    :return: the command that will be used to find and update documents
    ''' 
    
    
    if data['_type'] == 'forecast':
        filters = {'_id': data['_id']}
        updates = {'$push': {'forecasts': data}} # append to forecasts list
        return pymongo.UpdateOne(filters, updates,  upsert=True)
    elif data['_type'] == 'observation':
        filters = {'_id': data['_id']}
        updates = {'$set': {'observations': data}}
        return pymongo.UpdateOne(filters, updates,  upsert=True)

    elif "Weather" in data:
        try:
            filters = {'zipcode': data['Weather'].pop('zipcode'),
                        'instant': data['Weather'].pop('instant')}
            updates = {'$set': {'weather': data['Weather']}}
        except:
            ### for processing data in OWM.forecasted and OWM.observed.
            if "Weather" in data:
                try:
                    data['Weather']['time_to_instant'] = \
                            data['Weather'].pop('reference_time')\
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
                    updates = {'$push': {'forecasts': data}} 
                except KeyError:
                    print('caught keyerror')
    else:
        try:
            filters = {'zipcode': data.pop('zipcode'), 'instant': data.pop('instant')}
            updates = {'$push': {'forecasts': data}} # append to forecasts list
            return pymongo.UpdateOne(filters, updates,  upsert=True)
        except KeyError as e:
            if e.args == 'zipcode' or e.args == 'instant':
                print(f'It was a keyerror on {e.args}. Here is the data:', data)
            else:
                # Print the error and try to make an update command for it to
                # be added to the updates list.
                print(e)

