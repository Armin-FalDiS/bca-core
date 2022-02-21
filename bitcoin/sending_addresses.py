from datetime import datetime

def sending_addresses(client, resolution, collection):
    """Counts unique sending addresses for each timeframe based on resolution and saves the results in the corresponding collection

    Args:
        client (MongoClient): The mongoDB client connection set on db
        resolution (int): Resolution in hours
        collection (string): Name of the collection to save to results in
    """
    # Start time default value is the first bitcoin transaction timestamp (unix) 2009/Jan/03 00:00
    startTime = 1230940800
    # Find last record and fetch it's timestamp
    lastId = list(client[collection].find().sort('_id', -1).limit(1))[0]['_id']
    # If it exists
    if lastId:
        # Pick up from where we left
        startTime = lastId
    # End time is right now (unix)
    endTime = int(datetime.utcnow().timestamp())
    # Turn resolution into seconds
    resolution *= 60 * 60

    # Iterate through timeframes
    for i in range(startTime, endTime, resolution):
        # Count sending addresses
        result = client['btc_transactions'].aggregate([
            {
                '$match': {
                    'fee': {
                        '$gt': 0
                    },
                    'time': {
                        '$gte': i,
                        '$lt': i + resolution
                    }
                }
            }, {
                '$unwind': {
                    'path': '$inputs'
                }
            }, {
                '$group': {
                    '_id': None,
                    'inputs': {
                        '$addToSet': '$inputs.address'
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'vin': {
                        '$size': '$inputs'
                    }
                }
            }
        ])['vin']

        # Insert calculated datapoint into the collection
        client[collection].insert_one({
            '_id': i,
            'value': result
        })