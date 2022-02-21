from datetime import datetime

def sending_addresses(resolution, transactions, metric):
    """Counts unique sending addresses for each timeframe based on resolution and saves the results in the corresponding collection

    Args:
        resolution (int): Resolution in hours
        transactions (MongoClient): The mongoDB client set on BTC transactions collection
        metric (MongoClient): The mongoDB client set on the collection keeping this metric's datapoints
    """
    # Start time default value is the first bitcoin transaction timestamp (unix) 2009/Jan/03 00:00
    startTime = 1230940800
    # Find last record and fetch it's timestamp
    lastId = list(metric.find().sort('_id', -1).limit(1))[0]['_id']
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
        result = transactions.aggregate([
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
        metric.insert_one({
            '_id': i,
            'value': result
        })