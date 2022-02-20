from taskqueue import TaskQueue
from pymongo import MongoClient
from multiprocessing import cpu_count

# Init task queue doing [cpu_count] tasks simultaneously (cpu_count is the number of cores)
taskqueue = TaskQueue(num_workers=cpu_count())

# Init mongodb connection
mongo = MongoClient()['BCA']

# Check mongodb connection
try:
    mongo.admin.command('ismaster')
except:
    print("\n ---- Database Server not available !")
    exit(-1)


"""
Metrics are added to the task queue here
Each metric with a different resolution is a completely different metric
So sending_addresses with a resolution of 1 day has nothing to do with the 30 days one
"""


# Metric: Sending Addresses (1 day)
from sending_addresses import sending_addresses
taskqueue.add_task(sending_addresses, mongo, 24, 'sending_addresses_1day')


"""
End of the metrics section
"""

# Start working
taskqueue.join()

print('\n ~~~ All done !!')