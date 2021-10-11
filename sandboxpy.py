## test to pull last modified date from F2 file store it in Redis and then compare to a change 

import os, time, redis, pickle

#connect to local redis server
conn = redis.Redis('localhost')

rates = pickle.loads(conn.get(product.lower()+ 'Curve'))

print(rates)