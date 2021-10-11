#from parts import strikedf
import pandas as pd
import numpy as np
import redis, json, os


def buildStrikeVolas(spot, vola, skew,calls,puts,cmax, pmax, t):
    cols = ("strikes", "volas")
    volas = []
    for strike in strikedf["Strikes"]: 
        strikeVola = volaSurface(float(strike),spot, vola, skew,calls,puts,cmax, pmax, t)
        volas.append((float(strike), strikeVola))
        strikeVolas = pd.DataFrame(columns=cols, data=volas)
    
    return strikeVolas 

def sumbitVolas(product, data):
       #connect to redis (default to localhost).
       redisLocation = os.getenv('REDIS_LOCATION', default = 'localhost')
       conn = redis.Redis(redisLocation)

       dict = json.dumps(data)
       conn.set(product+'Vola', dict)



