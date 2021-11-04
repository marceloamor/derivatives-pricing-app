import pandas as pd
from pysimplesoap.client import SoapClient
import pickle, math, os, time
from time import sleep
from TradeClass import TradeClass, VolSurface
import ujson as json
import numpy as np
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from sql import sendTrade, deleteTrades, sendPosition, updatePos, updateRedisDelta, updateRedisPos, updateRedisTrade, updateRedisCurve, pulltrades, pullPosition
from data_connections import Connection, Cursor, conn

sdLocation = os.getenv('SD_LOCAITON', default = 'staticdata')
positionLocation = os.getenv('POS_LOCAITON', default = 'greekpositions')

def loadStaticData():
    #pull staticdata from redis
    i=0
    while i<5:
        try:
            staticData = conn.get(sdLocation) 
            break           
        except Exception as e:
            time.sleep(1)
            i = i+1    

    staticData = pd.read_json(staticData)
    #filter for non expired months
    today = datetime.now()+ timedelta(days=1)
    today = today.strftime('%Y-%m-%d')     
    staticData = staticData[pd.to_datetime(staticData['expiry'],format='%d/%m/%Y').dt.strftime('%Y-%m-%d') > today]

    return staticData

#static = loadStaticData()
data= conn.get('staticdata')
staticData = pd.read_json(data)
print(loadStaticData())