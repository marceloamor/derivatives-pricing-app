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

rates = pickle.loads(conn.get('copperCurve'))
print(rates)