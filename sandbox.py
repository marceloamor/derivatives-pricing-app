from ast import Str
from pandas.core.frame import DataFrame
import redis, pickle, json 
from data_connections import conn, call_function, select_from, PostGresEngine
import pandas as pd
import numpy as np
from calculators import linearinterpol
from datetime import date
from riskapi import runRisk
from parts import settleVolsProcess, pullPrompts

#update trades in redis
trades = pd.read_sql('trades', PostGresEngine())

trades.columns = trades.columns.str.lower()
pick_trades = pickle.dumps(trades, protocol=-1)
conn.set('trades', pick_trades)

#pull trades on data
data= conn.get('trades')

if data:
    dff= pickle.loads(data)
    print(dff)
    dff.columns = dff.columns.str.lower()
    #dff.deleted = dff.deleted.astype(int)
    #print(dff[dff['ID']==297])
    dff.deleted = dff.deleted.astype(bool)
    #print(dff['deleted'].unique())
    print(dff[dff['id']==310])
    print(dff['deleted'].unique())



