from pandas.core.frame import DataFrame
import redis, pickle, json 
from data_connections import conn, call_function, select_from, PostGresEngine
import pandas as pd
import numpy as np
from calculators import linearinterpol
from datetime import date
from riskapi import runRisk
from parts import settleVolsProcess, pullPrompts

portfolio = 'copper'
#pull prompt curve
rates = pullPrompts(portfolio)

data = conn.get('greekpositions')
df = pd.read_json(data)
df['third_wed'] = df.apply(lambda row:  date.fromtimestamp(row['third_wed']/1e3), axis=1)
df.loc[~df.index.str[-1:].isin(['c', 'p']), "third_wed"] = df.loc[~df.index.str[-1:].isin(['c', 'p'])].index.str[4:]
df['third_wed']= df['third_wed'].to_string.strip('-')

print(df[['third_wed','total_fullDelta', 'total_delta', 'quanitity']])


