import redis, pickle, json 
from data_connections import conn, call_function, select_from, PostGresEngine
import pandas as pd
import numpy as np
from calculators import linearinterpol
from riskapi import runRisk
from parts import settleVolsProcess

# ApiInputs= {'portfolio': 'aluminium',
#  'vol': ['0.01', '0.02', '0.03', '0.04', '0.05', '0', '0.01', '-0.02', '-0.03', '-0.04', '-0.05'],
#  'und': ['1', '2', '3', '4', '5', '0', '-1', '-2', '-3', '-4', '-5'],
#  'level': 'high',
#  'eval': '20/12/2021', 
#  'rel': 'abs'}
 
# risk = runRisk(ApiInputs)

# data = conn.get('positions')
# data = pickle.loads(data)
# df = pd.DataFrame(data)
# df[['instrument', 'quanitity']].to_csv()

