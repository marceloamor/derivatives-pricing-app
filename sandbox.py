import redis, pickle, json 
from data_connections import conn, call_function, select_from, PostGresEngine
from datetime import date
from apps.homepage import pullPortfolioGreeks 
import pandas as pd

portfolio= 'aluminium'

dff = conn.get('greekpositions')
dff=pd.read_json(dff)

if not dff.empty:        
    dff.sort_values('expiry', inplace=True)  
    dff = dff[dff['portfolio']==portfolio].groupby('product').sum().round(3).reset_index()
    dff.loc['Total']= dff.sum(numeric_only=True, axis=0)
    dff.loc['Total','product'] = 'Total'
    print(dff)

