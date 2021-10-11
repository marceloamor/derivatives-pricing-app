from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_table as dtable
import pandas as pd
import datetime as dt
from dash import no_update
import time, json
from flask import request
import pickle

#from sql import pulltrades
from parts import loadRedisData, onLoadProduct
from data_connections import conn
from app import app, topMenu

#Inteval time for trades table refresh 
interval = 1000*3
#column options for trade table 
columns = [{"name": 'Date', "id": 'dateTime'}, 
           {"name": 'Instrument', "id": 'instrument'},
             {"name": 'Price', "id": 'price'},
             {"name": 'Quantitiy', "id": 'quanitity'},
             {"name": 'Theo', "id": 'theo'},
             {"name": 'User', "id": 'user'},
             {"name": 'Counterparty', "id": 'counterPart'},
             {"name": 'Prompt', "id": 'prompt'},
             {"name": 'Venue', "id": 'venue'}
           ]

def timeStamp():
    now = dt.datetime.now()
    now.strftime('%Y-%m-%d %H:%M:%S')
    return now

def convertTimestampToSQLDateTime(value):
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(value))

def shortName(product):
    if product == None: return 'LCU'

    if product.lower() == 'aluminium': return 'LAD'
    elif product.lower() == 'lead': return 'PBD'
    elif product.lower() == 'copper': return 'LCU'
    elif product.lower() == 'nickel': return 'LND'
    elif product.lower() == 'zinc': return 'LZH'
    else: return 'LCU'

venueOptions = [{'label': 'Select', 'value': 'Select'},
                {'label': 'All', 'value': 'all'},
                {'label': 'Internal', 'value': 'Internal'},
                {'label': 'Inter-office', 'value': 'Inter-office'},
                {'label': 'Georgia', 'value': 'Georgia'}
    ]

options = dbc.Row([
        dbc.Col([dcc.Input(
                    id='date-picker',
                    #type='Date',
                    value=dt.date.today() 
                )], width = 3),

        dbc.Col([dcc.Dropdown(id='product', value='all', options =  onLoadProduct())
                  ], width = 3),
        dbc.Col([dcc.Dropdown(id='venue', value='all', options =  venueOptions)
                  ], width =3),
   
    ])

tables =     dbc.Row([
                dbc.Col([
    dtable.DataTable(id='tradesTable1',
                     columns = columns,
                     data = [{}],
                     #fixed_rows=[{'headers': True, 'data': 0 }],
                     style_data_conditional=[
                            {
                                'if': {'row_index': 'odd'},
                                'backgroundColor': 'rgb(137, 186, 240)'
                            }])])])

layout = html.Div([
    topMenu('Trades'),
    #interval HTML
    dcc.Interval(id='trades-update', interval=interval),
    options,
    tables,
    ])

#pulltrades use hiddien inputs to trigger update on new trade
@app.callback(
    [Output('tradesTable1','data'),Output('tradesTable1','columns')],
    [Input('date-picker', 'value'),
     Input('trades-update', 'n_intervals'),
     Input('product', 'value'), Input('venue', 'value')
     ])
def update_trades(date, interval, product, venue):
    if date and product:
        data= conn.get('trades')
        dff= pickle.loads(data)
        columns=[{"name": i.capitalize(), "id": i} for i in dff.columns]
        product = shortName(product)
        dff= dff[dff['dateTime']>=date]
        print(dff)
        #filter for product
        if product != 'all':
            print(product)
            dff = dff[dff['instrument'].str.contains(product)]
        #filter for venue
        if venue != 'all':
            print(venue)
            dff = dff[dff['venue']==venue]
        
        dff.sort_index(inplace = True, ascending  = True)
        dict = dff.to_dict('records')
        return dict, columns
    else: no_update



