from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_core_components as dcc
import dash_table as dtable
import dash_bootstrap_components as dbc
import pandas as pd
import datetime as dt
import time, json
from flask import request

from parts import topMenu, loadRedisData, loadStaticData, recTrades

#Inteval time for trades table refresh 
interval = str(1000*1)
#column options for trade table 
columns = [ 
           {"name": 'Instrument', "id": 'instrument'},
             {"name": 'Price', "id": 'price'},
             {"name": 'Quantitiy', "id": 'quanitity'},
             {"name": 'Prompt', "id": 'prompt'},
             {"name": 'Venue', "id": 'venue'}
           ]

def onLoadProduct():
    staticData = loadStaticData()
    staticData = pd.read_json(staticData)
    products = []
    for product in staticData['product']:
        products.append({'label': product, 'value': product})
    for product in staticData['underlying']:
        products.append({'label': product, 'value': product})

    return  products

def fetechstrikes(product):
    if product != None:
        strikes = []
        data = loadRedisData(product.lower())
        if data:
            data = json.loads(data)
            for strike in data["strikes"]:
                strikes.append({'label': strike, 'value': strike})
            return strikes
    else:
        return {'label': 0, 'value': 0}
  
def timeStamp():
    now = dt.datetime.now()
    now.strftime('%Y-%m-%d %H:%M:%S')
    return now

def convertTimestampToSQLDateTime(value):
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(value))

def convertToSQLDate(date):
    value = date.strftime(f)
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(value))

#trade entry form layout
def buildTradeEntry(product, strike, CoP, prompt, price, qty, counterparty, comment, submit):
    trade_entry =html.Div([
        html.Div([dcc.Dropdown(id=product, value='LAD', options =  onLoadProduct())], className = 'one columns'),
        html.Div([dcc.Dropdown(id =strike, placeholder='Strike')], className = 'one columns'),
        html.Div([dcc.Dropdown(id=CoP, options = [{'label': 'Call', 'value': 'C'}, {'label': 'Put', 'value': 'P'}] )], className = 'one columns'),
        html.Div([dcc.DatePickerSingle(id= prompt)], className = 'one columns'),
        html.Div([dcc.Input(id =price, placeholder='Enter a price..', type='text')], className = 'one columns'),
        html.Div([dcc.Input(id =qty, placeholder='qty', type='numeric', inputmode= 'numeric')], className = 'one columns'),
        html.Div([dcc.Input(id= counterparty, value = ' ', type='text')], className = 'two columns'),
        html.Div([dcc.Input(id= comment, value=' ', type='text')], className = 'two columns'),
        html.Div([html.Button('Submit', id=submit)], className = 'one columns')
        ], className = 'row')
    return trade_entry

def buildProductName(product, strike, Cop):
    if strike == None and Cop == None:
        return product
    else:
        return product+' '+str(strike)+' '+Cop

def onLoadProduct():
    staticData = loadStaticData()
    staticData = pd.read_json(staticData)
    products = []
    for product in set(staticData['name']):
        products.append({'label': product, 'value': shortName(product)})
    products.append({'label': 'All', 'value': 'all'})
    return  products

def shortName(product):
    if product == None: return 'LCU'

    if product.lower() == 'aluminium': return 'LAD'
    elif product.lower() == 'lead': return 'PBD'
    elif product.lower() == 'copper': return 'LCU'
    elif product.lower() == 'nickel': return 'LND'
    elif product.lower() == 'zinc': return 'LZH'
    else: return []

options = dbc.Col([  
         html.Button('Rec', id='rec', style={'background':'#F1C40F'})           
                    ], width = 2)

tables = dbc.Col([    dtable.DataTable(id='recTrades',
                     columns = columns,
                     data = [{}],
                     fixed_rows=[{ 'headers': True, 'data': 0 }],
                     style_data_conditional=[
                            {
                                'if': {'row_index': 'odd'},
                                'backgroundColor': 'rgb(248, 248, 248)'
                            }]   )])

layout = html.Div([
    topMenu('F2 Reconciliation'),
    dbc.Row([options]),
    dbc.Row([tables]),

    ])

def initialise_callbacks(app):
    #pulltrades use hiddien inputs to trigger update on new trade
    @app.callback(
        Output('recTrades','data'),
        [Input('rec', 'n_clicks')])
    def update_trades(click):
        
        dff = recTrades()
        dict = dff.to_dict('records')
        return dict



