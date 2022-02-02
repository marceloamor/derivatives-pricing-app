from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_table as dtable
import datetime as dt
from dash import no_update
from dash.exceptions import PreventUpdate
import dash_daq as daq
import time, pickle, json
import pandas as pd

#from sql import pulltrades
from parts import topMenu, onLoadPortFolioAll
from data_connections import conn
from sql import delete_trade

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
             {"name": 'Venue', "id": 'venue'},
             {"name": 'Deleted', "id": 'deleted'}
           ]

def timeStamp():
    now = dt.datetime.now()
    now.strftime('%Y-%m-%d %H:%M:%S')
    return now

def convertTimestampToSQLDateTime(value):
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(value))

def shortName(product):
    if product == None: return 'all'

    if product.lower() == 'aluminium': return 'LAD'
    elif product.lower() == 'lead': return 'PBD'
    elif product.lower() == 'copper': return 'LCU'
    elif product.lower() == 'nickel': return 'LND'
    elif product.lower() == 'zinc': return 'LZH'
    else: return 'all'

venueOptions = [{'label': 'Select', 'value': 'Select'},
                {'label': 'All', 'value': 'all'},
                {'label': 'Internal', 'value': 'Internal'},
                {'label': 'Inter-office', 'value': 'Inter-office'},
                {'label': 'Georgia', 'value': 'Georgia'},
                {'label': 'CQG', 'value': 'CQG'},
       ]

options = dbc.Row([
        dbc.Col([dcc.Input(
                    id='date-picker',
                    #type='Date',
                    value=dt.date.today() 
                )], width = 3),

        dbc.Col([dcc.Dropdown(id='product', value='all', options =  onLoadPortFolioAll())
                  ], width = 3),
        dbc.Col([dcc.Dropdown(id='venue', value='all', options =  venueOptions)
                  ], width =2),
        dbc.Col(['Deleted'], width =2),
        dbc.Col([daq.BooleanSwitch(id='deleted', on=False)], width =2)   
    ])

tables =     dbc.Row([
                dbc.Col([
    dtable.DataTable(id='tradesTable1',
                     columns = columns,
                     data = [{}],
                     row_deletable=True,
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
    html.Div(id='output')
    ])

def initialise_callbacks(app):
    #pulltrades use hiddien inputs to trigger update on new trade
    @app.callback(
        [Output('tradesTable1','data'),Output('tradesTable1','columns'), Output('tradesTable1','row_deletable')],
        [Input('date-picker', 'value'),
        Input('trades-update', 'n_intervals'),
        Input('product', 'value'), Input('venue', 'value'), Input('deleted', 'on')
        ])
    def update_trades(date, interval, product, venue, deleted):
        if len(date)==10 and product:
            #convert date into datetime
            date = dt.datetime.strptime(date, '%Y-%m-%d')

            #pull trades on data
            data= conn.get('trades')
            
            if data:
                dff= pickle.loads(data)
                
                #convert columsn to lower case
                dff.columns = dff.columns.str.lower()                         

                dff.deleted = dff.deleted.astype(bool)
     
                #filter for date and deleted
                dff= dff[dff['datetime']>=date]              
                dff= dff[dff['deleted']==bool(deleted)]    

                #create columns for end table
                columns=[{"name": i.capitalize(), "id": i} for i in dff.columns]
                
                product = shortName(product)
                #filter for product
                if product != 'all':
                    dff = dff[dff['instrument'].str.contains(product)]

                #filter for venue
                if venue != 'all':
                    dff = dff[dff['venue']==venue]
                
                dff.sort_index(inplace = True, ascending  = True)
                dict = dff.to_dict('records')

                if deleted: delete_rows= False
                else: delete_rows= True

                return dict, columns, delete_rows
            else:   no_update, no_update, no_update  
        else: no_update, no_update, no_update

    @app.callback(Output('trades-update', 'n_intervals'),
                [Input('tradesTable1', 'data_previous')],
                [State('tradesTable1', 'data')])
    def show_removed_rows(previous, current):
        if previous is None:
            PreventUpdate()
        else:
            diff = [row for row in previous if row not in current]
            id = diff[0]['id']
            delete_trade(id)

            return 1
            #return [f'Just removed {row}' for row in previous if row not in current]
