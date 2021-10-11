from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from dash import no_update
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_table as dtable
import pandas as pd
import  json
from flask import request
import numpy as np

from sql import  histroicParams
from parts import loadRedisData, buildTableData, loadStaticData, buildParamMatrix, sumbitVolas, buildTableData, onLoadProduct

from app import app, topMenu

#Inteval time for trades table refresh 
interval = str(1000*1)
#column options for trade table 
columns = [
    {"name": 'product', "id": 'product', 'editable': False}, 
           {"name": 'vol', "id": 'vol', 'editable': True},
             {"name": 'skew', "id": 'skew', 'editable': True},
             {"name": 'call', "id": 'call', 'editable': True},
             {"name": 'put', "id": 'put', 'editable': True},
             {"name": 'cmax', "id": 'cmax', 'editable': True},
             {"name": 'pmax', "id": 'pmax', 'editable': True},
             {"name": 'ref', "id": 'ref', 'editable': True},     ]

def draw_param_graphTraces(results, param):
    
    #sort data on date and adjust current dataframe
    results.sort_values(by=['strike'], inplace=True)
    
    strikes = results['strike']
    params = np.array(results[param])
    #vp = np.array(results['Vp'])
    data = [{'x': strikes, 'y': params, 'type': 'line', 'name': 'Vola'}
            # {'x': strikes, 'y': settleVolas, 'type': 'line', 'connectgaps': True, 'name': 'Settlement Vola' },
            ]
    return {'data':data}

def shortName(product):
    if product == None: return 'LCU'

    if product.lower() == 'aluminium': return 'LAD'
    elif product.lower() == 'lead': return 'PBD'
    elif product.lower() == 'copper': return 'LCU'
    elif product.lower() == 'nickel': return 'LND'
    elif product.lower() == 'zinc': return 'LZH'
    else: return []

graphs = html.Div([
        dcc.Loading( type="circle", children =[
                  html.Div([dcc.Graph(id= 'Vol_surface')])], 
                    className = 'rows'),

                  html.Div([
        dcc.Loading( type="circle", children =[
                   html.Div([dcc.Graph(id= 'volGraph')] )],
                    className = 'six columns'),
        dcc.Loading( type="circle", children =[
                   html.Div([dcc.Graph(id= 'skewGraph')] )],
                    className = 'six columns')
                                           ], className = 'row'),
                  html.Div([
        dcc.Loading( type="circle", children =[
                   html.Div([dcc.Graph(id= 'callGraph')] )],
                    className = 'six columns'),
        dcc.Loading( type="circle", children =[
                   html.Div([dcc.Graph(id= 'putGraph')] )],
                    className = 'six columns')
                                           ], className = 'row'),
                  ])

hidden = html.Div([
        html.Div(id='volhidden-div', style={'display':'none'}),
        dcc.Store(id= 'volIntermediate-value'),
        dcc.Store(id= 'volGreeks')
    ], className = 'row')

options =  dbc.Row([
    dbc.Col([dcc.Dropdown(id='volProduct', value='Copper', options =  onLoadProduct())
             ], width = 3)     
             ])   

layout = html.Div([
    topMenu('Vola Matrix'),
    options,
    dtable.DataTable(id='volsTable', columns = columns, editable=True, data = [{}],
                     
                                    style_data_conditional=[
                            {
                                'if': {'row_index': 'odd'},
                                'backgroundColor': 'rgb(248, 248, 248)'
                            }]),
    html.Button('Submit Vols', id='submitVol'),
    hidden,
    graphs
    ])

#pulltrades use hiddien inputs to trigger update on new trade
@app.callback(
    Output('volsTable','data'),
    [Input('volProduct', 'value')
     ])
def update_trades(portfolio):
    #pull matrix inputs
    dff = buildParamMatrix(portfolio.capitalize())
    #create product column
    dff['product'] = dff.index
    dff['prompt'] =pd.to_datetime(dff['prompt'], format='%d/%m/%Y')
    dff = dff.sort_values(['prompt'], na_position = 'first')

    #convert call/put max into difference
    dff['cmax'] = dff['cmax'] - dff['vol']
    dff['pmax'] = dff['pmax'] - dff['vol']

    #mult them all by 100 for display
    dff.loc[:,'vol'] *= 100
    dff.loc[:,'skew'] *= 100
    dff.loc[:,'call'] *= 100
    dff.loc[:,'put'] *= 100
    dff.loc[:,'cmax'] *= 100
    dff.loc[:,'pmax'] *= 100

    cols = ["vol", "skew", "call", "put", "cmax", "pmax"] 

    dff[cols] = dff[cols].round(2)

    dict = dff.to_dict('records')
    
    return dict

#loop over table and send all vols to redis
@app.callback(
    Output('volhidden-div','children'),
    [Input('submitVol', 'n_clicks')],
    [State('volsTable','data')])
def update_trades(clicks, data):
     if clicks != None:  
        
        for row in data:
            product = row['product']
            cleaned_df = {'spread':float(row['spread']),'vola':float(row['vol'])/100, 'skew':float(row['skew'])/100, 'calls':float(row['call'])/100, 'puts': float(row['put'])/100, 'cmax':(float(row['cmax'])+ float(row['vol']))/100, 'pmax':(float(row['pmax'])+ float(row['vol']))/100, 'ref': float(row['ref']) }
    
            sumbitVolas(product.lower(),cleaned_df )

#Load greeks for active cell
@app.callback(Output('Vol_surface', 'figure'),
             [Input('volsTable', 'active_cell')],
              [State('volsTable', 'data')]
    )
def updateData(cell, data):
    product = data[cell['row']]['product']
    if product:
        data = loadRedisData(product.lower())
        if data != None:
            data = json.loads(data)
            dff = pd.DataFrame.from_dict(data, orient='index')
            #data = buildTableData(data)
            
            if len(dff) > 0:
                figure = draw_param_graphTraces(dff, 'vol')
                return figure
        
        else: 
            figure = {'data': (0,0)}
            return figure

##update graphs on data update
@app.callback(
    [Output('volGraph', 'figure'),
     Output('skewGraph', 'figure'),
     Output('callGraph', 'figure'),
     Output('putGraph', 'figure')
     ],
             [Input('volsTable', 'active_cell')],
              [State('volsTable', 'data')]
)
def load_param_graph(cell, data):
    if cell == None:
        print(cell)
        return no_update, no_update, no_update, no_update
    else:
        if data[0] and cell:
            product = data[cell['row']]['product']
            if product:
                df = histroicParams(product)
                dates = df['saveddate'].values
                volFig = {'data':[{'x': dates, 'y': df['atm_vol'].values*100, 'type': 'line', 'name': 'Vola'}]}
                skewFig = {'data':[{'x': dates, 'y': df['skew'].values*100, 'type': 'line', 'name': 'Skew'}]}
                callFig = {'data':[{'x': dates, 'y': df['calls'].values*100, 'type': 'line', 'name': 'Call'}]}
                putFig = {'data':[{'x': dates, 'y': df['puts'].values*100, 'type': 'line', 'name': 'Put'}]}

                return volFig, skewFig, callFig, putFig
        else:
            return no_update, no_update, no_update, no_update



