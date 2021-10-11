from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_core_components as dcc
import dash_table as dtable
import dash_bootstrap_components as dbc
import pandas as pd
import datetime as dt
import json

from parts import loadStaticData, retriveParams, volCalc, onLoadPortfolio
from data_connections import conn
from app import app, topMenu

interval = str(1000)

columns = [{"name": ['','Product Code'], "id": 'Product Code'}, 
             {"name": ['-10Delta','Strike'], "id": '-10Delta Strike'},
             {"name": ['-10Delta', 'Vola'], "id": '-10Delta'},
             {"name": ['-25Delta','Strike'], "id": '-25Delta Strike'},           
             {"name": ['-25Delta', 'Vola'], "id": '-25Delta'},
             {"name": ['50Delta','Strike'], "id": '50Delta Strike'},
             {"name": ['50Delta','Vola'], "id": '50Delta'},
             {"name": ['+25Delta', 'Strike'], "id": '+25Delta Strike'},
             {"name": ['+25Delta', 'Vola'], "id": '+25Delta'},
             {"name":  ['+10Delta', 'Strike'], "id":  '+10Delta Strike'},
             {"name":  ['+10Delta', 'Vola'], "id":  '+10Delta'}
           ]

table = dbc.Row(
    [
        dbc.Col([
    dtable.DataTable(id='volasTable', columns = columns, data = [{}],
                      merge_duplicate_headers=True,
                                     style_data_conditional=[
                            {
                                'if': {'row_index': 'odd'},
                                'backgroundColor': 'rgb(248, 248, 248)'
                            }]
    )
])
    ])

options = dbc.Row([
                        dbc.Col([
                            dcc.Dropdown(id='portfolio-selector-volas', value='copper',options =  onLoadPortfolio())
                                ], width = 2),
                        dbc.Col([
                            dcc.Dropdown(id='diff', value='diff', options =  [
                                {'label': 'Diff', 'value': 'diff'},
                                {'label': 'Volas', 'value': 'volas'}
                                ] )
                                ], width = 2)
                        ])


layout = html.Div([
    topMenu('Vola by delta'),
    options,
    table
    ])

@app.callback(
    Output('volasTable','data'),
    [Input('portfolio-selector-volas', 'value'),
     Input('diff', 'value')
     ])
def productVolas(portfolio, diff):
    
    #pull staticdata for 
    staticData = loadStaticData()

    #filter for current portfolio
    staticData = staticData.loc[staticData['portfolio']==portfolio.lower()]

    #list products 
    products = staticData['product'].values

    #assign multiplier level depending on diff
    if diff == 'diff':
        multiplier = 1
    else : 
        multiplier = 0

    portfolioVolas = []
    #go collect params and turn into delta space volas
    for product in products:
        params = retriveParams(product.lower())

        #current undlying
        data = conn.get(product.lower())

        greeks = json.loads(data)
        und = greeks['calc_und']

        #find expiry to find days to expiry
        expiry =  dt.datetime.strptime(greeks['m_expiry'], '%d/%m/%Y')
        now = dt.datetime.now()
        #t = ((expiry-now).days)/365
        #t = t**0.5

        #calc the atm vol for relative using multiplier to turn on/off
        atm = params['vol']*100*multiplier
        
        #build product voals per strike
        volas = {
           'Product Code': product,
           '-10Delta Strike' :round(((params['vol'])*-1.28155*und)+und,0),
           '-10Delta' :round(volCalc(1.28155, params['vol'], params['skew'], params['call'], params['put'], params['cmax'], params['pmax'])-atm,2),
           '-25Delta Strike' :round(((params['vol'])*-0.67449*und)+und,0),
           '-25Delta':round(volCalc(0.67449, params['vol'], params['skew'], params['call'], params['put'], params['cmax'], params['pmax'])-atm,2),
           '50Delta':round(params['vol']*100,2),
           '50Delta Strike':round(und,0),
           '+25Delta Strike' :round(((params['vol'])*+0.67449*und)+und,0),
           '+25Delta':round(volCalc(-0.67449, params['vol'], params['skew'], params['call'], params['put'], params['cmax'], params['pmax'])-atm,2),
           '+10Delta Strike' :round(((params['vol'])*+1.28155*und)+und,0),
           '+10Delta':round(volCalc(-1.28, params['vol'], params['skew'], params['call'], params['put'], params['cmax'], params['pmax'])-atm,2)
            }
        #append to bucket
        portfolioVolas.append(volas)

    return portfolioVolas
