#dash libs
import numpy as np
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from datetime import date
import dash_table as dtable
import plotly.graph_objs as go
from dash import no_update
import ujson as json
import pandas as pd

#vola libs
from parts import loadRedisData, retriveParams, loadStaticData, ringTime, onLoadProductMonths, sumbitVolas
from app import app, topMenu

interval = 5000

def onLoadProductProducts():
    staticData = loadStaticData()
    products = []
    staticData['product'] = [x[:3] for x in staticData['product']]
    productNames = staticData['product'].unique()
    staticData.sort_values('product')
    for product in productNames:
        products.append({'label': product, 'value': product})
    return  products, products[0]['value']

def fetechstrikes(product): 
    if product != None:
        strikes = []
        data = loadRedisData(product.lower())
        #data = json.loads(data)
        for strike in data["strikes"]:
            strikes.append({'label': strike, 'value': strike})
        return strikes
    else:
        return {'label': 0, 'value': 0}

def draw_param_graphTraces(results, param):
    
    #sort data on date and adjust current dataframe
    #results.sort_values(by=['strikes'], inplace=True)
    
    #strikes = results['strikes']
    #params = np.array(results[param])
    #setleVolas = np.array(results['SettleVol'])
    #vp = np.array(results['Vp'])
    #data = [{'x': strikes, 'y': params, 'type': 'line'}]
    #return {'data':data}
    return         {
            'data': [
                {'x': [1, 2, 3], 'y': [4, 1, 2], 'type': 'bar', 'name': 'SF'},
                {'x': [1, 2, 3], 'y': [2, 4, 5], 'type': 'bar', 'name': u'Montréal'},
            ],
            'layout': { }
        }

def draw_param_graphTracesold(results, param):
    #sort data on date and adjust current dataframe
    results.sort_values(by=['strikes'], inplace=True)
    
    strikes = results['strikes']
    
    params = np.array(results[param])
    setleVolas = np.array(results['SettleVol'])
    vp = np.array(results['Vp'])
    #build scatter graph pd.to_datetime([dates)
    traces = list()
    #atach current volas
    traces.append(go.Scatter(
            x=strikes,
            y=params,
            name='Volas',
            mode= 'lines',
            hoveron='points',
            line = dict(
            color = 'blue',
            width = 2,),
                )  )  
    #attach settlement volas
    traces.append(go.Scatter(
            x=strikes,
            y=setleVolas,
            name='Settlement',
            mode= 'lines',
            hoveron='points',
            connectgaps = True,
            line = dict(
            color = 'green',
            width = 2,),
                )  ) 
    #attach vp
    traces.append(go.Scatter(
            x=strikes,
            y=vp,
            name='VolPath',
            mode= 'lines',
            hoveron='points',
            line = dict(
              color = 'red',
             width = 2,),
                )  ) 

    layout = go.Layout(
                title= str(param),
                showlegend=True,
                xaxis={ 'range': [min(strikes),  max(strikes)],
                                      'fixedrange': True,
                                      'title': 'Strike'
                                      },
                yaxis={ 'range': [min(params)-(0.10*min(params)),  max(params)+(0.10*max(params))],
                                      'fixedrange': True,
                                      'title': 'Vol'
                                      }
                )
    return {'data': traces, 'layout': layout}

def draw_price_graphTraces(tickdata):
    #sort data on date and adjust current dataframe
    tickdata.sort_values(by=['TimeStamp'], inplace=True)
    
    time = tickdata['TimeStamp']
    price = np.array(tickdata['Price'])
    #build scatter graph pd.to_datetime([dates)
    data = []
    layout = []
    data.append(go.Scatter(
            x=time,
            y=price,
            name='Price',
            mode= 'lines',
            ))

    layout.append(go.Layout(
                             xaxis={ 'range': [min(time),  max(time)],
                                     'title': 'TimeStamp'
                                      },
                             yaxis={ 'range': [min(price),  max(price)],
                                      'fixedrange': True,
                                      'title': 'Price'
                                      }
                                              ))
    #figure = go.Figure(data = data, layout = layout)
    #return figure
    return {'data': data, 'layout':layout}
   
def paramUpdateCheck(new, old, mult):
    mult = 1/mult
    if new == None or not new:
        if old == None or old == ' ' :
            return 0
        else:
            old = str(old)
            return float(old)*mult
    else:
        return float(new)*mult   

productDropdown = dbc.Col([
        dbc.Row([
            dbc.Col([dcc.Dropdown(id='product-selector', value =onLoadProductProducts()[1],  options =  onLoadProductProducts()[0])])
            ]),
        
        dbc.Row([
         dbc.Col([   
            dcc.Dropdown(id='month-selector')])
        ]), 

        dbc.Row([
        dbc.Col([            
            html.Div(id= 'product')])])
            ],width = 2)

infoCurrentPrice = dbc.Col([dbc.Row([
            dbc.Col(html.Div(id= 'name'),width = 1),
            dbc.Col(['Basis:'],width = 1),
            dbc.Col(html.Div(id = '3m'),width = 2),
            dbc.Col(['Future:'],width = 1),
            dbc.Col(html.Div(id = 'forward'),width = 2),
            dbc.Col(['Change:'],width = 1),
            dbc.Col(html.Div(id = 'change'),width = 1),
            dbc.Col(['Expiry:'],width = 1),
            dbc.Col(html.Div(id = 'expiry'),width = 2)           
                    ])], width = 10)

columns = [  {"name": 'Call Theo', "id": 'calc_price_call'},
             {"name": 'Call Delta', "id": 'delta_call'},
             {"name": 'Strike', "id": 'strike'},
             {"name": 'Put Theo', "id": 'calc_price_put'},
             {"name": 'Vega', "id": 'vega'},
             {"name": 'Gamma', "id": 'gamma'},
             {"name": 'Theta', "id": 'theta'},
             {"name": 'Volas', "id": 'vol'},
             {"name": 'Settle Vola', "id": 'SettleVol'},
             {"name": 'Skew', "id": 'skew'},
             {"name": 'Call', "id": 'call'},
             {"name": 'Put', "id": 'put'},
             {"name": 'Full Delta', "id": 'fullDelta_call'}]

table = dbc.Row([  
    
    dbc.Col([
            dtable.DataTable(id='datatable',
                             data = [{}],
                             columns=columns,
                             fixed_rows={'headers': True},
                             virtualization=True,
                             style_data_conditional=[
                            {
                                'if': {'row_index': 'odd'},
                                'backgroundColor': 'rgb(248, 248, 248)'
                            },
                            {'if': {'column_id': 'strike'}, 'background-color': 'rgb(171, 190, 249)'},
                                    {
                                    'if': {
                                        'filter_query': '{delta_call} > 0.48 && {delta_call} < 0.52',
                                        'column_id': 'delta_call'
                                    },
                                    'backgroundColor': 'rgb(171, 190, 249)',                                    
                                }
                            ])
                
                ])
                ])

paramInputs = html.Div([
    dbc.Row([
        dbc.Col(['Spread'], width = 1),
        dbc.Col(['ATM Vol'], width = 1),
        dbc.Col( ['Skew'], width = 1),
        dbc.Col( ['Calls'], width = 1),
        dbc.Col( ['Puts'], width = 1),
        dbc.Col( ['Call Max'], width = 1),
        dbc.Col( ['Put Max'], width = 1),
        dbc.Col([ 'Ref'], width = 1),
        dbc.Col( ['Vola'], width = 1)
        ]),
    dbc.Row([
        dbc.Col([html.Div(id='cspread')],  width = 1),
        dbc.Col([html.Div(id='cvol')]  ,  width = 1),
        dbc.Col( [html.Div(id='cskew')]  ,  width = 1),
        dbc.Col([ html.Div(id='ccalls')] ,  width = 1),
        dbc.Col([html.Div( id='cputs')] ,  width = 1),
        dbc.Col([html.Div( id='ccmax')]  ,  width = 1),
        dbc.Col([html.Div( id='cpmax')]  ,  width = 1),
        dbc.Col([html.Div( id='cref')] ,  width = 1),
        dbc.Col([html.Div( id='cvola')]  ,  width = 1)
        ]),
    dbc.Row([
       dbc.Col([dcc.Input(id='spread',  type='text')], width= 1),
        dbc.Col([ dcc.Input(id='vola',  type='text')], width= 1),
        dbc.Col([dcc.Input(id='skew',  type='text')], width= 1),
        dbc.Col([dcc.Input(id='calls',  type='text')], width= 1),
        dbc.Col([dcc.Input(id='puts',  type='text')], width= 1),
        dbc.Col([dcc.Input(id='cmax',  type='text')], width= 1),
        dbc.Col([dcc.Input(id='pmax',  type='text')], width= 1),
        dbc.Col([dcc.Input(id='ref',  type='text')], width= 1)        
        ]),
    dbc.Row([
        dbc.Col([
            html.Button('Submit', id='button')
            ], width = 2),
         dbc.Col([
             #dropdown to select if greeks are combinded or not. 
            dcc.Dropdown(id='combine', value='single',
                       options = [{'label':'Single', 'value':'single'},
                                  {'label':'Combined', 'value':'combined'},
                                  {'label':'Bucket', 'value':'bucket'}
                                 ]),
                                   
            ],width = 2),
            ])
    ])      

stores = dbc.Row([
        # dcc.Store(id="hidden-div"),
    dcc.Store(id="hidden-div1"),
    dcc.Store(id="greeks"),

    ])

layout = html.Div([
    topMenu('LME Volaility Surface'),
    stores,
    dcc.Interval(id='live-update', interval=interval),
    dbc.Row([
            productDropdown,
            infoCurrentPrice
                ]),
    #graphs,
    paramInputs,
    #strike table and vola
    table,
    #data stores

    ])

#update months options on product change
@app.callback(Output('month-selector', 'options'),
              [Input('product-selector', 'value')])
def updateOptions(product):
    if product:
        return onLoadProductMonths(product)[0]

#update months value on product change
@app.callback(Output('month-selector', 'value'),
              [Input('month-selector', 'options')])
def updatevalue(options):
    if options:

        return options[0]['value']

#update product name
@app.callback(Output('product', 'children'),
              [Input('product-selector', 'value'),
               Input('month-selector', 'value')
               ])
def updatevalue(product, month):
    if product and month:
     return product +'O'+month
   
#populate table
@app.callback(
    [Output('datatable', 'data'),
     Output('expiry', 'children'),
     Output('name', 'children'),
     Output('forward', 'children'),
     Output('3m', 'children'),
    Output('cspread', 'children') ],
    [Input('greeks', 'data'),Input('combine', 'value') ]
)
def load_table(intermediate_data, combine):
    if intermediate_data != None and type(intermediate_data) != int:
        dff = pd.DataFrame.from_dict(intermediate_data, orient='index')
        
        if 'calc_price' in dff.columns:
            #load details for top menu
            expiry = dff.iloc[0]['expiry']
            dff['expiry'] = date.fromtimestamp(expiry/ 1e3)
            third_wed = dff.iloc[0]['third_wed']
            dff['third_wed'] = date.fromtimestamp(third_wed/ 1e3)
            
            #calculate columns
            dff.drop(['volModel', 'option'], axis=1, inplace=True, errors='ignore')
                
            combinded = dff.loc[dff.cop=='c'][['strike','instrument','delta', 'calc_price', 'fullDelta']].merge(dff.loc[dff.cop=='p'], how='left', on='strike', suffixes=('_call', '_put'))
            print(dff.loc[dff.cop=='p'])
            combinded.sort_index(inplace = True)

            bucketSize = 10/100
            #decide which type of table to show
            if combine == 'single':
                return [combinded.round(3).to_dict('records'),
                combinded.iloc[0]['expiry'],
                combinded.iloc[0]['product'],
                combinded.iloc[0]['und_calc_price'],
                combinded.iloc[0]['und_calc_price'] - combinded.iloc[0]['spread'],
                combinded.iloc[0]['spread']]
            elif combine == 'combined':
                #calc combinded columns
                dff['Vega'] = (dff['Cpos']+dff['Ppos'])*dff['Vega']
                dff['Skew'] = (dff['Cpos']+dff['Ppos'])*dff['Skew']
                dff['Call'] = (dff['Cpos']+dff['Ppos'])*dff['Call']
                dff['Put'] = (dff['Cpos']+dff['Ppos'])*dff['Put']
                dff['Theta'] = (dff['Cpos']+dff['Ppos'])*dff['Theta']
                dff['Gamma'] = (dff['Cpos']+dff['Ppos'])*dff['Gamma']
                return[combinded.round(3).to_dict('records'),
                combinded.iloc[0]['expiry'],
                combinded.iloc[0]['product'],
                combinded.iloc[0]['und_calc_price'],
                combinded.iloc[0]['und_calc_price'] - combinded.iloc[0]['spread'],
                combinded.iloc[0]['spread'] ] 
            elif combine == 'bucket':
                #calc combinded columns
                dff['Vega'] = (dff['Cpos']+dff['Ppos'])*dff['Vega']
                dff['Skew'] = (dff['Cpos']+dff['Ppos'])*dff['Skew']
                dff['Call'] = (dff['Cpos']+dff['Ppos'])*dff['Call']
                dff['Put'] = (dff['Cpos']+dff['Ppos'])*dff['Put']
                dff['Theta'] = (dff['Cpos']+dff['Ppos'])*dff['Theta']
                dff['Gamma'] = (dff['Cpos']+dff['Ppos'])*dff['Gamma']
                bucketRange = np.arange(0, 1.0+bucketSize, bucketSize)
                #group by binned columns and summed selected columns
                dff  = dff.groupby(pd.cut(dff["Cdelta"], bucketRange, labels=bucketRange[1:]))[['Ppos', 'Cpos', 'Vega', 'Gamma', 'Theta', 'Skew', 'Call', 'Put']].sum().reset_index()
                dff.sort_values('Cdelta', ascending = False)
                dff.round({'Cdelta':2})
                
                return dff.to_dict('records'), dff.columns
            else:
                [{}], no_update, no_update, no_update,no_update, no_update           
        else:
            [{}], no_update, no_update, no_update,no_update, no_update    
    else:
        [{}], no_update, no_update, no_update,no_update, no_update

@app.callback(Output('greeks', 'data'), [Input('live-update', 'n_intervals')], 
              [State('product', 'children')]   
    )
def updateData(interval, product):
    if product:
        data = loadRedisData(product.lower())
        if data != None:
            data = json.loads(data)            
            return data
        else : return 0   

@app.callback(Output('hidden-div1', 'data'), [Input('live-update', 'n_intervals')], 
              [State('product', 'children')]   
    )
def updateParams(interval, product):
    if product:
        params = retriveParams(product.lower())
    
        return params

#calc future change
@app.callback(
    Output('change', 'children'),
    [Input('forward', 'children'),
    Input('cref', 'children')]  
)
def calcChange(forward,ref):
    if forward == 'ERROR':
        x= 'MD lost'
    else:
        if forward and ref:
            forward = float(forward)
            ref = float(ref)
            if forward > 0 and ref > 0:
                change = (forward - ref)/ref
                change = '{} %'.format(round(change*100,2))
                return change

#add current col params   
@app.callback(
    [Output(component_id='cvol', component_property='children'),
     Output(component_id='cskew', component_property='children'),
     Output(component_id='ccalls', component_property='children'),
     Output(component_id='cputs', component_property='children'),
     Output(component_id='ccmax', component_property='children'),
     Output(component_id='cpmax', component_property='children'),
     Output(component_id='cref', component_property='children')     
     ],
    [Input('hidden-div1', 'data') ]  
)
def LoadCurrentParams(params):
    if params == None:
         return no_update, no_update, no_update, no_update, no_update,  no_update, no_update
    else:
        mult = 100
        return str("%.2f" % (params['vol']*mult)), str("%.2f" % (params['skew']*mult)), str("%.2f" % (params['call']*mult)), str("%.2f" % (params['put']*mult)), str("%.2f" % (params['cmax']*mult)),  str("%.2f" % (params['pmax']*mult)), str("%.2f" % (params['ref']*1))

#update ref
@app.callback(
    [Output('ref', 'value'),
     Output('spread', 'value'),
     Output('pmax', 'value'),
     Output('cmax', 'value'),
     Output('puts', 'value'),
     Output('calls', 'value'),
     Output('skew', 'value'),
     Output('vola', 'value'),
     Output('live-update', 'n_intervals')     
    ],
    [Input('product', 'children'), Input('button', 'n_clicks') ],
    [State('{}'.format(i), 'children') for i in ['cspread','cvol','cskew','ccalls','cputs','ccmax','cpmax','cref']]+
    [State('{}'.format(i), 'value') for i in ['spread','vola','skew','calls','puts','cmax','pmax','ref']])
def blankout(product, clicks,cspread, cvol, cskew, ccalls, cputs, ccmax, cpmax, cref, spread, vola, skew, calls, puts, cmax, pmax, ref):
    if clicks:
        if not vola: vola=cvol
        if not spread: spread=cspread
        if not skew: skew=cskew
        if not calls: calls=ccalls
        if not puts: puts=cputs
        if not ref: ref=cref
        if not cmax: cmax=ccmax
        if not pmax: pmax=cpmax

        cleaned_df = {'spread':float(spread),'vola':float(vola)/100, 'skew':float(skew)/100, 'calls':float(calls)/100, 'puts': float(puts)/100, 'cmax':float(cmax)/100, 'pmax':float(pmax)/100, 'ref': float(ref) }
        print(cleaned_df)
        sumbitVolas(product.lower(),cleaned_df)
        return '', '','','','','','','',1
    else: return no_update, no_update, no_update, no_update, no_update,  no_update, no_update, no_update, no_update

#update ringtime
@app.callback(
    Output('ringVs','children'), 
    [Input('live-update', 'n_intervals')]
    )
def updareRing(interval):
    return ringTime()

   