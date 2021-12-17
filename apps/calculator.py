from dash.dependencies import Input, Output, State, ClientsideFunction
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
from dash import no_update
from datetime import datetime
from datetime import date
import dash_table as dtable
import pandas as pd
import datetime as dt
import time, os, json
from dash.exceptions import PreventUpdate
from flask import request

from TradeClass import TradeClass, Option
from sql import sendTrade, storeTradeSend, pullCodeNames, updatePos
from parts import topMenu, calc_lme_vol, onLoadProductProducts, sendPosQueueUpdate, loadRedisData, pullCurrent3m, buildTradesTableData, retriveParams, updateRedisDelta, updateRedisPos, updateRedisTrade, sendFIXML, tradeID, loadVolaData, buildSurfaceParams, codeToName, codeToMonth, onLoadProductMonths 

stratColColor = '#9CABAA'

def fetechstrikes(product):
    if product[-2:]=='3M':
        return {'label': 0, 'value': 0}

    if product != None:
        strikes = []
        data = loadRedisData(product.lower())
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

def buildProductName(product, strike, Cop):
    if strike == None and Cop == None:
        return product
    else:
        return product+' '+str(strike)+' '+Cop

def buildCounterparties():
    #load couterparties from DB
    try:
        df = pullCodeNames()
        nestedOptions = df['codename'].values
        options=[{'label':opt, 'value':opt} for opt in nestedOptions]
        options.append({'label':'ERROR', 'value':'ERROR'})
    except Exception as e:
        print('failed to load codenames')
        print(e)
        options = [{'label':'ERROR', 'value':'ERROR'}]

    return options 

def excelNameConversion(name):
    if name == 'cu': return 'LCUO'
    elif name == 'zn': return 'LZHO'
    elif name == 'ni': return 'LNDO'
    elif name == 'pb': return 'PBDO'
    elif name == 'al': return 'LADO'
    
stratOptions = [{'label': 'Outright' , 'value':'outright'},
                {'label': 'Spread' , 'value':'spread'},
                {'label': 'Straddle/Strangle' , 'value':'straddle'}, 
                {'label': 'Fly' , 'value':'fly'},
                {'label': 'Condor' , 'value':'condor'},
                {'label': 'Ladder' , 'value':'ladder'},  
              {'label': '1*2' , 'value':'ratio'} ]       

stratConverstion = {'outright':[1,0,0,0],
                 'spread' :[1,-1,0,0],
                'straddle' :[1,1,0,0], 
                'fly' :[1,-2,1,0],
                'condor' :[1,-1,-1,1],
                'ladder' :[1,-1,-1,0],
              'ratio' :[1,-2,0,0]}  
  
#trades table layout
calculator = dbc.Col([
#top row lables
            dbc.Row([
                    dbc.Col(['Basis'], width = 4),
                    dbc.Col(['Forward'], width = 4),
                    dbc.Col(['Interest'], width = 4)
                    ]
                    ),
#top row values
            dbc.Row([
                    dbc.Col([dcc.Input(id='calculatorBasis',  type='text', debounce=True)], width = 4),
                    dbc.Col([dcc.Input(id='calculatorForward',  type='text')], width = 4),
                    dbc.Col([dcc.Input(id='interestRate',  type='text')], width = 4),
                    ]
                    ),
#second row labels
            dbc.Row(    [ 
                    dbc.Col([html.Div('Spread')], width = 4),
                    dbc.Col([html.Div('Strategy')],width = 4),
                    dbc.Col([html.Div('Days Convention')],width = 4)
                    ]
                ),
#second row values
            dbc.Row([
                 dbc.Col([html.Div([dcc.Input(type='text', id='calculatorSpread',debounce=True)])], width = 4),
                 dbc.Col([html.Div([dcc.Dropdown(id = 'strategy', value = 'outright', options = stratOptions)])], width = 4),
                 dbc.Col([html.Div([dcc.Dropdown('dayConvention',
                                        value = '',
                                        options = [{'label': 'Bis/Bis', 'value': 'b/b'},
                                                   {'label': 'Calendar/365', 'value': ''}]
                         )])], width = 4),
                 ]  ),
#model settings
            dbc.Row(
                [
                 dbc.Col([html.Div([dcc.RadioItems(id = 'calculatorVol_price',
                        options=[{'label': 'Vol', 'value': 'vol'},
                                 {'label': 'Price', 'value': 'price'}],   value='vol'
                             )])], width = 3),
                 dbc.Col([html.Div([dcc.RadioItems(id = 'nowOpen',
                        options=[{'label': 'Now', 'value': 'now'},
                                 {'label': 'Open', 'value': 'open'}],   value='open'
                             )])], width = 3),
                 dbc.Col([html.Div('Counterparty:')], width = 3),
                 dbc.Col([dcc.Dropdown(id='counterparty', value = '', options = buildCounterparties())], width = 3),
                 ]),
#leg inputs and outputs

#leg inputs
#labels
dbc.Row([
        dbc.Col(['Strike: '], width = 2),
        dbc.Col([dcc.Input(id = 'oneStrike')], width = 2),
        dbc.Col([dcc.Input(id = 'twoStrike')], width = 2),
        dbc.Col([dcc.Input(id = 'threeStrike')], width = 2),
        dbc.Col([dcc.Input(id = 'fourStrike')], width = 2),
        dbc.Col([dcc.Input(id='qty',  type='number', value = 10, min = 0)], width = 2),        
          ]),
dbc.Row([
        dbc.Col(['Price/Vol: '], width = 2),
        dbc.Col([dcc.Input(id = 'oneVol_price')], width = 2),
        dbc.Col([dcc.Input(id = 'twoVol_price')], width = 2),
        dbc.Col([dcc.Input(id = 'threeVol_price')], width = 2),
        dbc.Col([dcc.Input(id = 'fourVol_price')], width = 2),
        dbc.Col([dbc.Button('Buy', id = 'buy', n_clicks_timestamp='0', active = True)], width = 1),
        dbc.Col([dbc.Button('Sell', id = 'sell', n_clicks_timestamp='0', active = True)], width = 1)         
          ]), 
dbc.Row([
        dbc.Col(['C/P: '], width = 2),
        dbc.Col([dcc.Dropdown(id='oneCoP', value = 'c', options = [{'label': 'C', 'value': 'c'},{'label': 'P', 'value': 'p'}],
              style = {'height': '50%', 'verticalAlign':"middle"}              )], width = 2),
        dbc.Col([dcc.Dropdown(id='twoCoP', value = 'c', options = [{'label': 'C', 'value': 'c'},{'label': 'P', 'value': 'p'}],
              style = {'height': '50%', 'verticalAlign':"middle"}              )], width = 2),
        dbc.Col([dcc.Dropdown(id='threeCoP', value = 'c', options = [{'label': 'C', 'value': 'c'},{'label': 'P', 'value': 'p'}],
              style = {'height': '50%', 'verticalAlign':"middle"}              )], width = 2),
        dbc.Col([dcc.Dropdown(id='fourCoP', value = 'c', options = [{'label': 'C', 'value': 'c'},{'label': 'P', 'value': 'p'}],
              style = {'height': '50%', 'verticalAlign':"middle"}              )], width = 2),
                ]), 
dbc.Row([
        dbc.Col(['Theo: '], width = 2),
        dbc.Col([html.Div(id = 'oneTheo')], width = 2),
        dbc.Col([html.Div(id = 'twoTheo')], width = 2),
        dbc.Col([html.Div(id = 'threeTheo')], width = 2),
        dbc.Col([html.Div(id = 'fourTheo')], width = 2),
        dbc.Col([html.Div(id = 'stratTheo',style={'background':stratColColor})], width = 2),
                ]), 
dbc.Row([
        dbc.Col(['IV: '], width = 2),
        dbc.Col([html.Div(id = 'oneIV')], width = 2),
        dbc.Col([html.Div(id = 'twoIV')], width = 2),
        dbc.Col([html.Div(id = 'threeIV')], width = 2),
        dbc.Col([html.Div(id = 'fourIV')], width = 2),
        dbc.Col([html.Div(id = 'stratIV',style={'background':stratColColor})], width = 2),
                ]), 
dbc.Row([
        dbc.Col(['Settle IV:'], width = 2),
        dbc.Col([html.Div(id = 'oneSettleVol')], width = 2),
        dbc.Col([html.Div(id = 'twoSettleVol')], width = 2),
        dbc.Col([html.Div(id = 'threeSettleVol')], width = 2),
        dbc.Col([html.Div(id = 'fourSettleVol')], width = 2),
        dbc.Col([html.Div(id = 'stratSettleVol',style={'background':stratColColor})], width = 2),
                ]),
dbc.Row([
        dbc.Col(['Delta: '], width = 2),
        dbc.Col([html.Div(id = 'oneDelta')], width = 2),
        dbc.Col([html.Div(id = 'twoDelta')], width = 2),
        dbc.Col([html.Div(id = 'threeDelta')], width = 2),
        dbc.Col([html.Div(id = 'fourDelta')], width = 2),
        dbc.Col([html.Div(id = 'stratDelta',style={'background':stratColColor})], width = 2),
                ]),
dbc.Row([
        dbc.Col(html.Div('Full Delta: ', id= 'fullDeltaLabel'), width = 2),
        dbc.Col([html.Div(id = 'oneFullDelta')], width = 2),
        dbc.Col([html.Div(id = 'twoFullDelta')], width = 2),
        dbc.Col([html.Div(id = 'threeFullDelta')], width = 2),
        dbc.Col([html.Div(id = 'fourFullDelta')], width = 2),
        dbc.Col([html.Div(id = 'stratFullDelta',style={'background':stratColColor})], width = 2),
                ]), 
dbc.Row([
        dbc.Col(['Gamma: '], width = 2),
        dbc.Col([html.Div(id = 'oneGamma')], width = 2),
        dbc.Col([html.Div(id = 'twoGamma')], width = 2),
        dbc.Col([html.Div(id = 'threeGamma')], width = 2),
        dbc.Col([html.Div(id = 'fourGamma')], width = 2),
        dbc.Col([html.Div(id = 'stratGamma',style={'background':stratColColor})], width = 2),
                ]), 
dbc.Row([
        dbc.Col(['Vega: '], width = 2),
        dbc.Col([html.Div(id = 'oneVega')], width = 2),
        dbc.Col([html.Div(id = 'twoVega')], width = 2),
        dbc.Col([html.Div(id = 'threeVega')], width = 2),
        dbc.Col([html.Div(id = 'fourVega')], width = 2),
        dbc.Col([html.Div(id = 'stratVega',style={'background':stratColColor})], width = 2),
                ]), 
dbc.Row([
        dbc.Col(['Theta: '], width = 2),
        dbc.Col([html.Div(id = 'oneTheta')], width = 2),
        dbc.Col([html.Div(id = 'twoTheta')], width = 2),
        dbc.Col([html.Div(id = 'threeTheta')], width = 2),
        dbc.Col([html.Div(id = 'fourTheta')], width = 2),
        dbc.Col([html.Div(id = 'stratTheta',style={'background':stratColColor})], width = 2),
                ]), 
dbc.Row([
        dbc.Col(html.Div('Vol Theta: ', id = 'volThetaLabel'), width = 2),
        dbc.Col([html.Div(id = 'onevolTheta')], width = 2),
        dbc.Col([html.Div(id = 'twovolTheta')], width = 2),
        dbc.Col([html.Div(id = 'threevolTheta')], width = 2),
        dbc.Col([html.Div(id = 'fourvolTheta')], width = 2),
        dbc.Col([html.Div(id = 'stratvolTheta',style={'background':stratColColor})], width = 2),
                ]),     
    
             ], width = 9 )

hidden = (
    #hidden to store greeks from the 4 legs
        # html.Div(id='oneCalculatorCalculatorData', style={'display':'none'}), 
        # html.Div(id='twoCalculatorCalculatorData', style={'display':'none'}), 
        # html.Div(id='threeCalculatorCalculatorData', style={'display':'none'}), 
        # html.Div(id='fourCalculatorCalculatorData', style={'display':'none'}), 
        dcc.Store(id='tradesStore'),
        dcc.Store(id= 'paramsStore'),
        dcc.Store(id = 'productInfo'),
        html.Div(id='trades_div', style={'display': 'none'}),
        html.Div(id='trade_div', style={'display': 'none'}),
        html.Div(id='trade_div2', style={'display': 'none'}),
        html.Div(id='productData', style={'display': 'none'})
    )

actions = dbc.Row([
    dbc.Col([html.Button('Delete', id = 'delete', n_clicks_timestamp=0)], width = 3),
    dbc.Col([html.Button('Trade', id = 'trade', n_clicks_timestamp=0)], width = 3),
    dbc.Col([html.Button('Client Recap', id = 'clientRecap', n_clicks_timestamp=0)], width = 3),
    dbc.Col([html.Button('Report', id = 'report', n_clicks_timestamp=0)], width = 3)
    ])

columns=[
            {'id': 'Instrument', 'name': 'Instrument'},
            {'id': 'Qty', 'name': 'Qty', },
            {'id': 'Theo', 'name': 'Theo', },
            {'id': 'Prompt', 'name': 'Prompt'},
            {'id': 'Forward', 'name': 'Forward'},
            {'id': 'IV', 'name': 'IV'},
            {'id': 'Delta', 'name': 'Delta'},
            {'id': 'Gamma', 'name': 'Gamma'},
            {'id': 'Vega', 'name': 'Vega'},
            {'id': 'Theta', 'name': 'Theta'},
            {'id': 'Counterparty', 'name': 'Counterparty'}
                    ]

tables =  dbc.Col(
        dtable.DataTable(id = 'tradesTable',                              
                              data=[{}],
                              columns=columns,
                              row_selectable='multi',
                              editable=True,

                            )   
        )
       
toolTips = html.Div([
    dbc.Tooltip(
        "Theta in terms of vol change equivalent",
        target = 'volThetaLabel'
        ),
     dbc.Tooltip(
        "Full price change of the option with underlying" 
        "including both BS dleta and the option moving on the Vol surface",
        target = 'fullDeltaLabel'
        ),   

    ])

sideMenu = dbc.Col([
        dbc.Row(
            dbc.Col([dcc.Dropdown(id='productCalc-selector', value =onLoadProductProducts()[1],  options =  onLoadProductProducts()[0])], width = 12)
                     ),
        dbc.Row(
            dbc.Col([ dcc.Dropdown(id='monthCalc-selector')], width = 12)
                ),
        dbc.Row(
           dbc.Col([ 'Product:'], width = 12)
            ),
        dbc.Row(
            dbc.Col(['Expiry:'], width = 12)
            ),
        dbc.Row(
               dbc.Col(
                   [html.Div('expiry',id ='calculatorExpiry')], width = 12)
                ),
        dbc.Row(
                dbc.Col(['Third Wednesday:'], width =12)
                ),
        dbc.Row(
            dbc.Col([html.Div('3wed',id = '3wed')])
            ),
    ], width = 3)

output =  dcc.Markdown(id = 'reponseOutput') 

alert = html.Div([
    dbc.Alert(
            'Trade sent',
            id="tradeSent",
            dismissable=True,
            is_open = False,
            duration=3000,
            color="success"
                   ),       
    dbc.Alert(
            'Trade Routed',
            id="tradeRouted",
            dismissable=True,
            is_open = False,
            duration=3000,
            color="success"
                   ),  
    dbc.Alert(
            'Trade Routing Failed',
            id="tradeRouteFail",
            dismissable=True,
            is_open = False,
            duration=3000,
            color="danger"
                   )    
    ])

layout = html.Div([
    topMenu('Calculator'),
    dbc.Row(alert),
    dbc.Row(hidden),
    dbc.Row([sideMenu, calculator]),
    dbc.Row(tables),
    actions, 
    dbc.Row(output),
    toolTips
    ])

def initialise_callbacks(app):
    @app.callback(Output('productData', 'children'),
                [Input('productCalc-selector', 'value')])
    def updateSpread1(product):
        
        params = retriveParams(product.lower())
        if params:
            spread = params['spread']
            return spread
        
    #load vola params for us fulldelta calc later
    @app.callback(Output('paramsStore', 'data'),
                [Input('productCalc-selector', 'value'),
                Input('monthCalc-selector', 'value'),
                Input('calculatorForward' , 'value'),
                Input('calculatorForward' , 'placeholder'),
                Input('calculatorExpiry' , 'children'),
                ]               
                )
    def updateSpread1(product, month, spot, spotP, expiry):
        #build product from month and product
        if product and month:
            if month != '3M':
                product = product + 'O' + month
                params = loadVolaData(product.lower())
                if params:
                    return params

    #update months options on product change
    @app.callback(Output('monthCalc-selector', 'options'),
                [Input('productCalc-selector', 'value')])
    def updateOptions(product):
        
        if product:
            return onLoadProductMonths(product)[0]

    #update months value on product change
    @app.callback(Output('monthCalc-selector', 'value'),
                [Input('monthCalc-selector', 'options')])
    def updatevalue(options):
        if options:
            return options[0]['value']

    #change the CoP dropdown options depning on if £m or not
    @app.callback([Output('oneCoP', 'options'),
                Output('twoCoP', 'options'),
                Output('threeCoP', 'options'),
                Output('fourCoP', 'options'),
                Output('oneCoP', 'value'),
                Output('twoCoP', 'value'),
                Output('threeCoP', 'value'),
                Output('fourCoP', 'value')],
                [Input('monthCalc-selector', 'value')])
    def sendCopOptions(month):

        if month == '3M':
            options = [{'label': 'F', 'value': 'f'}]
            return options, options, options, options, 'f', 'f', 'f', 'f'
        else:
            options = [{'label': 'C', 'value': 'c'},{'label': 'P', 'value': 'p'}, {'label': 'F', 'value': 'f'}]
            return options, options, options, options, 'c', 'c', 'c', 'c'

    @app.callback(Output('tradesTable', 'data'),
                [Input('tradesStore', 'data')])
            
    def loadTradeTable(data):
        if data != None:
            trades = buildTradesTableData(data)   
            return trades.to_dict('records')

        else:
            return [{}]

    @app.callback([Output('tradesStore', 'data'),
                Output('tradesTable', 'selected_rows')],
                                [Input('buy', 'n_clicks_timestamp'),
                                Input('sell', 'n_clicks_timestamp'),
                                Input('delete', 'n_clicks_timestamp'),
                                ],
                                #standard trade inputs
                                [State('tradesTable', 'selected_rows'),
                                State('tradesTable', 'data'),
                                State('calculatorVol_price', 'value'),
                                State('tradesStore' , 'data'),
                                State('counterparty' , 'value'),
                                State('3wed', 'children'),
                                #State('trades_div' , 'children'),
                                State('productCalc-selector', 'value'),
                                State('monthCalc-selector', 'value'),
                                State('qty', 'value'),
                                State('strategy', 'value'),
                                #trade value inputs
                                #one vlaues
                                State('oneStrike', 'value'),
                                State('oneStrike', 'placeholder'),
                                State('oneCoP', 'value'),
                                State('oneTheo' , 'children'),
                                State('oneIV' , 'children'),
                                State('oneDelta' , 'children'),
                                State('oneGamma' , 'children'),
                                State('oneVega' , 'children'),
                                State('oneTheta' , 'children'),

                                #two values
                                State('twoStrike', 'value'),
                                State('twoStrike', 'placeholder'),
                                State('twoCoP', 'value'),
                                State('twoTheo' , 'children'),
                                State('twoIV' , 'children'),
                                State('twoDelta' , 'children'),
                                State('twoGamma' , 'children'),
                                State('twoVega' , 'children'),
                                State('twoTheta' , 'children'),

                                #three values
                                State('threeStrike', 'value'),
                                State('threeStrike', 'placeholder'),
                                State('threeCoP', 'value'),
                                State('threeTheo' , 'children'),
                                State('threeIV' , 'children'),
                                State('threeDelta' , 'children'),
                                State('threeGamma' , 'children'),
                                State('threeVega' , 'children'),
                                State('threeTheta' , 'children'),

                                #four values
                                State('fourStrike', 'value'),
                                State('fourStrike', 'placeholder'),
                                State('fourCoP', 'value'),
                                State('fourTheo' , 'children'),
                                State('fourIV' , 'children'),
                                State('fourDelta' , 'children'),
                                State('fourGamma' , 'children'),
                                State('fourVega' , 'children'),
                                State('fourTheta' , 'children'),

                                State('calculatorExpiry' , 'children'),
                                State('3wed' , 'children'),
                                State('calculatorForward' , 'value'),
                                State('calculatorForward' , 'placeholder'),
                                    ]
            )
    def stratTrade(buy, sell, delete, 
                clickdata, rows, pricevola, data, counterparty, tm, product, month, qty, strat,
                onestrike, ponestrike, onecop, onetheo, oneiv, onedelta, onegamma, onevega, onetheta, 
                twostrike, ptwostrike, twocop, twotheo, twoiv, twodelta, twogamma, twovega, twotheta, 
                threestrike, pthreestrike, threecop, threetheo, threeiv, threedelta, threegamma, threevega, threetheta,
                fourstrike, pfourstrike, fourcop, fourtheo, fouriv, fourdelta, fourgamma, fourvega, fourtheta,
                expiry, wed, forward, pforward):
        
        if (int(buy) + int(sell)+ int(delete)) == 0:
            return [], []

        #replace rows if nonetype
        if not clickdata:
            clickdata = []

        #reset buy sell signal
        bs = 0
        #convert qty to float to save multiple tims later
        qty = float(qty)

        #build product from month and product dropdown
        if product and month:
            product = product + 'O' + month

            if data:
                trades = data  
            else: trades = {}

            #on delete work over indices and delete rows then update trades dict
            if int(delete) > int(buy) and int(delete) > int(sell):
                if clickdata:
                    for i in clickdata:
                        instrument = rows[i]['Instrument']
                        trades.pop(instrument, None)
                        clickdata = []
            else:
                #not delete so see ifs its a buy/sell button click
                #create name then go over buy/sell and action

                #find the stat mults
                statWeights = stratConverstion[strat]
                #clac forward and prompt
                if forward:
                    Bforward = forward
                else:
                    Bforward = pforward  

                prompt = dt.datetime.strptime(tm, '%Y-%m-%d').strftime('%Y-%m-%d')
                futureName = str(product)[:3]+ ' '+ str(prompt)

                #calc strat for buy
                if int(buy) > int(sell) and int(buy) > int(delete):
                    bs = 1

                elif int(buy) < int(sell) and int(sell) > int(delete):  
                    bs = -1


                if bs != 0:
                    deltaBucket = 0
                    #calc one leg weight
                    if statWeights[0] !=0:
                        weight = statWeights[0]*bs
                        #get strike from value and placeholder
                        if onecop == 'f':
                            hedge = {'qty':float(onedelta)*weight*qty , 'theo': Bforward, 'prompt' : prompt, 'forward': Bforward, 'iv': 0, 'delta': float(onedelta)*weight*qty, 'gamma': 0, 'vega': 0, 'theta': 0, 'counterparty': counterparty }
                            if futureName in trades:
                                trades[futureName]['qty'] = trades[futureName]['qty'] +  hedge['qty']
                            else: trades[futureName] =  hedge
                        else:
                            if not onestrike: onestrike = ponestrike    
                            #onestrike = strikePlaceholderCheck(onestrike, ponestrike)
                            name = str(product) + ' '+ str(onestrike) + ' '+ str(onecop).upper()
                            trades[name] = {'qty': qty*weight, 'theo': float(onetheo), 'prompt' : prompt, 'forward': Bforward, 'iv': float(oneiv)*weight, 'delta': float(onedelta)*weight*qty, 'gamma': float(onegamma)*weight*qty, 'vega': float(onevega)*weight*qty, 'theta': float(onetheta)*weight*qty, 'counterparty': counterparty } 
                            #add delta to delta bucket for hedge
                            deltaBucket += float(onedelta)*weight*qty

                    #calc two leg weight
                    if statWeights[1] !=0:
                        weight = statWeights[1]*bs
                        if twocop == 'f':
                            hedge = {'qty':float(onedelta)*weight*qty , 'theo': Bforward, 'prompt' : prompt, 'forward': Bforward, 'iv': 0, 'delta': float(twodelta)*weight*qty, 'gamma': 0, 'vega': 0, 'theta': 0, 'counterparty': counterparty }
                            if futureName in trades:
                                trades[futureName]['qty'] = trades[futureName]['qty'] +  hedge['qty']
                            else: trades[futureName] =  hedge
                        else:
                            #get strike from value and placeholder
                            if not twostrike: twostrike = ptwostrike

                            name = str(product) + ' '+ str(twostrike) + ' '+ str(twocop).upper()
                            trades[name] = {'qty': float(qty)*weight, 'theo': float(twotheo), 'prompt' : prompt, 'forward': Bforward, 'iv': float(twoiv)*weight, 'delta': float(twodelta)*weight*qty, 'gamma': float(twogamma)*weight*qty, 'vega': float(twovega)*weight*qty, 'theta': float(twotheta)*weight*qty, 'counterparty': counterparty }  
                            #add delta to delta bucket for hedge
                            deltaBucket += float(twodelta)*weight*qty

                    #calc three leg weight
                    if statWeights[2] !=0:
                        weight = statWeights[2]*bs
                        if twocop == 'f':
                            hedge = {'qty':float(threedelta)*weight*qty , 'theo': Bforward, 'prompt' : prompt, 'forward': Bforward, 'iv': 0, 'delta': float(threedelta)*weight*qty, 'gamma': 0, 'vega': 0, 'theta': 0, 'counterparty': counterparty }
                            if futureName in trades:
                                trades[futureName]['qty'] = trades[futureName]['qty'] +  hedge['qty']
                            else: trades[futureName] =  hedge
                        else:
                            #get strike from value and placeholder
                            if not threestrike: threestrike = pthreestrike
                            name = str(product) + ' '+ str(threestrike) + ' '+ str(threecop).upper()
                            trades[name] = {'qty': float(qty)*weight, 'theo': float(threetheo), 'prompt' : prompt, 'forward': Bforward, 'iv': float(threeiv)*weight, 'delta': float(threedelta)*weight*qty, 'gamma': float(threegamma)*weight*qty, 'vega': float(threevega)*weight*qty, 'theta': float(threetheta)*weight*qty, 'counterparty': counterparty }     
                            #add delta to delta bucket for hedge
                            deltaBucket += float(threedelta)*weight*qty

                    #calc four leg weight
                    if statWeights[3] !=0:
                        weight = statWeights[3]*bs
                        if twocop == 'f':
                            hedge = {'qty':float(fourdelta)*weight*qty , 'theo': Bforward, 'prompt' : prompt, 'forward': Bforward, 'iv': 0, 'delta': float(fourdelta)*weight*qty, 'gamma': 0, 'vega': 0, 'theta': 0, 'counterparty': counterparty }
                            if futureName in trades:
                                trades[futureName]['qty'] = trades[futureName]['qty'] +  hedge['qty']
                            else: trades[futureName] =  hedge
                        else:
                            #get strike from value and placeholder
                            if not fourstrike: fourstrike = pfourstrike
                            name = str(product) + ' '+ str(fourstrike) + ' '+ str(fourcop).upper()
                            trades[name] = {'qty': float(qty)*weight, 'theo': float(fourtheo), 'prompt' : prompt, 'forward': Bforward, 'iv': float(fouriv)*weight, 'delta': float(fourdelta)*weight*qty, 'gamma': float(fourgamma)*weight*qty, 'vega': float(fourvega)*weight*qty, 'theta': float(fourtheta)*weight*qty, 'counterparty': counterparty }  
                            #add delta to delta bucket for hedge
                            deltaBucket += float(fourdelta)*weight*qty
                    #if vol trade then add hedge along side
                    if pricevola == 'vol':
                        delta = round(float(deltaBucket),0)*-1

                        hedge = {'qty':delta , 'theo': Bforward, 'prompt' : prompt, 'forward': Bforward, 'iv': 0, 'delta': delta, 'gamma': 0, 'vega': 0, 'theta': 0, 'counterparty': counterparty }
                        if futureName in trades:
                            trades[futureName]['qty'] = trades[futureName]['qty'] +  hedge['qty']
                        else: trades[futureName] =  hedge
            return trades, clickdata

    #delete all input values on product changes
    @app.callback([Output('oneStrike', 'value'),
                Output('oneVol_price', 'value'),
                Output('twoStrike', 'value'),
                Output('twoVol_price', 'value'),
                Output('threeStrike', 'value'),
                Output('threeVol_price', 'value'),
                Output('fourStrike', 'value'),
                Output('fourVol_price', 'value'),               
                ],
                [Input('productCalc-selector', 'value'), 
                Input('monthCalc-selector', 'value')])
    def clearSelectedRows(product, month):
        return '','','','','','','',''

    #send trade to system
    @app.callback(Output('tradeSent', 'is_open'),
                [Input('trade', 'n_clicks')],
                [State('tradesTable', 'selected_rows'),
                State('tradesTable', 'data') ])
    def sendTrades(clicks, indices, rows):
        timestamp= timeStamp()  
        #pull username from site header
        user = request.headers.get('X-MS-CLIENT-PRINCIPAL-NAME')
        if not user:
            user = 'Test'

        if indices:
            for i in indices:
                #create st to record which products to update in redis 
                redisUpdate = set([])   
                #check that this is not the total line.
                if rows[i]['Instrument'] != 'Total':

                    if rows[i]['Instrument'][3] =='O':
                        #is option
                        product = rows[i]['Instrument'][:6]
                        redisUpdate.add(product)
                        productName= (rows[i]['Instrument']).split(' ')
                        strike = productName[1]
                        CoP = productName[2]

                        prompt = rows[i]['Prompt']
                        price = rows[i]['Theo']
                        qty = rows[i]['Qty']
                        counterparty  = rows[i]['Counterparty']

                        trade = TradeClass(0, timestamp, product, strike, CoP, prompt, price, qty, counterparty, '', user, 'Georgia')
                        #send trade to DB and record ID returened
                        print('Sending trade to sendTrade')
                        trade.id = sendTrade(trade)
                        updatePos(trade)
                        
                    elif rows[i]['Instrument'][3] ==' ':
                        #is futures
                        product = rows[i]['Instrument'][:3]
                        redisUpdate.add(product)
                        prompt = rows[i]['Prompt']
                        price = rows[i]['Theo']
                        qty = rows[i]['Qty']
                        counterparty  = rows[i]['Counterparty']

                        trade = TradeClass(0, timestamp, product, None, None, prompt, price, qty, counterparty, '', user, 'Georgia')
                        #send trade to DB and record ID returened
                        trade.id = sendTrade(trade)
                        updatePos(trade)
                        
                    #update redis for each product requirng it
                    for update in redisUpdate: 
                        updateRedisDelta(update)
                        updateRedisPos(update)
                        updateRedisTrade(update)
                        #updateRedisCurve(update)
                        sendPosQueueUpdate(update)
            return True

    #send trade to F2 and exchange
    @app.callback([Output('reponseOutput', 'children')],
                [Input('report', 'n_clicks_timestamp'), 
                Input('clientRecap', 'n_clicks_timestamp')],
                [State('tradesTable', 'selected_rows'),
                State('tradesTable', 'data') ])
    def sendTrades(report, recap, indices, rows):

        #string to hold router respose
        tradeResponse = '## Response'
        if (int(report) + int(recap)) == 0:
            raise PreventUpdate

        #enact trade recap logic 
        if int(recap)> int(report):
            response = 'Recap: \r\n'
            if indices:
                for i in indices:
                    if rows[i]['Instrument'][3] =='O':
                        #is option
                        instrument = rows[i]['Instrument'].split()
                        product = codeToName(instrument[0])
                        strike = instrument[1]
                        CoP = instrument[2]
                        month = codeToMonth(instrument[0])

                        if CoP == 'C': CoP = 'calls'
                        elif CoP == 'P': CoP = 'puts'

                        price = round(abs(float(rows[i]['Theo'])),2)
                        qty = float(rows[i]['Qty'])
                        vol = round(abs(float(rows[i]['IV'])),2)
                        if qty > 0: bs = 'Sell'
                        elif qty < 0: bs = 'Buy'
                        else: continue

                        response += 'You {} {} {} {} {} {} at {} ({}%) \r\n'.format(bs,abs(int(qty)), month, product, strike, CoP, price, round(vol,2))
                    elif rows[i]['Instrument'][3] ==' ':
                        #is futures
                        instrument = rows[i]['Instrument'].split()
                        date = datetime.strptime(instrument[1], '%Y-%m-%d')
                        month = date.strftime('%b')[:3]
                        product = rows[i]['Instrument'][:3]
                        prompt = rows[i]['Prompt']
                        price = rows[i]['Theo']
                        qty = rows[i]['Qty']
                        if qty > 0: bs = 'Sell'
                        elif qty < 0: bs = 'Buy'


                        response += 'You {} {} {} {} at {} \r\n'.format(bs,abs(int(qty)), month, product, price)
                
                return response
            else: return 'No rows selected'

        #find user related trade details 
        timestamp= timeStamp()
        #pull username from site header
        user = request.headers.get('X-MS-CLIENT-PRINCIPAL-NAME')
        if int(recap)< int(report):
            if indices:
                for i in indices:
                    if rows[i]['Instrument'][3] =='O':
                        #is option
                        instrument = rows[i]['Instrument'].split()
                        product = instrument[0]
                        strike = instrument[1]
                        CoP = instrument[2]
                        prompt = rows[i]['Prompt']
                        price = rows[i]['Theo']
                        qty = rows[i]['Qty']
                        vol = rows[i]['IV']
                        counterparty  = rows[i]['Counterparty']
                        underlying = rows[i]['Forward']

                        #build trade object 
                        trade = TradeClass(0, timestamp, product, strike, CoP, prompt, price, qty, counterparty, '', user, 'Georgia', underlying = underlying)
                    
                        #assign unique id to trade
                        trade.id = tradeID()
                        #assign vol
                        trade.vol = vol

                        #take trade fixml
                        fixml = trade.fixml()
                
                        #send it to the soap server
                        response = sendFIXML(fixml)

                        #set pop alert booleans
                        if response['Status'] == 'Routed':
                            good, bad = True, False
                        else: 
                            good, bad = False, True

                        #store action for auditing
                        storeTradeSend(trade,response)

                        response = responseParser(response)

                        #attached reposne to print out
                        tradeResponse = tradeResponse +os.linesep + response

                    elif rows[i]['Instrument'][3] ==' ':
                        #is futures
                        product = rows[i]['Instrument'][:3]
                        prompt = rows[i]['Prompt']
                        price = rows[i]['Theo']
                        qty = rows[i]['Qty']
                        counterparty  = rows[i]['Counterparty']
                        underlying = rows[i]['Forward']

                        #build trade object 
                        trade = TradeClass(0, timestamp, product, None, None, prompt, price, qty, counterparty, '', user, 'Georgia', underlying = underlying)

                        #assign unique id to trade
                        trade.id = tradeID()

                        #take trade fixml
                        fixml = trade.fixml()

                        #send it to the soap server
                        response = sendFIXML(fixml)

                        #set pop alert booleans
                        if response['Status'] == 'Routed':
                            good, bad = True, False
                        else: 
                            good, bad = False, True

                        #store action for auditing
                        storeTradeSend(trade,response)

                        response = responseParser(response)

                        #attached reposne to print out
                        tradeResponse = tradeResponse + os.linesep + response

                return tradeResponse

    def responseParser(response):
        
        return 'Status: {} Error: {}'.format(response['Status'], response['ErrorMessage'])

    @app.callback(Output('calculatorPrice/Vola', 'value'),
                [Input('productCalc-selector', 'value'), Input('monthCalc-selector', 'value')])
    def loadBasis(product, month):
        return '' 

    #update product info on product change
    @app.callback(Output('productInfo', 'data'),
                [Input('productCalc-selector', 'value'),
                Input('monthCalc-selector', 'value'),
                Input('monthCalc-selector', 'options')])
    def updateProduct(product, month, options):
        if month and product:
            if month != '3M':
                product = product + 'O' + month
                params = loadRedisData(product.lower())
                params = json.loads(params)
                return params
            elif month == '3M':
                #get default month params to find 3m price
                product = product + 'O' + options[0]['value']

                topParams = loadRedisData(product.lower())
                topParams = json.loads(topParams)
                #builld 3M param dict
                params = {}
                date = pullCurrent3m()
                #convert to datetime
                date = datetime.strptime(str(date)[:10], '%Y-%m-%d')    
                
                params['third_wed'] = date.strftime("%d/%m/%Y")
                params['m_expiry'] = date.strftime("%d/%m/%Y")
                params['3m_und'] = 0

                return params 

    def placholderCheck(value, placeholder):
        if type(value) is float:
            return value, value
        elif type(placeholder) is float:
            return placeholder, placeholder
        elif value and value != None and value != ' ':
            value = value.split('/')
            if len(value) >1:
                if value[1] != '':
                    return float(value[0]), float(value[1])
                else:
                    return float(value[0]), float(value[0])
            else:
                return float(value[0]), float(value[0])

        elif placeholder and placeholder != ' ':
            placeholder = placeholder.split('/')
            if len(placeholder) >1 and placeholder[1] != ' ':
                return float(placeholder[0]), float(placeholder[1])
            else:
                return float(placeholder[0]), float(placeholder[0])
        else: return 0, 0

    def strikePlaceholderCheck(value, placeholder):
        if value:
            return value
        elif placeholder:
            value = placeholder.split('.')
            return value[0]
        else: return 0

    legOptions = ['one', 'two', 'three', 'four']

    #create fecth strikes function
    def buildFetchStrikes():
        def updateDropdown(product, month, cop):
            if product and month:
                if cop == 'f' or month == '3M':
                    return ''
                else:
                    product = product + 'O' + month
                    strikes = fetechstrikes(product)
                    length = int(len(strikes)/2)
                    value = strikes[length]['value']
                    return value
            return updateDropdown

    #create vola function
    def buildUpdateVola(leg):
        def updateVola(params, strike, pStrike, cop, priceVol, pforward, forward):
            #user input or placeholder
            if not forward: forward = pforward

            if cop == 'f':            
                return forward, None
            else:
                #get strike from strike vs pstrike
                if not strike: strike = pStrike
                strike = int(strike)
                if strike:          
                    if params:
                        params = pd.DataFrame.from_dict(params,orient='index')
                        #if strike is real strike
                        if strike in params['strike'].values:
                            if priceVol == 'vol':
                                vol =round(params.loc[((params['strike'] == strike) & (params['cop'] == 'c'))]['vol'][0]*100,2)
                                settle  = calc_lme_vol(params, float(forward), float(strike))
                                return vol, settle*100
    
                            elif priceVol == 'price':
                                price = round(params.loc[((params['strike'] == strike) & (params['cop'] == 'c'))]['calc_price'][0],2)
                                settle  = calc_lme_vol(params, float(forward), float(strike))
                                return price, settle*100

                else: return 0, 0
        return updateVola

    def buildvolaCalc(leg):
        def volaCalc( expiry, nowOpen, rate, prate, forward, pforward, strike, pStrike, cop, priceVola, ppriceVola, volprice, days, params):
            #get inputs placeholders vs values
            if not strike: strike = pStrike
            Brate, Arate = placholderCheck(rate, prate)
            Bforward, Aforward = placholderCheck(forward, pforward)
            BpriceVola, ApriceVola  = placholderCheck(priceVola, ppriceVola)

            #if no params then return nothing
            if not params or cop == 'f':
                Bgreeks = [0,1,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0]
                return {'bid':Bgreeks, 'Bvol': 0}

            #set eval date
            eval_date = dt.datetime.now()  
            #build params object
            params = buildSurfaceParams(params, Bforward,  expiry ,eval_date)
    
            if None not in (expiry, Bforward, Aforward, BpriceVola, ApriceVola, strike, cop):
                if nowOpen == 'now':
                    now = True
                else: now = False
                today = dt.datetime.today()           
                if volprice == 'vol':
                
                    option = Option(cop,Bforward,strike,today,expiry,Brate/100,BpriceVola/100, days = days, now=now, params = params )
                    Bgreeks = option.get_all()

                    return {'bid':Bgreeks, 'Bvol': BpriceVola}

                elif volprice == 'price':
                    option = Option(cop,Bforward,strike,today,expiry,Brate/100,0,price = BpriceVola,  days = days, now = now, params = params)
                    option.get_impl_vol()
                    Bvol = option.vol
                    Bgreeks = list(option.get_all())
                    Bgreeks[0]= BpriceVola
                    
                    return {'bid':Bgreeks,  'Bvol': Bvol*100}
                
        return volaCalc

    def createLoadParam(param):
        def loadtheo(params):
            #pull greeks from stored hidden
            if params !=None:          
                return  str("%.4f" % params['bid'][param[1]])
            else: 
                return  str("%.4f" % 0)
        return loadtheo

    def buildVoltheta():
    
        def loadtheo(vega, theta):
            if vega !=None and theta != None:
                vega = float(vega)
                if vega>0:
                    return "%.2f" % (float(theta) / vega)
                else: return 0
            else: return 0 
        return loadtheo

    def buildTheoIV():
        def loadIV(params):
            if params !=None:
                #params = json.loads(params)
                return  str("%.4f" % params['vol'])
            else: return 0    
        return loadIV

    @app.callback(
            Output('calculatorForward', 'placeholder'),
            [Input('calculatorBasis','value'), 
            Input('calculatorBasis','placeholder'),
            Input('calculatorSpread','value'), 
            Input('calculatorSpread','placeholder')  ])
    def forward_update(basis, basisp, spread, spreadp):
        if not basis: basis =basisp
        if not spread: spread = spreadp

        return float(basis) + float(spread)
        
    #create placeholder function for each {leg}Strike
    for leg in legOptions:
        #clientside black scholes
            app.clientside_callback(
                ClientsideFunction(
                    namespace='clientside',
                    function_name='blackScholes'
                    ),
                    [Output('{}{}'.format(leg,i), 'children') for i in ['Theo','Delta', 'Gamma', 'Vega', 'Theta','IV']],
                    [Input('calculatorVol_price','value')]+
                    [Input('{}{}'.format(leg,i), 'value') for i in ['CoP', 'Strike', 'Vol_price']]+
                    [Input('{}{}'.format(leg,i), 'placeholder') for i in ['Strike',  'Vol_price']]+
                    [Input('{}'.format(i), 'value') for i in ['calculatorForward', 'interestRate']]+
                    [Input('{}'.format(i), 'placeholder') for i in ['calculatorForward', 'interestRate']]+     
                    [Input('calculatorExpiry', 'children')]
                    )


            #update vol_price placeholder
            app.callback([Output('{}Vol_price'.format(leg), 'placeholder'), Output('{}SettleVol'.format(leg), 'children')],
                [Input('productInfo', 'data'),
                Input('{}Strike'.format(leg), 'value'),
                Input('{}Strike'.format(leg), 'placeholder'),
                Input('{}CoP'.format(leg), 'value'),
                Input('calculatorVol_price', 'value'),
                Input('calculatorForward', 'placeholder'),
                Input('calculatorForward', 'value')
                ]) (buildUpdateVola(leg))

            #calculate the vol thata from vega and theta
            app.callback(
                Output('{}volTheta'.format(leg), 'children'),
                [Input('{}Vega'.format(leg), 'children'), Input('{}Theta'.format(leg), 'children')]
                ) (buildVoltheta())

    def buildStratGreeks():
        def stratGreeks(strat, one, two, three, four):
            if any([one, two, three, four]) and strat:
                strat = stratConverstion[strat]
                #greek = 0
                # for leg, multi in zip([one, two, three, four],strat):
                #     if leg and len(leg) > 0:
                #         leg_greek = leg * multi
                #         greek = greek + float(leg_greek)
                greek = (strat[0] * float(one)) + (strat[1] * float(two)) + (strat[2] * float(three)) + (strat[3] * float(four))
                greek= round(greek,2)
                return str(greek)
            else: return 0
        return stratGreeks

    #add different greeks to leg and calc
    for param in ['Theo', 
                'FullDelta',
                    'Delta', 
                    'Gamma',
                    'Vega',  
                    'Theta', 
                    'IV' ,
                    'volTheta']:

        app.callback(Output('strat{}'.format(param), 'children'),
                    [Input('strategy', 'value'),
                    Input('one{}'.format(param), 'children'),
                    Input('two{}'.format(param), 'children'),
                    Input('three{}'.format(param), 'children'),
                    Input('four{}'.format(param), 'children')])    (
                        buildStratGreeks()
                        )

    inputs = ['interestRate', 'calculatorBasis','calculatorSpread']

    @app.callback([Output('{}'.format(i), 'placeholder') for i in inputs]+
                [Output('{}'.format(i), 'value') for i in inputs]+
                [Output('calculatorExpiry', 'children'), Output('3wed', 'children')]+
                [Output('{}Strike'.format(i), 'placeholder') for i in legOptions],
                [Input('productInfo', 'data')]
                )
    def updateInputs(params):
        if params:
            params = pd.DataFrame.from_dict(params,orient='index')
            atm = params.iloc[0]['und_calc_price']
            params = params.iloc[(params['strike']-atm).abs().argsort()[:2]]
            valuesList = [''] * len(inputs)
            atmList = [params.iloc[0]['strike']] * len(legOptions)
            expriy = date.fromtimestamp(params.iloc[0]['expiry']/ 1e3)
            third_wed = date.fromtimestamp(params.iloc[0]['third_wed']/ 1e3)
            return [params.iloc[0]['interest_rate'], atm - params.iloc[0]['spread'],
            params.iloc[0]['spread']] + valuesList+ [expriy, third_wed] +atmList

        else:
            atmList = [no_update] * len(legOptions)
            valuesList = [no_update] * len(inputs)
            return [no_update for _ in len(inputs)]+valuesList+ [no_update, no_update]+atmList


