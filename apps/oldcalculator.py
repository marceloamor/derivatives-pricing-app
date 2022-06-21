from dash.dependencies import Input, Output, State
from dash import dcc, html
from dash import dcc
from datetime import timedelta 
import json
from dash import dash_table as dtable
import pandas as pd
import datetime as dt
import time, math, ast
from dash.exceptions import PreventUpdate
from flask import request

from TradeClass import TradeClass, Option
from sql import pulltrades, sendTrade, storeTradeSend
from parts import loadRedisData, buildTableData, buildTradesTableData, retriveParams, retriveTickData, loadStaticData, get_theo, updateRedisDelta, updateRedisPos, updateRedisTrade, updatePos, sendFIXML, tradeID
from app import app

def fetechstrikes(product): 
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

def onLoadProduct():
    staticData = loadStaticData()
    staticData = pd.read_json(staticData)
    staticData.sort_values('product')
    products = []
    for product in staticData['product']:
        products.append({'label': product, 'value': product})
    return  products, products[0]['value']

def onLoadProductProducts():
    staticData = loadStaticData()
    staticData = pd.read_json(staticData)
    products = []
    staticData['product'] = [x[:3] for x in staticData['product']]
    productNames = staticData['product'].unique()
    staticData.sort_values('product')
    for product in productNames:
        products.append({'label': product, 'value': product})
    return  products, products[0]['value']

def onLoadProductMonths(product):
    staticData = loadStaticData()
    staticData = pd.read_json(staticData)
    staticData['productShort'] = [x[:3] for x in staticData['product']]
    staticData = staticData.loc[staticData['productShort'] == product.upper()]

    staticData['product'] = [x[4:] for x in staticData['product']]
    productNames = staticData['product'].unique()

    products = []
    for product in productNames:
        products.append({'label': product, 'value': product})
    return  products, products[0]['value']

def buildProductName(product, strike, Cop):
    if strike == None and Cop == None:
        return product
    else:
        return product+' '+str(strike)+' '+Cop

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

countrparties = [
    {'label': 'COMMON' , 'value': 'COMMON'}, 
    {'label': 'LEATHER', 'value': 'LEATHER'}, 
    {'label': 'GRASS' , 'value': 'GRASS'}, 
    {'label': 'GHOST' , 'value': 'GHOST'}, 
    {'label': 'KOI' , 'value': 'KOI'}, 
    {'label': 'CRUCIAN' , 'value': 'CRUCIAN'}, 
    {'label': 'SIAMESE' , 'value': 'SIAMESE'}, 
    {'label': 'MUD', 'value': 'MUD'}, 
    {'label': 'SILVER' , 'value': 'SILVER'}, 
    {'label': 'CATLA' , 'value': 'CATLA'}
    ]                                  

#trades table layout
calculator = html.Div([
#top row lables
            html.Div([
                    html.Div('Basis', className = 'four columns'),
                    html.Div('Forward', className = 'four columns'),
                    html.Div('Interest', className = 'four columns')
                    ],className = 'row'),
#top row values
            html.Div([
                    html.Div([dcc.Input(id='calculatorBasis',  type='text')], className = 'four columns'),
                    html.Div([dcc.Input(id='calculatorForward',  type='text')], className = 'four columns'),
                    html.Div([dcc.Input(id='interestRate',  type='text')], className = 'four columns'),
                    ], className = 'row'),
#second row labels
            html.Div([     
                    html.Div('Spread', className = 'four columns'),
                    html.Div('Strategy', className = 'four columns'),
                    html.Div('Days Convention', className = 'four columns'),
                ], className = 'row'),
#second row values
            html.Div([
                 html.Div([dcc.Input(type='text', id='calculatorSpread1')], className = 'four columns'),
                 html.Div([dcc.Dropdown(id = 'strategy', value = 'outright', options = stratOptions)], className = 'four columns'),
                 html.Div([dcc.Dropdown(id='dayConvention',
                                        value = '',
                                        options = [{'label': 'Bis/Bis', 'value': 'b/b'},
                                                   {'label': 'Calendar/365', 'value': ''}]
                         )], className = 'four columns'),
                 ], className = 'row'),
#model settings
            html.Div([

                 html.Div([dcc.RadioItems(id = 'calculatorVol_price',
                        options=[{'label': 'Vol', 'value': 'vol'},
                                 {'label': 'Price', 'value': 'price'}],   value='vol'
                             )], className = 'three columns'),
                 html.Div([dcc.RadioItems(id = 'nowOpen',
                        options=[{'label': 'Now', 'value': 'now'},
                                 {'label': 'Open', 'value': 'open'}],   value='open'
                             )], className = 'three columns'),


                 ], className = 'row'),
#leg inputs and outputs
html.Div([
#leg inputs
    html.Div([
#labels
        html.Div([
                html.Div('Strike: ', id = 'calctheo', className = 'row'),
                html.Div('Price/Vol: ', id = 'calciv', className = 'row'),
                html.Div('C/P: ', id = 'calciv', className = 'row')           
            ], className = 'two columns'),
#one inputs
        html.Div([
                html.Div([dcc.Input(id = 'oneStrike')], className = 'row'),
                html.Div([dcc.Input(id = 'oneVol_price')], className = 'row'),
                html.Div([dcc.Dropdown(id='oneCoP', value = 'c', options = [{'label': 'C', 'value': 'c'},{'label': 'P', 'value': 'p'}],
              style = {'height': '50%', 'verticalAlign':"middle"}              )], className = 'row')            
            ], className = 'two columns'),
#two inputs
        html.Div([
                html.Div([dcc.Input(id = 'twoStrike')], className = 'row'),
                html.Div([dcc.Input(id = 'twoVol_price')], className = 'row'),
                html.Div([dcc.Dropdown(id='twoCoP', value = 'c', options = [{'label': 'C', 'value': 'c'},{'label': 'P', 'value': 'p'}],
              style = {'height': '50%', 'verticalAlign':"middle"}                                        
                                )], className = 'row'),            
            ], className = 'two columns'),
#three inputs
        html.Div([
                html.Div([dcc.Input(id = 'threeStrike')], className = 'row'),
                html.Div([dcc.Input(id = 'threeVol_price')], className = 'row'),
                html.Div([dcc.Dropdown(id='threeCoP', value = 'c', options = [{'label': 'C', 'value': 'c'},{'label': 'P', 'value': 'p'}],
              style = {'height': '50%', 'verticalAlign':"middle"}
                      )], className = 'row'),            
            ], className = 'two columns'),
#four inputs
        html.Div([
                html.Div([dcc.Input(id = 'fourStrike')], className = 'row'),
                html.Div([dcc.Input(id = 'fourVol_price')], className = 'row'),
                html.Div([dcc.Dropdown(id='fourCoP', value = 'c', options = [{'label': 'C', 'value': 'c'},{'label': 'P', 'value': 'p'}],
              style = {'height': '50%', 'verticalAlign':"middle"}
                         )], className = 'row'),           
            ], className = 'two columns'),
#trade inputs
        html.Div([
                html.Div([dcc.Input(id='qty',  type='number', value = 10, min = 0)], className = 'row'),
                html.Div([
                    html.Div([html.Button('Buy', id = 'buy', n_clicks_timestamp='0')], className = 'six columns'),
                    html.Div([html.Button('Sell', id = 'sell', n_clicks_timestamp='0')], className = 'six columns')                    
                    ], className = 'row')
                ], className = 'two columns'),    
        
            ], className = 'row')
        ],className= 'row'),
#outputs
    html.Div([
#labels
            html.Div([
                html.Div('Theo: ', id = 'theo', className = 'row'),
                html.Div('IV: ', id = 'iv', className = 'row'),
                html.Div('Delta: ', id = 'calcdelta', className = 'row'),
                html.Div('Gamma: ', id = 'calcgamma', className = 'row'),
                html.Div('Vega: ', id = 'calcvega', className = 'row'),
                html.Div('Theta: ', id = 'calctheta', className = 'row'),
                html.Div('Vol Theta: ', id = 'calcvolTheta', className = 'row'),
                html.Div('Full Delta: ', id = 'calcFullDelta', className = 'row')
                #html.Div('Delta Decay: ', id = 'calctheta', className = 'row'),
                #html.Div('Vega Decay: ', id = 'calctheta', className = 'row'),
                #html.Div('Gamma Decay: ', id = 'calctheta', className = 'row'),
                #html.Div([dcc.Input(id='qty',  type='number', value = 10, min = 0)], className = 'row')
                ], className = 'two columns'),        
#column one outputs
            html.Div([
                html.Div(id = 'oneTheo', className = 'row'),
                html.Div(id = 'oneIV', className = 'row'),
                html.Div(id = 'oneDelta', className = 'row'),                
                html.Div(id = 'oneGamma', className = 'row'),
                html.Div(id = 'oneVega', className = 'row'),
                html.Div(id = 'oneTheta', className = 'row'),
                html.Div(id = 'onevolTheta', className = 'row'),
                html.Div(id = 'oneFullDelta', className = 'row'),
                #html.Div(id = 'oneDeltaDecay', className = 'row'),
                #html.Div(id = 'oneVegaDecay', className = 'row'),
                #html.Div(id = 'oneGammaDecay', className = 'row'),
                ], className = 'two columns'),
#column two outputs
            html.Div([
                html.Div(id = 'twoTheo', className = 'row'),
                html.Div(id = 'twoIV', className = 'row'),
                html.Div(id = 'twoDelta', className = 'row'),                
                html.Div(id = 'twoGamma', className = 'row'),
                html.Div(id = 'twoVega', className = 'row'),
                html.Div(id = 'twoTheta', className = 'row'),
                html.Div(id = 'twovolTheta', className = 'row'),
                html.Div(id = 'twoFullDelta', className = 'row'),
                #html.Div(id = 'twoDeltaDecay', className = 'row'),
                #html.Div(id = 'twoVegaDecay', className = 'row'),
                #html.Div(id = 'twoGammaDecay', className = 'row'),
                ], className = 'two columns'),
#column three outputs
            html.Div([
                html.Div(id = 'threeTheo', className = 'row'),
                html.Div(id = 'threeIV', className = 'row'),
                html.Div(id = 'threeDelta', className = 'row'),                
                html.Div(id = 'threeGamma', className = 'row'),
                html.Div(id = 'threeVega', className = 'row'),
                html.Div(id = 'threeTheta', className = 'row'),
                html.Div(id = 'threevolTheta', className = 'row'),
                html.Div(id = 'threeFullDelta', className = 'row'),
                #html.Div(id = 'threeDeltaDecay', className = 'row'),
                #html.Div(id = 'threeVegaDecay', className = 'row'),
                #html.Div(id = 'threeGammaDecay', className = 'row'),
                ], className = 'two columns'),
#column four outputs
            html.Div([
                html.Div(id = 'fourTheo', className = 'row'),
                html.Div(id = 'fourIV', className = 'row'),
                html.Div(id = 'fourDelta', className = 'row'),                
                html.Div(id = 'fourGamma', className = 'row'),
                html.Div(id = 'fourVega', className = 'row'),
                html.Div(id = 'fourTheta', className = 'row'),
                html.Div(id = 'fourvolTheta', className = 'row'),
                html.Div(id = 'fourFullDelta', className = 'row'),
                #html.Div(id = 'fourDeltaDecay', className = 'row'),
                #html.Div(id = 'fourVegaDecay', className = 'row'),
                #html.Div(id = 'fourGammaDecay', className = 'row'),
                ], className = 'two columns'),
#stratgies outputs
            html.Div([
                #stratgies outputs
                html.Div(id = 'stratTheo', className = 'row'),
                html.Div(id = 'stratIV', className = 'row'),
                html.Div(id = 'stratDelta', className = 'row'),                
                html.Div(id = 'stratGamma', className = 'row'),
                html.Div(id = 'stratVega', className = 'row'),
                html.Div(id = 'stratTheta', className = 'row'),
                html.Div(id = 'stratvolTheta', className = 'row'),
                html.Div(id = 'stratFullDelta', className = 'row'),
                #html.Div(id = 'stratDeltaDecay', className = 'row'),
                #html.Div(id = 'stratVegaDecay', className = 'row'),
                #html.Div(id = 'stratGammaDecay', className = 'row'),

                ], className = 'two columns'),
        ],className= 'row'),

#], className= 'row')

              ], className = 'eight columns' )

hidden = html.Div([
    #hidden to store greeks from the 4 legs
        html.Div(id='oneCalculatorCalculatorData', style={'display':'none'}), 
        html.Div(id='twoCalculatorCalculatorData', style={'display':'none'}), 
        html.Div(id='threeCalculatorCalculatorData', style={'display':'none'}), 
        html.Div(id='fourCalculatorCalculatorData', style={'display':'none'}), 

        html.Div(id='trades_div', style={'display': 'none'}),
        html.Div(id='trade_div', style={'display': 'none'}),
        html.Div(id='trade_div2', style={'display': 'none'}),
        html.Div(id='productData', style={'display': 'none'})
    ], className = 'row')

actions = html.Div([
    html.Div([html.Button('Delete', id = 'delete', n_clicks_timestamp='0')], className = 'four columns'),
    html.Div([html.Button('Trade', id = 'trade')], className = 'four columns'),
    html.Div([html.Button('Report', id = 'report')], className = 'four columns')
    ], className = 'row')

tables = html.Div([
    html.Div([dtable.DataTable(id = 'tradesTable',
                              #rows=[{}],
                              data=[],
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
            {'id': 'Counterparty', 'name': 'Counterparty', 'type': 'dropdown' }

        ],
                            row_selectable=True,
                            editable=True,
                          column_static_dropdown=[
                              {
                                  'id': 'Counterparty',
                                  'dropdown': countrparties
                                  }
                              #                                                              dropdown_conditional=[
                              #{
                              #    'id': 'Counterparty',
                              #    'dropdown': countrparties
                              #    }
                              ]
                            
                            )], className = 'row')
    ], className = 'row')

sideMenu = html.Div([
        html.Div([dcc.Link('Home', href='/')], className = 'row'),
        html.Div([dcc.Dropdown(id='productCalc-selector', value =onLoadProductProducts()[1],  options =  onLoadProductProducts()[0])], className = 'row' ),
        html.Div([dcc.Dropdown(id='monthCalc-selector')], className = 'row' ),
        html.Div('Product:', className = 'row'),

        html.Div('Expiry:',className= 'row'),
        html.Div('expiry',id = 'calculatorExpiry',className= 'row'),
        html.Div('Third Wednesday:',className= 'row'),
        html.Div('3wed',id = '3wed',className= 'row')
    ], className = 'three columns')

output = html.Div([
    
   dcc.Markdown(id = 'reponseOutput')
   ])

layout = html.Div([
html.H3('Calculator'),
sideMenu,   
    calculator,
    tables,
    hidden,
    actions,
    output
    ])

@app.callback(Output('productData', 'children'),
              [Input('productCalc-selector', 'value')])
def updateSpread1(product):
    params = retriveParams(product.lower())
    if params:
        spread = params['spread']
        return spread

@app.callback(Output('calculatorSpread1', 'placeholder'),
              [Input('productCalc-selector', 'value'),
               Input('monthCalc-selector', 'value')])
def updateSpread1(product, month):
    product = product + 'O' + month
    params = retriveParams(product.lower())
    if params:
        spread = params['spread']
        return spread

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

#update expiry date
@app.callback(
    Output(component_id='calculatorExpiry', component_property='children'),
    [Input('productCalc-selector', 'value'), Input('monthCalc-selector', 'value')]  
)
def findExpiryDate(product, month):
    product = product + 'O' + month
    params = loadRedisData(product.lower())
    if params:
        #params = json.loads(params)

    return params['m_expiry']   
#update 3wed
@app.callback(
    Output(component_id='3wed', component_property='children'),
    [Input('calculatorExpiry', 'children') ]  
)
def find3Wed(expiry):
    expiry = dt.datetime.strptime(expiry, "%d/%m/%Y")
    wed = expiry + timedelta(days=14)
    wed = wed.strftime('%d/%m/%Y')
    return wed

@app.callback(Output('trades_div', 'children'),
                             [Input('buy', 'n_clicks_timestamp'),
                              Input('sell', 'n_clicks_timestamp'),
                              Input('delete', 'n_clicks_timestamp')],
                              #standard trade inputs
                             [State('tradesTable', 'selected_rows'),
                              State('tradesTable', 'data'),
                              State('calculatorVol_price', 'value'),
                              State('trades_div' , 'children'),
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
                              State('calculatorForward' , 'placeholder')
                                 ]
        )
def stratTrade(buy, sell, delete,
              clickdata, rows, pricevola, data, product, month, qty, strat,
              onestrike, ponestrike, onecop, onetheo, oneiv, onedelta, onegamma, onevega, onetheta, 
              twostrike, ptwostrike, twocop, twotheo, twoiv, twodelta, twogamma, twovega, twotheta, 
              threestrike, pthreestrike, threecop, threetheo, threeiv, threedelta, threegamma, threevega, threetheta,
              fourstrike, pfourstrike, fourcop, fourtheo, fouriv, fourdelta, fourgamma, fourvega, fourtheta,
             expiry, wed, forward, pforward):
    #reset buy sell signal
    bs = 0

    #convert qty to float to save multiple tims later
    qty = float(qty)

    #build product from month and product dropdown
    if product and month:
        product = product + 'O' + month
        #prevent error from empty inputs on page load
        if (int(buy) + int(sell)+ int(delete)) == 0:
            raise PreventUpdate
        else:
        #load trades from hidden div
            if data:
                trades = json.loads(data)   
            else: trades = {}

            #on delete work over indices and delete rows then update trades dict
            if int(delete) > int(buy) and int(delete) > int(sell):
                if clickdata:
                    for i in clickdata:
                        instrument = rows[i]['Instrument']
                        trades.pop(instrument, None)

            else:
                #not delete so see ifs its a buy/sell button click
                #create name then go over buy/sell and action

                #find the stat mults
                statWeights = stratConverstion[strat]
                #clac forward and prompt
                Bforward, Aforward = placholderCheck(forward, pforward)
                prompt = str(dt.datetime.strptime(expiry[:10], "%d/%m/%Y") + timedelta(days=14) )[:10] 
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
                        #get strike from value and placeholder
                        onestrike = strikePlaceholderCheck(onestrike, ponestrike)

                        weight = statWeights[0]*bs
                        name = str(product) + ' '+ str(onestrike) + ' '+ str(onecop).upper()
                        trades[name] = {'qty': qty*weight, 'theo': float(onetheo), 'prompt' : prompt, 'forward': Bforward, 'iv': float(oneiv)*weight, 'delta': float(onedelta)*weight*qty, 'gamma': float(onegamma)*weight*qty, 'vega': float(onevega)*weight*qty, 'theta': float(onetheta)*weight*qty, 'counterparty': '' } 
                        #add delta to delta bucket for hedge
                        deltaBucket += float(onedelta)*weight*qty

                    #calc two leg weight
                    if statWeights[1] !=0:
                        #get strike from value and placeholder
                        twostrike = strikePlaceholderCheck(twostrike, ptwostrike)

                        weight = statWeights[1]*bs
                        name = str(product) + ' '+ str(twostrike) + ' '+ str(twocop).upper()
                        trades[name] = {'qty': float(qty)*weight, 'theo': float(twotheo), 'prompt' : prompt, 'forward': Bforward, 'iv': float(twoiv)*weight, 'delta': float(twodelta)*weight*qty, 'gamma': float(twogamma)*weight*qty, 'vega': float(twovega)*weight*qty, 'theta': float(twotheta)*weight*qty, 'counterparty': '' }  
                        #add delta to delta bucket for hedge
                        deltaBucket += float(twodelta)*weight*qty

                    #calc three leg weight
                    if statWeights[2] !=0:
                        #get strike from value and placeholder
                        threestrike = strikePlaceholderCheck(threestrike, pthreestrike)

                        weight = statWeights[2]*bs
                        name = str(product) + ' '+ str(threestrike) + ' '+ str(threecop).upper()
                        trades[name] = {'qty': float(qty)*weight, 'theo': float(threetheo), 'prompt' : prompt, 'forward': Bforward, 'iv': float(threeiv)*weight, 'delta': float(threedelta)*weight*qty, 'gamma': float(threegamma)*weight*qty, 'vega': float(threevega)*weight*qty, 'theta': float(threetheta)*weight*qty, 'counterparty': '' }     
                        #add delta to delta bucket for hedge
                        deltaBucket += float(threedelta)*weight*qty

                    #calc four leg weight
                    if statWeights[3] !=0:
                        #get strike from value and placeholder
                        fourstrike = strikePlaceholderCheck(fourstrike, pfourstrike)

                        weight = statWeights[3]*bs
                        name = str(product) + ' '+ str(fourstrike) + ' '+ str(fourcop).upper()
                        trades[name] = {'qty': float(qty)*weight, 'theo': float(fourtheo), 'prompt' : prompt, 'forward': Bforward, 'iv': float(fouriv)*weight, 'delta': float(fourdelta)*weight*qty, 'gamma': float(fourgamma)*weight*qty, 'vega': float(fourvega)*weight*qty, 'theta': float(fourtheta)*weight*qty, 'counterparty': '' }  
                        #add delta to delta bucket for hedge
                        deltaBucket += float(fourdelta)*weight*qty
                    #if vol trade then add hedge along side
                    if pricevola == 'vol':
                        delta = round(float(deltaBucket),0)*-1

                        hedge = {'qty':delta , 'theo': Bforward, 'prompt' : prompt, 'forward': Bforward, 'iv': 0, 'delta': delta, 'gamma': 0, 'vega': 0, 'theta': 0, 'counterparty': '' }
                        if futureName in trades:
                           trades[futureName]['qty'] = trades[futureName]['qty'] +  hedge['qty']
                        else: trades[futureName] =  hedge

            return json.dumps(trades)

@app.callback(Output('tradesTable', 'data'),
              [Input('trades_div', 'children')])
def loadTradeTable(data):
    if data != None:
        data = json.loads(data)
        trades = buildTradesTableData(data)
        return trades.to_dict('rows')
    else:
        return 'No Data'

#reset row indices on delete
@app.callback(Output('tradesTable', 'selected_rows'),
              [Input('trades_div', 'children')])
def clearSelectedRows(clicks):
    return []

#send trade to system
@app.callback(Output('trade_div', 'children'),
              [Input('trade', 'n_clicks')],
              [State('tradesTable', 'selected_rows'),
               State('tradesTable', 'data') ])
def sendTrades(clicks, indices, rows):
    timestamp= timeStamp()
    user = request.authorization['username']
    if indices:
        for i in indices:
            #create st to record which products to update in redis 
            redisUpdate = set([])   

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
                trade.id = sendTrade(trade)
                updatePos(trade)
            elif rows[i]['Instrument'][3] ==' ':
                #is futures
                product = rows[i]['Instrument'][:3]
                redisUpdate.add(product)
                prompt = rows[i]['Prompt']
                price = rows[i]['Forward']
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

#send trade to F2 and exchange
@app.callback(Output('reponseOutput', 'children'),
              [Input('report', 'n_clicks')],
              [State('tradesTable', 'selected_rows'),
               State('tradesTable', 'data') ])
def sendTrades(clicks, indices, rows):
    #string to hold router respose
    tradeResponse = '## Response'
    
    #find user related trade details 
    timestamp= timeStamp()
    user = request.authorization['username']

    if indices:
        for i in indices:
            if rows[i]['Instrument'][3] =='O':
                #is option
                product = rows[i]['Instrument'][:6]
                strike = rows[i]['Instrument'][7:11]
                CoP = rows[i]['Instrument'][12:13]
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

                #store action for auditing
                storeTradeSend(trade,response)

                response = responseParser(response)

                #attached reposne to print out
                tradeResponse = tradeResponse +'\n' + response

            elif rows[i]['Instrument'][3] ==' ':
                #is futures
                product = rows[i]['Instrument'][:3]
                prompt = rows[i]['Prompt']
                price = rows[i]['Forward']
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

                #store action for auditing
                storeTradeSend(trade,response)

                response = responseParser(response)

                #attached reposne to print out
                tradeResponse = tradeResponse +'\n' + response

        return tradeResponse

def responseParser(response):
    
    return 'Status: {} Error: {}'.format(response['Status'], response['ErrorMessage'])

#pull 3m from product data  
@app.callback(Output('calculatorBasis', 'placeholder'),
              [Input('productCalc-selector', 'value'), Input('monthCalc-selector', 'value')  ])
def loadBasis(product, month):
    product = product + 'O' + month
    data = loadRedisData(product.lower())
    if data != None:
        data = json.loads(data)
        data  = str(data['3m_und'])
        return data
    else : return str(0)  

 #clear inputs on product change 
@app.callback(Output('calculatorBasis', 'value'),
              [Input('productCalc-selector', 'value'), Input('monthCalc-selector', 'value')])
def loadBasis(product, month):
 return '' 

@app.callback(Output('calculatorForward', 'value'),
              [Input('productCalc-selector', 'value'), Input('monthCalc-selector', 'value')])
def loadBasis(product, month):
 return '' 

@app.callback(Output('interestRate', 'value'),
              [Input('productCalc-selector', 'value'), Input('monthCalc-selector', 'value')])
def loadBasis(product, month):
 return '' 

@app.callback(Output('calculatorSpread1', 'value'),
              [Input('productCalc-selector', 'value'), Input('monthCalc-selector', 'value')])
def loadBasis(product, month):
 return '' 

@app.callback(Output('calculatorPrice/Vola', 'value'),
              [Input('productCalc-selector', 'value'), Input('monthCalc-selector', 'value')])
def loadBasis(product, month):
 return '' 

@app.callback(Output('interestRate', 'placeholder'),
              [Input('3wed', 'children')])
def loadBasis(expiry):
    data = loadRedisData('USDRate')
    expiry = expiry.split("/")
    expiry = expiry[2]+expiry[1]+expiry[0]
    if data != None:
        data = json.loads(data)
        data  = float(data[expiry]['Interest Rate'])*100
        return str(data)
    else : return str(0)  

@app.callback(Output('calculatorForward', 'placeholder'),
              [Input('calculatorBasis', 'value'),
               Input('calculatorBasis', 'placeholder'),
               Input('calculatorSpread1', 'value'),
               Input('calculatorSpread1', 'placeholder')
              ])
def loadBasis(basis, pbasis, spread, pspread):
    if not basis: basis = pbasis
    if not spread: spread = pspread
    if basis and spread:
        basis = str(basis).split('/')
        spread = str(spread).split('/')
        if len(basis) >1 and len(spread)>1:
            bid = float(basis[0]) + float(spread[0])
            ask = float(basis[1]) + float(spread[1])
            return str(bid) +'/'+ str(ask) 
        elif len(spread)>1:
            bid = float(basis[0]) + float(spread[0])
            ask = float(basis[0]) + float(spread[1])
            return str(bid) +'/'+ str(ask) 
        elif  len(basis) >1:
            bid = float(basis[0]) + float(spread[0])
            ask = float(basis[1]) + float(spread[0])
            return str(bid) +'/'+ str(ask) 
        spread2 = float(basis[0]) + float(spread[0])
        return str(spread2) 
    else: return 0

def placholderCheck(value, placeholder):
    if value and value != None and value != ' ':
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
    def updateDropdown(product, month):
        product = product + 'O' + month
        strikes = fetechstrikes(product)
        length = int(len(strikes)/2)
        value = strikes[length]['value']
        return value
    return updateDropdown

#create vola function
def buildUpdateVola(leg):
    def updateVola(product, month, strike, pStrike, cop, priceVol):
            #get strike from strike vs pstrike
            strike = str(int(placholderCheck(strike, pStrike)[0]))

            if strike:            
                product = product + 'O' + month
                params = loadRedisData(product.lower())

                if params:
                    params = json.loads(params)
                    #if strike is real strike
                    if strike in params['strikes']:
                        if priceVol == 'vol':
                            vola = float(params['strikes'][strike][cop.upper()]['vola'])
                            if type(vola) == float:
                                return str(round(vola*100,2))
                            else: 
                                return ' '

                        elif priceVol == 'price':
                            return params['strikes'][strike][cop.upper()]['theo']
            else: return 0
    return updateVola

def buildvolaCalc(leg):
    def volaCalc( expiry, nowOpen, rate, prate, forward, pforward, strike, pStrike, cop, priceVola, ppriceVola, volprice, days):


        #get inputs placeholders vs values
        strike = str(int(placholderCheck(strike, pStrike)[0]))
        Brate, Arate = placholderCheck(rate, prate)
        Bforward, Aforward = placholderCheck(forward, pforward)
        BpriceVola, ApriceVola  = placholderCheck(priceVola, ppriceVola)
 
        if None not in (expiry, Bforward, Aforward, BpriceVola, ApriceVola, strike, cop):
            if nowOpen == 'now':
                now = True
            else: now = False
            today = dt.datetime.today()           
            if volprice == 'vol':
            
                option = Option(cop,Bforward,strike,today,expiry,Brate/100,BpriceVola/100, days = days, now=now )
                Bgreeks = option.get_all()

                return {'bid':Bgreeks, 'Bvol': BpriceVola}
                #return json.dumps({'bid':Bgreeks, 'Bvol': BpriceVola})

            elif volprice == 'price':
                option = Option(cop,Bforward,strike,today,expiry,Brate/100,0,price = BpriceVola,  days = days, now = now)
                option.get_impl_vol()
                Bvol = option.vol
                Bgreeks = list(option.get_all())
                Bgreeks[0]= BpriceVola

                return {'bid':Bgreeks,  'Bvol': Bvol*100}            
                #return json.dumps({'bid':Bgreeks,  'Bvol': Bvol*100})   
            
    return volaCalc

def createLoadParam(param):
    def loadtheo(params):
        #pull greeks from stored hidden
        if params !=None:
            
            #params = json.loads(params)
            
            return  str("%.4f" % params['bid'][param[1]])
    return loadtheo

def buildVoltheta():
   
    def loadtheo(vega, theta):
        if vega !=None and theta != None:
            vega = float(vega)
            if vega>0:
                return "%.2f" % (float(theta) / vega)
            else: return 0
        else: return 'n/a'  
    return loadtheo

def buildTheoIV():
    def loadIV(params):
        if params !=None:
            #params = json.loads(params)
            return  str("%.4f" % params['Bvol'])
        else: return 'n/a'    
    return loadIV

#create placeholder function for each {leg}Strike
for leg in legOptions:
        app.callback(Output('{}Strike'.format(leg), 'placeholder'),
            [Input('productCalc-selector', 'value'), Input('monthCalc-selector', 'value')])     (buildFetchStrikes())

        app.callback(Output('{}Vol_price'.format(leg), 'placeholder'),
              [Input('productCalc-selector', 'value'),
               Input('monthCalc-selector', 'value'),
               Input('{}Strike'.format(leg), 'value'),
               Input('{}Strike'.format(leg), 'placeholder'),
               Input('{}CoP'.format(leg), 'value'),
               Input('calculatorVol_price', 'value')
               ]) (buildUpdateVola(leg))

        app.callback(
                Output('{}CalculatorCalculatorData'.format(leg), 'children'),
                [Input(component_id='calculatorExpiry', component_property='children'),
                 Input(component_id='nowOpen', component_property='value'),
                 Input(component_id='interestRate', component_property='value'),  
                 Input(component_id='interestRate', component_property='placeholder'),
                 Input(component_id='calculatorForward', component_property='value'),  
                 Input(component_id='calculatorForward', component_property='placeholder'),
                 Input(component_id='{}Strike'.format(leg), component_property='value'),
                 Input(component_id='{}Strike'.format(leg), component_property='placeholder'),
                 Input(component_id='{}CoP'.format(leg), component_property='value'),
                 Input(component_id='{}Vol_price'.format(leg), component_property='value'),
                 Input(component_id='{}Vol_price'.format(leg), component_property='placeholder'),
                 Input(component_id='calculatorVol_price', component_property='value'),
                 Input(component_id='dayConvention', component_property='value')
                 ]
                )(buildvolaCalc(leg))

        #calculate the vol thata from vega and theta
        app.callback(
            Output('{}volTheta'.format(leg), 'children'),
            [Input('{}Vega'.format(leg), 'children'), Input('{}Theta'.format(leg), 'children')]
            ) (buildVoltheta())

        #add callbacks that fill in the IV 
        app.callback(
            Output('{}IV'.format(leg), 'children'),
            [Input('{}CalculatorCalculatorData'.format(leg), 'children') ]  
        )(buildTheoIV())


        #add different greeks to leg and calc
        for param in [['Theo', 0],
                     ['Delta',  1],
                     ['Gamma', 3],
                     ['Vega',  4],
                     ['Theta', 2],
                     ['FullDelta', 11]
                     ]:
  
            app.callback(Output('{}{}'.format(leg,param[0]), 'children'),
                [Input('{}CalculatorCalculatorData'.format(leg), 'children') ]) (
                    createLoadParam(param))

def buildStratGreeks():
    def stratGreeks(strat, one, two, three, four):
        strat = stratConverstion[strat]
        greek = (strat[0] * float(one)) + (strat[1] * float(two)) + (strat[2] * float(three)) + (strat[3] * float(four))
        greek= round(greek,2)
        return str(greek)
    return stratGreeks

#add different greeks to leg and calc
for param in ['Theo', 
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

