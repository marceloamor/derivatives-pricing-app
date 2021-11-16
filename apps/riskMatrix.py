from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_core_components as dcc
import plotly.figure_factory as ff
import dash_bootstrap_components as dbc
import dash_table as dtable
import pandas as pd
from pandas.plotting import table 
import datetime as dt
from datetime import datetime
import requests, math, ast, os
import plotly.graph_objs as go
import plotly

from app import app, topMenu
from data_connections import riskAPi
from parts import onLoadPortFolio, heatunpackRisk, loadStaticData, unpackRisk, heampMapColourScale, productsFromPortfolio, curren3mPortfolio, unpackPriceRisk, sendEmail

#production port
baseURL = "http://{}:5000/RiskApi/V1/risk".format(riskAPi)

undSteps = {
    'aluminium':'10',
    'copper':'40',
    'nickel':'100',
    'zinc':'10',
    'lead':'10',
    }

def buildURL(base,portfolio, und, vol, level, eval, rels):
    und = 'und='+ str(und)[1:-1]
    vol = 'vol=' +str(vol)[1:-1]
    level = 'level='+level
    portfolio = 'portfolio='+ portfolio
    rels = 'rel=' + rels   
    eval = 'eval='+ eval

    url = base+'?'+portfolio+'&'+vol+'&'+und+'&'+level+'&'+eval+'&'+rels
    return url

options = dbc.Row([
    dbc.Col([
        dbc.Row([
            dbc.Col([
                            dcc.Dropdown('riskPortfolio',
                                    options=onLoadPortFolio(),
                                    value = 'copper')
                          ])  
            ]),
        dbc.Row([
            dbc.Col([
                dcc.Dropdown('riskType',
                                    options=[
                                        {'label': 'Full Delta', 'value': 'fullDelta'},
                                        {'label': 'Delta', 'value': 'delta'},
                                        {'label': 'Vega', 'value': 'vega'},
                                        {'label': 'Gamma', 'value': 'gamma'},
                                        {'label': 'Delta Decay', 'value': 'deltaDecay'},
                                        {'label': 'Vega Decay', 'value': 'vegaDecay'},
                                        {'label': 'Gamma Decay', 'value': 'gammaDecay'}
                                    ],
                                    value = 'fullDelta')
           ]) 
            ])
        ], width = 2),

    dbc.Col([
        html.Div(['Underlying Step Size']),
        html.Div([dcc.Input(id='stepSize', type='number')])
        
        ], width =2),
    dbc.Col([
        html.Div(['Volatility Step Size']),
        html.Div([dcc.Input(id='VstepSize', placeholder = 1, type='number')])
        
        ], width = 2),
    dbc.Col([
        html.Div(['Absolute/Relative']),
                            dcc.Dropdown('abs/rel',
                                    [
                                        {'label': 'Absolute', 'value': 'abs'},
                                        {'label': 'Relative', 'value': 'rel'}
                                        ],
                                    value = 'abs')        
        ], width = 2),
    dbc.Col([
        html.Div(['Evaluation Date']),
        html.Div([        dcc.DatePickerSingle(
            id = 'evalDate',
            month_format='MMMM Y',
            placeholder='MMMM Y',
            date=dt.datetime.today()
)]),

        ], width = 2),

    dbc.Col([
        html.Button('Email', id= 'email')     
        ], width = 2)


    ])

priceMatrix = dbc.Row([
    dbc.Col([
    dcc.Loading(id="loading-2",
                 type="circle", children = [
                     dtable.DataTable(id='priceMatrix', data =[{}])
              
                     ])
    ])
    ])

heatMap = dbc.Row([
    dbc.Col([
    dcc.Loading(id="loading-1",
                 type="circle", children = [
     dcc.Graph(id= 'heatMap')])                    
                     ])
])

hidden = dbc.Row([
    dcc.Store(id='riskData')   
    ])

layout = html.Div([
    topMenu('RISK MATRIX'),
    options,
    priceMatrix,
    heatMap,
    hidden
            ])

def placholderCheck(value, placeholder):
    if value and value != None:
        return float(value)

    elif placeholder and placeholder != None:
        return float(placeholder)

#populate data
@app.callback(
    Output('riskData', 'data'),
    [Input('riskPortfolio', 'value'),
    Input('stepSize', 'placeholder'),
    Input('stepSize', 'value'),
    Input('VstepSize', 'placeholder'),
    Input('VstepSize', 'value'),
    Input('evalDate', 'date'),
    Input('abs/rel', 'value')
    ]
)
def load_data(portfolio, stepP, stepV, vstepP, vstepV, eval, rels):
    list = [-5,-4,-3,-2,-1,0,1,2,3,4,5]
    step = placholderCheck(stepV, stepP)
    vstep = placholderCheck(vstepV, vstepP)/100    

    eval = datetime.strptime(eval[:10], '%Y-%m-%d')
    eval = datetime.strftime(eval, '%d/%m/%Y')

    if step:
        und = [x * step for x in list]
        vol =  [x * vstep for x in list]
        url = buildURL(baseURL, portfolio, und, vol, 'high', eval, rels)
        myResponse = requests.get(url)
   
        if(myResponse.ok):        
            messageContent = myResponse.content
            return ast.literal_eval(messageContent.decode('utf-8'))
        else:
          # If response code is not ok (200), print the resulting http error code with description
            print(myResponse.raise_for_status())

@app.callback(
    Output('heatMap', 'figure'),
    [Input('riskType', 'value'), Input('riskData', 'data')],
    [State('stepSize', 'placeholder'),
    State('stepSize', 'value'),
    State('VstepSize', 'placeholder'),
    State('VstepSize', 'value'),
    State('riskPortfolio', 'value')]
)
def load_data(greek, data, stepP, stepV, vstepP, vstepV, portfolio):
    #find und/vol step from placeholder/value
    step = placholderCheck(stepV, stepP)
    vstep = placholderCheck(vstepV, vstepP)
    
    if data:
        #json data and unapck into heatmap dict
        #data = json.loads(data)
  
        #uun pack then re pack data into the required frames
        jdata, underlying, volaility = heatunpackRisk(data, greek)

        #convert underlying in to absolute from relataive
        tM = curren3mPortfolio(portfolio.lower())
        underlying = [float(x) + tM for x in underlying]

        #build anotaions
        annotations = []
        z =jdata
        y=underlying
        x = volaility
        for n, row in enumerate(z):
            for m, val in enumerate(row):
                annotations.append(go.layout.Annotation(text=str(z[n][m]), x=x[m], y=y[n],
                                                 xref='x1', yref='y1', showarrow=False, 
                     font=dict(
                            color='white'
                                    )        
                                     ))
        #build traces to pass to heatmap
        trace = go.Heatmap(x=x, y=y, z=z, colorscale=heampMapColourScale, showscale=False)

        fig = go.Figure(data=([trace]))
        fig.layout.annotations = annotations
        fig.layout.yaxis.title = 'Underlying ($)'
        fig.layout.xaxis.title = 'Volatility (%)'
        fig.layout.xaxis.tickmode='linear'
        fig.layout.xaxis.dtick=vstep
        fig.layout.yaxis.dtick=step
        return fig

@app.callback(
    [Output('priceMatrix', 'data'),
     Output('priceMatrix', 'columns'),
     Output('priceMatrix', 'style_data_conditional')],
    [Input('riskData', 'data')],
    [State('riskPortfolio', 'value')]
)
def load_data(data, portfolio):
    if data:        

        tm = curren3mPortfolio(portfolio.lower())
        data = unpackPriceRisk(data, tm)
        columns=[{'name': str(i), 'id': str(i)} for i in data[0]]

        #find middle column to highlight later
        middleColumn = columns[6]['id']
        style_data_conditional=[{
                                'if': {'column_id': middleColumn},
                                'backgroundColor': '#3D9970',
                                'color': 'white',
                                }]
   
        return data, columns, style_data_conditional

#rounding function for stepSize
def roundup(x):
    return int(math.ceil(x / 5.0)) * 5

#filled in breakeven on product change
@app.callback(
    Output('stepSize', 'placeholder'),
    [Input('riskPortfolio', 'value')]
)
def pullStepSize(portfolio):

    return undSteps[portfolio.lower()]

#send email when button is pressed
@app.callback(
    Output('email', 'n_clicks_timestamp'),
    [Input('email', 'n_clicks'), Input('heatMap', 'figure'), Input('priceMatrix', 'data')]
)
def sendEmail(click, fig, data):
    if click:
        figure = go.Figure(fig)
        if not os.path.exists(r"P:\Options Market Making\LME\images"):
            os.mkdir(r"P:\Options Market Making\LME\images")
        #save heat map image to p drive folders
        plotly.io.write_image(figure, r'P:\Options Market Making\LME\images\fig1.jpeg', format='jpeg')

        #load price matrix data into pandas then save as image
        df = pd.DataFrame.from_dict(data)
        #rearrange columns so greek names first
        cols = df.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        df =df[cols]
        #save to html file
        df.to_html(r'P:\Options Market Making\LME\images\data1.html')

        sendEmail(product)




 #clear inputs on product change 

@app.callback(Output('stepSize', 'value'),
              [Input('riskPortfolio', 'value')])
def loadBasis(product):
 return '' 

 #clear inputs on product change 

@app.callback(Output('VstepSize', 'value'),
              [Input('riskPortfolio', 'value')])
def loadBasis(product):
 return '' 
