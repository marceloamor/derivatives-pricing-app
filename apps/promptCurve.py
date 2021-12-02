from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
from datetime import datetime as dt
import dash_table as dtable
import plotly.figure_factory as ff
import plotly.graph_objs as go
import pandas as pd
import numpy as np

from parts import loadStaticData, pullPrompts, onLoadPortFolio

from app import app, topMenu
#1 second interval
interval = 1000*1

def generate_table(dataframe, max_rows=75):
    '''Given dataframe, return template generated using Dash components
    '''
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns[::-1]])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns[::-1]
        ]) 
         for i in range(min(len(dataframe), max_rows))]
    , style={'background':'#fff'})

graph = dbc.Col([
    dcc.Graph(id= "prompt-curve"),
    dcc.Interval(id='live-update', interval=interval)
    ])

#column options for prompt table 
columns = [{"name": 'Forward', "id": 'forward_date'}, 
           {"name": 'Price', "id": 'price'},
             {"name": 'Spread', "id": 'spread'},
             {"name": 'Position', "id": 'position'},
             {"name": 'Open Position', "id": 'open_position'},
             {"name": 'Option Position', "id": 'opt position'},
             {"name": 'Total', "id": 'total delta'},
             {"name": 'Cumlative', "id": 'cumlative'}
           ]

tables = dbc.Col([
                dtable.DataTable(id ='prompt-table',
                    columns = columns,
                    data = [{}],
                    fixed_rows={'headers': True},
                    )]            
                  , width = 12
                     )
options = dbc.Col([dcc.Dropdown(id='portfolio-selector', value='copper', options =  onLoadPortFolio())], width = 4)

submit = dbc.Col([html.Button('Submit Spreads', id = 'SumbitSpreads')], width = 4)
    
layout = html.Div([
    topMenu('Prompt Curve'),
    
    dbc.Row([options]),
    dbc.Row([graph]),
        #table holder
    dbc.Row([tables],),
        #table option holder                
     dbc.Row([submit])
    ])

#update graphs on data update
@app.callback(
    Output('prompt-curve', 'figure'),
    [Input('prompt-table', 'data')]
)
def load_prompt_graph(rates):

    #pull prompt curve
    rates = pd.DataFrame.from_dict(rates).head(100)
    print(rates)
    #find the axis values

    index = pd.to_datetime(rates['forward_date'], format='%Y%m%d')
    forwardDate = list(index)
    price = np.array(rates['price'])
    position = np.array(rates['position'])
    total = np.array(rates['total delta'])
    cumlative = np.array(rates['cumlative'])
    optionDelta = np.array(rates['opt position'])
    #build scatter graph pd.to_datetime([dates)
    trace1 = go.Scatter(x= forwardDate, y=price, mode='lines', hoveron='points', name = 'Price', visible='legendonly',
                    line = dict(
    color = ('rgb(22, 96, 167)'),
    width = 2,))
    trace2 = go.Bar(x= forwardDate, y=position, yaxis='y2', name = 'Futures', visible='legendonly')
    trace5 = go.Bar(x= forwardDate, y=optionDelta, yaxis='y2',  name = 'Options Delta', visible='legendonly')
    trace3 = go.Scatter(x= forwardDate, y=cumlative, mode='lines', yaxis='y2', hoveron='points', name = 'Cumlative',
                    line = dict(
    color = ('rgb(220, 244, 66)'),
    width = 2,))
    trace4 = go.Bar(x= forwardDate, y=total, yaxis='y2', name = 'Total')

    figure = go.Figure(

        data=[trace1, trace2, trace3, trace4, trace5],

        layout = go.Layout(
            title='Prompt Curve',
            
            yaxis=dict(
                title='Price'
            ),
            yaxis2=dict(
                title='Position',
                titlefont=dict(
                    color='rgb(148, 103, 189)'
                ),
                tickfont=dict(
                    color='rgb(148, 103, 189)'
                ),
                overlaying='y',
                side='right'
                    ),
            xaxis = dict(title = 'Date')
)
    )
    return figure

#update table on data update
@app.callback(
    Output('prompt-table', 'data'),
    [Input('portfolio-selector', 'value')]
)
def load_prompt_table(portfolio):

    #pull prompt curve
    rates = pullPrompts(portfolio)
    #remove underlying column
    rates.drop(['underlying'], axis =1, inplace=True)
    print(portfolio)
    
    rates['forward_date'] = rates.index
    rates = rates.round(2)
    return rates.to_dict('records')


    




