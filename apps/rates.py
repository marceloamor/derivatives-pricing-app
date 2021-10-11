from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_core_components as dcc
from datetime import datetime as dt
import dash_table as dtable
import plotly.figure_factory as ff
import plotly.graph_objs as go
import pandas as pd
import time
import numpy as np

from sql import pulltrades, sendTrade
from parts import pullRates

from app import app, topMenu

interval = str(750)

graph = html.Div([
    dcc.Graph(id= "usd"),
    dcc.Interval(id='live-update', interval=interval)
    ], className = 'row')

layout = html.Div([
    topMenu('Rates'),
    html.Div([dcc.Link('Home', href='/')], className = 'row'),
    graph
    ])


#update graphs on data update
@app.callback(
    Output(component_id='usd', component_property='figure'),
    [Input('live-update', 'interval')]
)
def load_param_graph(interval):

    rates = pullRates('USD')
    #sort data on date and adjust current dataframe
    rates.sort_index(inplace=True)
    
    #find the axis values
    rates.index = pd.to_datetime(rates.index)
    index = rates.index
    forwardDate = list(index)
    price = np.array(rates['Interest Rate'])
    #build scatter graph pd.to_datetime([dates)
    figure = go.Figure(
        data=[
            go.Scatter(x= forwardDate, y=price, mode='lines', hoveron='points', 
                      line = dict(
        color = ('rgb(22, 96, 167)'),
        width = 2,))
        ],
        layout=go.Layout(
            title= 'Rates',
            showlegend=False
        )
    )
    return figure