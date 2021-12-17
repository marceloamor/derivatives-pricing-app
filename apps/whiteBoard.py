from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_core_components as dcc
from datetime import datetime as dt
import dash_table as dtable
from flask import request

from parts import topMenu, timeStamp, sendMessage, pullMessages

interval = str(2000)

COLORS = [
    {
        'background': '#fef0d9',
        'text': 'rgb(30, 30, 30)'
    },
    {
        'background': '#fdcc8a',
        'text': 'rgb(30, 30, 30)'
    },
    {
        'background': '#fc8d59',
        'text': 'rgb(30, 30, 30)'
    },
    {
        'background': '#d7301f',
        'text': 'rgb(30, 30, 30)'
    },]

#trades table layout
text_table = html.Div([
    dtable.DataTable(id='textLines', data=[{}],
                             row_selectable=False,
                             #filterable=True,
                             #sortable=False,
                             editable=False,
                             
                             )
        ] ,className = 'row')

#hold message entry page
messageEntry = html.Div([
    html.Div([
            dcc.Textarea(
                id= 'textMessage',
            placeholder='Message...',
            style={'width': '100%'}
        )
        ], className = 'row'),

    html.Div([
        html.Div([html.Button('Send', id = 'send', n_clicks_timestamp='0')], className = 'two columns'),
        ]),
    html.Div(id= 'messageOutput')
    ], className ='row')

layout = html.Div([
    topMenu('Order Whiteboard'),
    dcc.Interval(id='wblive-update', interval=interval),
    html.Div([dcc.Link('Home', href='/')], className = 'row'),
    messageEntry,
    text_table
    ])

def initialise_callbacks(app):
    #pull logs
    @app.callback(
        Output('textLines','rows'), 
        [Input('wblive-update', 'n_intervals')])
    def update_messages(interval):
        print('trigger')
        return pullMessages()
        
    #send message to redis
    @app.callback(
        Output('messageOutput','children'),
    [Input('send', 'n_clicks')],
    [State('textMessage', 'value')])
    def sendWhiteboardMessage(clicks, text):
        if clicks:
            user = request.authorization['username']
            messageTime = timeStamp()

            sendMessage(text, user, messageTime)

