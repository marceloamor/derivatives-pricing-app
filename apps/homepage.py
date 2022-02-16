from dash.dependencies import Input, Output
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_table as dtable
from datetime import datetime as datetime
from datetime import date
from datetime import timedelta
from dash import no_update
import json

from parts import topMenu, pullPortfolioGreeks
from data_connections import conn

columns = [ {"name": 'Portfolio', "id": 'portfolio'}, 
            {"name": 'Delta', "id": 'total_delta'},
            {"name": 'Full Delta', "id": 'total_fullDelta'},
            {"name": 'Vega', "id": 'total_vega'},
            {"name": 'Theta', "id": 'total_theta'},
            {"name": 'Gamma', "id": 'total_gamma'},
            {"name": 'Delta Decay', "id": 'total_deltaDecay'},
            {"name": 'Vega Decay', "id": 'total_vegaDecay'},
            {"name": 'Gamma Decay', "id": 'total_gammaDecay'}
           ]           

jumbotron = dbc.Jumbotron(
    [
        html.H1("Georgia", className="display-3"),
        html.P(
            "Welcome to Georgia your specialised LME "
            "risk and pricing system.",
            className="lead",
        ),
        html.Hr(className="my-2"),
        html.P(
            "Lets get trading!!"

        ),
        html.P(dbc.Button("Learn more", color="primary"), className="lead"),
    ])

totalsTable = dbc.Row([
    dbc.Col([
  dtable.DataTable(id='totals',
                columns = columns,
               data = [{}],
               style_data_conditional=[
                            {
                                'if': {'row_index': 'odd'},
                                'backgroundColor': 'rgb(248, 248, 248)'
                            }])  
    ])])

badges = dbc.Row(
    [
        dbc.Col([dbc.Badge("Vols", id='vols', pill=True, color="success", className="ms-1")]),
        dbc.Col([dbc.Badge("FCP", id='fcp',pill=True, color="success",  className="ms-1")]),
        dbc.Col([dbc.Badge("INR", id='inr',pill=True, color="success",  className="ms-1")]),
        dbc.Col([dbc.Badge("EXR", id='exr',pill=True, color="success", className="ms-1")]),
        dbc.Col([dbc.Badge("NAP", id='nap',pill=True, color="success", className="ms-1")]),
        dbc.Col([dbc.Badge("SMP", id='smp',pill=True, color="success", className="ms-1")]),
        dbc.Col([dbc.Badge("TCP", id='tcp',pill=True, color="success",  className="ms-1")]),
        dbc.Col([dbc.Badge("CLO", id='clo',pill=True, color="success", className="ms-1")]),
        dbc.Col([dbc.Badge("ACP", id='acp',pill=True, color="success", className="ms-1")]),
        dbc.Col([dbc.Badge("SCH", id='sch',pill=True, color="success", className="ms-1")]),

    ]
)

files = ['vols', 'fcp', 'inr', 'exr', 'nap', 'smp', 'tcp', 'clo', 'acp', 'sch']

layout = html.Div([
    dcc.Interval(id='live-update', 
                 interval=1*1000, # in milliseconds
                 n_intervals=0),
        dcc.Interval(id='live-update2', 
                 interval=360*1000, # in milliseconds
                 n_intervals=0),             
topMenu('Home'),
html.Div([
    jumbotron
    ]),
totalsTable,
badges
])

def initialise_callbacks(app):
    #pull totals
    @app.callback(
        Output('totals','data'), 
        [Input('live-update', 'n_intervals')]
        )
    def update_greeks(interval):
        try:
            dff = pullPortfolioGreeks()    
            dff = dff.groupby('portfolio').sum()   
            dff['portfolio'] = dff.index       
            return dff.round(3).to_dict('records')
        except Exception as e:
            return no_update

    @app.callback(
        [Output('{}'.format(file),'color') for file in files], 
        [Input('live-update2', 'n_intervals')]
        )
    def update_greeks(interval):
        color_list = ['sucess' for i in files]
        i=0
        for file in files:
            if file == 'vols':
                color_list[i]='success'
            else:
                #get current date                
                update_time = json.loads(conn.get('{}_update'.format(file.upper())))
                update_time = datetime.strptime(str(update_time), '%Y%m%d')

                #compare to yesterday to see if old
                yesterday = date.today() - timedelta(days = 1)
                if update_time.date() >= yesterday:
                    color_list[i]='success'
                else:
                    color_list[i]='danger' 

            i=i+1

        return color_list    
