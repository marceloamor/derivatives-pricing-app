from dash.dependencies import Input, Output
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_table as dtable
from dash import no_update

from app import app, topMenu
from parts import pullPortfolioGreeks

# columns = [{"name": 'Portfolio', "id": 'portfolio'}, 
#            {"name": 'Delta', "id": 'delta'},
#              {"name": 'Full Delta', "id": 'fullDelta'},
#              {"name": 'Vega', "id": 'vega'},
#              {"name": 'Theta', "id": 'theta'},
#              {"name": 'Gamma', "id": 'gamma'},
#              {"name": 'Skew', "id": 'skew'},
#              {"name": 'Call', "id": 'call'},
#              {"name": 'Put', "id": 'put'},
#              {"name": 'Delta Decay', "id": 'deltaDecay'},
#              {"name": 'Vega Decay', "id": 'vegaDecay'},
#              {"name": 'Gamma Decay', "id": 'gammaDecay'}
#            ]


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
    ]
)

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

layout = html.Div([
    dcc.Interval(id='live-update', 
                 interval=1*1000, # in milliseconds
                 n_intervals=0),
topMenu('Home'),
html.Div([
    jumbotron
    ]),
totalsTable
])

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
