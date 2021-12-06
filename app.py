import dash, flask, os
import dash_bootstrap_components as dbc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import dash_html_components as html

from parts import ringTime
from company_styling import favicon_name

#waitress server
from waitress import serve

server = flask.Flask(__name__)
external_stylesheets = []

from company_styling import main_color, logo

#add external style sheet for bootstrap
app = dash.Dash(__name__, server = server, external_stylesheets=[dbc.themes.BOOTSTRAP])

# force offline usage
app.scripts.config.serve_locally = True

#suprees callback exceptions to stop erros being triggered when loading layouts. 
app.config.suppress_callback_exceptions = True

georgiaLogo = logo

def topMenu(page):

    return html.Div([
dbc.Navbar(
    children=[
html.A(
            dbc.Row(                [
                    dbc.Col(html.Img(src=georgiaLogo, height="40px")),
                    dbc.Col(dbc.NavbarBrand(page, className="ml-1")),
                    ]
                    )
            , href='/'
            ),
           
        dbc.DropdownMenu(
            children=[
                dbc.DropdownMenuItem('Calculator', href='/calculator'),
                dbc.DropdownMenuItem('Vol Surface', href='/volsurface'),
                dbc.DropdownMenuItem('Vol Matrix', href='/volMatrix'),
                dbc.DropdownMenuItem('Pnl', href='/pnl'),
            ],
            #nav=True,
            in_navbar=True,
            label="Pricing",
        ),
        dbc.DropdownMenu(
            children=[
                dbc.DropdownMenuItem('Risk', href='/riskmatrix'),
                dbc.DropdownMenuItem('Strike Risk', href='/strikeRisk'),
                dbc.DropdownMenuItem('Delta Vola', href='/deltaVola'),
                dbc.DropdownMenuItem('Portfolio', href='/portfolio'),
                dbc.DropdownMenuItem('Prompt Curve', href='/prompt'),
            ],
            #nav=True,
            in_navbar=True,
            label="Risk",
        ),
        dbc.DropdownMenu(
            children=[
                dbc.DropdownMenuItem('Trades', href='/trades'),
                dbc.DropdownMenuItem('Position', href='/position'),
                dbc.DropdownMenuItem('F2 Rec', href='/rec'),
                dbc.DropdownMenuItem('Route Status', href='/routeStatus'),
                dbc.DropdownMenuItem('Expiry', href='/expiry'),
                dbc.DropdownMenuItem('Rate Curve', href='/rates'),

            ],
            #nav=True,
            in_navbar=True,
            label="Reconciliation",
        ),
        dbc.DropdownMenu(
            children=[
                dbc.DropdownMenuItem('Static Data', href='/staticData'),
                dbc.DropdownMenuItem('Brokers', href='/brokers'),
                dbc.DropdownMenuItem('Data Load', href='/dataload'),
                dbc.DropdownMenuItem('Logs', href='/logpage'),                
            ],
            #nav=True,
            in_navbar=True,
            label="Settings",
        ),
        html.Div([ringTime()])
                        ],
   
    color=main_color,
    dark=True,
)
])

from apps import dataLoad, brokers, trades, app2, homepage, rates,  portfolio, position, promptCurve, logPage, calculator, settings, pnl, riskMatrix, strikeRisk, whiteBoard, deltaVolas, rec, volMatrix, expiry, routeStatus, staticData 
import volSurfaceUI

#add icon and title for top of website
@app.server.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(server.root_path, 'assets/images'),
                               favicon_name, mimetype='image/vnd.microsoft.icon')
app.title = 'Georgia'

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(list("ABC"), id="data", style={"display":"none"}),
    html.Div(id='page-content')
])

@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/trades':
         return trades.layout
    elif pathname == '/app2':
         return app2.layout
    elif pathname == '/volsurface':
         return volSurfaceUI.layout
    elif pathname == '/rates':
         return rates.layout
    elif pathname == '/portfolio':
         return portfolio.layout
    elif pathname == '/position':
         return position.layout
    elif pathname == '/prompt':
         return promptCurve.layout
    elif pathname == '/logpage':
         return logPage.layout
    elif pathname == '/calculator':
         return calculator.layout
    elif pathname == '/settings':
         return settings.layout  
    elif pathname == '/pnl':
         return pnl.layout
    elif pathname == '/riskmatrix':
         return riskMatrix.layout
    elif pathname == '/strikeRisk':
         return strikeRisk.layout
    elif pathname == '/volMatrix':
        return volMatrix.layout
    elif pathname == '/deltaVola':
         return deltaVolas.layout
    elif pathname == '/rec':
         return rec.layout
    elif pathname == '/expiry':
         return expiry.layout
    elif pathname == '/routeStatus':
         return routeStatus.layout
    elif pathname == '/staticData':
        return staticData.layout
    elif pathname == '/brokers':
         return brokers.layout
    elif pathname == '/dataload':
         return dataLoad.layout
    else:
        return homepage.layout

if __name__ == '__main__':
   #app.run()
   server.run(debug=True)

   #app.run_server(debug=True)
   server = app.server
   #serve(app.server, host='0.0.0.0', port=8050, ipv6 = False, threads = 25)

