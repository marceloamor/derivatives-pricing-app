import dash, flask
import dash_bootstrap_components as dbc
import dash_html_components as html
from parts import ringTime

server = flask.Flask(__name__)

external_stylesheets = []

#add external style sheet for bootstrap
app = dash.Dash(__name__, server = server, external_stylesheets=[dbc.themes.BOOTSTRAP])

# force offline usage
app.scripts.config.serve_locally = True

#suprees callback exceptions to stop erros being triggered when loading layouts. 
app.config.suppress_callback_exceptions = True

georgiaLogo = 'assets/images/favicon.ico'

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
                dbc.DropdownMenuItem('Logs', href='/logpage'),
            ],
            #nav=True,
            in_navbar=True,
            label="Settings",
        ),
        html.Div([ringTime()])
                        ],
   
    color="primary",
    dark=True,
)
])
