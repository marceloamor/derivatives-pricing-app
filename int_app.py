from dash import dcc
import dash, flask
import dash_bootstrap_components as dbc
from dash import dcc, html


def create_app():
    server = flask.Flask(__name__)

    # add external style sheet for bootstrap
    app = dash.Dash(
        __name__, server=server, external_stylesheets=[dbc.themes.BOOTSTRAP]
    )

    # force offline usage
    app.scripts.config.serve_locally = True

    # suprees callback exceptions to stop erros being triggered when loading layouts.
    app.config.suppress_callback_exceptions = True

    # add title to website
    app.title = "Georgia"

    # build layout
    app.layout = html.Div(
        [
            dcc.Location(id="url", refresh=False),
            html.Div(list("ABC"), id="data", style={"display": "none"}),
            html.Div(id="page-content"),
        ]
    )

    from routes import routes

    routes(app, server)

    return app, server
