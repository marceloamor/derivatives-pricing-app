from dash import dcc
import dash, flask, os
import dash_bootstrap_components as dbc
from dash import dcc, html
from flask_sqlalchemy import SQLAlchemy
#from data_connections import db


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

    # # connect database to app
    # postgresURL = os.environ.get("GEORGIA_POSTGRES_URL")
    # app.server.config["SQLALCHEMY_DATABASE_URI"] = postgresURL
    # # necessary to suppress warning when using flask_sqlalchemy
    # app.server.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    
    #db.init_app(app.server)

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
