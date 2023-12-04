import dash_bootstrap_components as dbc
from dash import dcc, html
import dash, flask
import os

import pandas as pd
from flask_sqlalchemy import SQLAlchemy

# db = SQLAlchemy()


class Config(object):
    SQLALCHEMY_DATABASE_URI = os.getenv("GEORGIA_POSTGRES_URI")
    SQLALCHEMY_TRACK_MODIFICATIONS = False


def create_app():
    server = flask.Flask(__name__)
    server.config.from_object(Config)

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

    # postgresURI = os.environ.get("GEORGIA_POSTGRES_URI")
    # app.server.config["SQLALCHEMY_DATABASE_URI"] = postgresURI
    # # necessary to suppress console warning
    # app.server.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    with app.server.app_context():
        from routes import routes

        routes(app, server)

    print("georgia starting...")
    return app, server
