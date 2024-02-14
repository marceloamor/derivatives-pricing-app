import os

import dash
import dash_bootstrap_components as dbc
import flask
from dash import dcc, html
from dotenv import load_dotenv
from flask_sqlalchemy import SQLAlchemy


def create_app():
    load_dotenv()
    server = flask.Flask(__name__)

    # add external style sheet for bootstrap
    app = dash.Dash(
        __name__,
        server=server,
        external_stylesheets=[dbc.themes.BOOTSTRAP],
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

    with app.server.app_context():

        class Config(object):
            SQLALCHEMY_DATABASE_URI = os.getenv("GEORGIA_POSTGRES_URI")
            SQLALCHEMY_TRACK_MODIFICATIONS = False

        app.server.config.from_object(Config)
        flask.g.db = SQLAlchemy()
        flask.g.db.init_app(app.server)

        from routes import routes

        routes(app, server)

    print("georgia starting...")
    return app, server
