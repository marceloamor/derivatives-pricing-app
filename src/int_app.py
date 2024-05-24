import logging
import os

import dash
import dash_bootstrap_components as dbc
import flask
from azure.monitor.opentelemetry import configure_azure_monitor
from dash import dcc, html
from dotenv import load_dotenv
from flask_sqlalchemy import SQLAlchemy
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.instrumentation.system_metrics import SystemMetricsInstrumentor
from opentelemetry.metrics import set_meter_provider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    PeriodicExportingMetricReader,
)


def create_app():
    load_dotenv()

    azure_metrics_connection_string = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")
    if azure_metrics_connection_string is not None:
        configure_azure_monitor(
            connection_string=azure_metrics_connection_string,
            logger_name="frontend",
            instrumentation_options={
                "flask": {"enabled": True},
                "psycopg": {"enabled": True},
                "redis": {"enabled": True},
                "sqlalchemy": {"enabled": True},
                "system-metrics": {"enabled": True},
                "logging": {"enabled": True},
            },
        )
        SQLAlchemyInstrumentor().instrument(enable_commenter=True, commenter_options={})
        RedisInstrumentor().instrument()
        LoggingInstrumentor().instrument()

        exporter = ConsoleMetricExporter()
        set_meter_provider(MeterProvider([PeriodicExportingMetricReader(exporter)]))
        SystemMetricsInstrumentor().instrument()

    logging_level = logging.INFO
    formatter = logging.Formatter(
        "{asctime}.{msecs:3.0f} | {levelname:8s} | {name} | {message}",
        "%Y-%m-%d %H:%M:%S",
        style="{",
    )
    console_log = logging.StreamHandler()
    console_log.setFormatter(formatter)
    logger = logging.getLogger("frontend")
    logger.setLevel(logging_level)
    logger.addHandler(console_log)

    server = flask.Flask(__name__)

    if azure_metrics_connection_string is not None:
        FlaskInstrumentor().instrument_app(server)

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

    logger.info("Georgia starting...")
    return app, server
