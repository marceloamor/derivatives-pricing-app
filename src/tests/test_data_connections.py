from dash.testing.application_runners import import_app
import flask_sqlalchemy
import pandas as pd
import sys
import pytest
import upestatic


sys.path.append("src/")
from app import app


# Test case 1: Write your test case here
import data_connections


# tests/test_config.py
class TestConfig:
    TESTING = True
    SQLALCHEMY_DATABASE_URI = "your_test_database_uri_here"
    SQLALCHEMY_TRACK_MODIFICATIONS = False  # Disable modification tracking


# @pytest.fixture
# def dash_app():
#     app_to_test = import_app("app")  # Replace with the correct import path
#     with app_to_test.server.app_context():
#         yield app_to_test


def test_engine_not_none():
    assert data_connections.engine is not None
    assert isinstance(data_connections.db, flask_sqlalchemy.SQLAlchemy)


def test_engine_queries():
    # test that the engine can query the database
    with data_connections.engine.connect() as cnxn:
        positions = pd.read_sql_table("positions", cnxn)

    assert isinstance(positions, pd.DataFrame)


def test_Session_not_none():
    with app.server.app_context():
        with data_connections.Session() as session:
            products = session.query(upestatic.Product.currency).all()
            print(products)
