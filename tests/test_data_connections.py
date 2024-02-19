import sys

import flask_sqlalchemy
import pandas as pd
from app import app
from upedata import static_data as upe_static

sys.path.append("src/")
import data_connections  # noqa: E402


# TEST NEW DATABASE -----------------------------------------------------------------
def test_engine_not_none():
    assert data_connections.shared_engine is not None
    assert isinstance(data_connections.db, flask_sqlalchemy.SQLAlchemy)


def test_engine_queries():
    # test that the engine can query the database
    with data_connections.shared_engine.connect() as cnxn:
        positions = pd.read_sql_table("positions", cnxn)

    assert isinstance(positions, pd.DataFrame)


def test_Session_not_none():
    with app.server.app_context():
        with data_connections.shared_session() as session:
            assert session is not None


def test_Session_functionally():
    with app.server.app_context():
        with data_connections.shared_session() as session:
            gareth = (
                session.query(upe_static.Trader.full_name)
                .filter(upe_static.Trader.trader_id == 1)
                .first()
            )
            assert gareth[0] == "Gareth Upe"


# TEST REDIS--------------------------------------------------------------------
def test_getRedis_valid_connection():
    conn = data_connections.getRedis(
        data_connections.redisLocation,
        data_connections.redis_port,
        data_connections.redis_key,
    )
    assert conn is not None


def test_getRedis_with_valid_parameters():
    # mock parameters, host = localhost to test that works too
    redisLocation = "mock_host"
    redis_port = 6379
    redis_key = "mock_password"

    conn = data_connections.getRedis(redisLocation, redis_port, redis_key)
    assert conn.connection_pool.connection_kwargs["host"] == "mock_host"
    assert conn.connection_pool.connection_kwargs["port"] == 6379
    assert conn.connection_pool.connection_kwargs["password"] == "mock_password"


def test_getRedis_localhost_connection():
    redisLocation = "localhost"
    conn = data_connections.getRedis(redisLocation)
    assert conn.connection_pool.connection_kwargs["host"] == "localhost"


# redis functional tests
def test_redis_set_get():
    conn = data_connections.getRedis(
        data_connections.redisLocation,
        data_connections.redis_port,
        data_connections.redis_key,
    )
    # get nil value
    assert conn.get("redis_test_key") is None

    # set value
    conn.set("redis_test_key", "test_value")
    assert conn.get("redis_test_key") == b"test_value"

    # del value
    conn.delete("redis_test_key")
    assert conn.get("redis_test_key") is None


# TEST LEGACY DATABASE---------------------------------------------------------
def test_legacy_db_connection():
    # test that the engine can query the database
    with data_connections.PostGresEngine().connect() as cnxn:
        positions = pd.read_sql_table("positions", cnxn)

    assert isinstance(positions, pd.DataFrame)
