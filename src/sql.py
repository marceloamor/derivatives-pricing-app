from data_connections import PostGresEngine, conn

import pandas as pd

import pickle


# used, upgrade to sqlalchemy
def pullPosition(product, date):
    sql = (
        "SELECT *  FROM positions where left(instrument, 3) = '"
        + product
        + "' and quanitity <> 0"
    )
    df = pd.read_sql(sql, PostGresEngine())
    return df


# used, upgrade to sqlalchemy
def pullCodeNames():
    sql = "SELECT *  FROM brokers"
    df = pd.read_sql(sql, PostGresEngine())
    return df

# used, upgrade to sqlalchemy
def pullRouteStatus():
    # sql = "SELECT TOP 100 * FROM route_status order by saveddate desc"
    sql = """SELECT *
        FROM public.routed_trades
        WHERE AGE(datetime) < INTERVAL '24 hours'
        ORDER BY datetime desc"""
    df = pd.read_sql(sql, PostGresEngine())
    return df


# used and important - upgrade to sqlalchemy
def histroicParams(product):
    sql = "SELECT * FROM vol_model_param_history where product = '" + product + "'"
    df = pd.read_sql(sql, PostGresEngine())
    return df
