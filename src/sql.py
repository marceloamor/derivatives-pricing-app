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


# currently used for backwards compatibility with old trades table -- change to sqlalchemy
def delete_trade(id):
    # connect to the database using PostGresEngine()

    with PostGresEngine().connect() as cnxn:
        # execute the delete_trade function
        sql = "select public.delete_trade ({})".format(int(id))
        cnxn.execute(sql)

    # update trades in redis
    trades = pd.read_sql("trades", PostGresEngine())
    trades.columns = trades.columns.str.lower()
    pick_trades = pickle.dumps(trades, protocol=-1)
    conn.set("trades", pick_trades)

    # update pos in redis from postgres.
    pos = pd.read_sql("positions", PostGresEngine())
    pos.columns = pos.columns.str.lower()
    pos = pickle.dumps(pos)
    conn.set("positions", pos)


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
