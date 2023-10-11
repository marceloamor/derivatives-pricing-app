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
        # changed from calling delete_trade(id) psql function to manual update after it broke
        # change to sqlalchemy text/paramterized query after georgia update
        sql = f'SELECT quanitity FROM public.trades WHERE "ID" = {id}'
        qty = cnxn.execute(sql).fetchone()[0]

        if qty is not None:
            # Update the "public.trades" table
            sql1 = f'UPDATE public.trades SET deleted = 1 WHERE "ID" = {id}'
            cnxn.execute(sql1)

            # Update the "public.positions" table
            sql2 = f"""
            UPDATE public.positions
            SET quanitity = quanitity - {qty}
            WHERE instrument = (SELECT instrument FROM public.trades WHERE "ID" = {int(id)})
            """
            cnxn.execute(sql2)

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
