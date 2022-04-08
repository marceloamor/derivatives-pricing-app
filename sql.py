# RecSQl is a group of funstions to pull data from the SQL database using strings of stroed procedures
import pickle
from datetime import datetime
import pandas as pd
import psycopg2

from data_connections import Connection, Cursor, conn, connect, PostGresEngine


def productToPortfolio(product):
    if product.lower() == "lcu":
        return "copper"
    elif product.lower() == "lad":
        return "aluminium"
    elif product.lower() == "pbd":
        return "lead"
    elif product.lower() == "lnd":
        return "nickel"
    elif product.lower() == "lzh":
        return "zinc"
    else:
        return "unkown"


def pulltrades(date):
    cnxn = Connection("Sucden-sql-soft", "LME")
    sql = 'SELECT*  FROM trades where "dateTime" > {} order by "dateTime" desc'.format(
        date
    )
    df = pd.read_sql(sql, cnxn)
    cnxn.close()
    return df


def pullPosition(product, date):
    cnxn = Connection("Sucden-sql-soft", "LME")
    sql = (
        "SELECT *  FROM positions where left(instrument, 3) = '"
        + product
        + "' and quanitity <> 0"
    )
    df = pd.read_sql(sql, cnxn)
    cnxn.close()
    return df


def pullAllPosition(date):
    cnxn = Connection("Sucden-sql-soft", "LME")
    sql = "SELECT *  FROM positions where  quanitity <> 0 and dateTime > '" + date + "'"
    df = pd.read_sql(sql, cnxn)
    cnxn.close()
    return df


def pullCodeNames():
    cnxn = Connection("Sucden-sql-soft", "LME")
    sql = "SELECT *  FROM brokers"
    df = pd.read_sql(sql, cnxn)
    cnxn.close()
    return df


# pull position from F2 DB
def pullF2Position(date, product):
    try:
        cnxn = Connection("LIVE-BOSQL1", "FuturesII")
        sql = """select DISTINCT productId, prompt, optionTypeId, strike,  (buyLots - sellLots) as quanitity from DBO.OpenPositionCOB 
                    where positionHolderId in ('90601', '90602', '90603', '90604', '90605') and cobDate = '{}' and (buyLots - sellLots) <>0 and left(productId,3) = '{}'""".format(
            date, product
        )
        df = pd.read_sql(sql, cnxn)
        cnxn.close()
        return df
    except Exception as e:
        return pd.DataFrame()


def pullAllF2Position(date):
    # cnxn = Connection('LIVE-ACCSQL','FuturesIICOB')
    cnxn = Connection("LIVE-BOSQL1", "FuturesII")
    sql = """select DISTINCT productId, prompt, optionTypeId, strike,  (buyLots - sellLots) as quanitity from DBO.OpenPositionCOB 
                where positionHolderId in ('90601', '90602', '90603', '90604', '90605') and cobDate = '{}' and (buyLots - sellLots) <>0 and productId NOT IN ('TCAO', 'TADO', 'TZSO', 'TNDO')""".format(
        date
    )
    df = pd.read_sql(sql, cnxn)
    cnxn.close()
    return df


def deletePositions(date, product):
    cursor = Cursor("Sucden-sql-soft", "LME")
    cursor.execute(
        """delete FROM positions where left(instrument,3)='{}'""".format(product)
    )
    cursor.commit()
    cursor.close()


def deleteAllPositions():
    cursor = Cursor("Sucden-sql-soft", "LME")
    cursor.execute("""delete FROM positions """)
    cursor.commit()
    cursor.close()


# insert trade in trades sql then update other sources
def sendTrade(trade):
    try:
        # prevent empty prices and qty being sent
        if not trade.price:
            trade.price = 0
        if not trade.qty:
            trade.qty = 0

        cursor = Cursor("Sucden-sql-soft", "LME")
        sql = """
        INSERT INTO public.trades(
                "dateTime", instrument, price, quanitity, theo, "user", "counterPart", "Comment", prompt, venue, deleted)
                VALUES ('{}', '{}', {}, {}, {}, '{}', '{}', '{}', '{}', '{}', '{}');	
        """.format(
            trade.timestamp.strftime("%Y-%m-%d, %H:%M:%S"),
            trade.name,
            abs(float(trade.price)),
            float(trade.qty),
            float(trade.theo),
            trade.user,
            trade.countPart,
            trade.comment,
            trade.prompt,
            trade.venue,
            0,
        )

        cursor.execute(sql)
        cursor.commit()
        cursor.close()

        trades = pd.read_sql("trades", PostGresEngine())
        trades.columns = trades.columns.str.lower()
        trades = pickle.dumps(trades)
        conn.set(trades, "trades")

        return 1
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return 0


def sendPosition(trade):
    cursor = Cursor("Sucden-sql-soft", "LME")
    data = [
        trade.timestamp,
        trade.name,
        abs(trade.price),
        float(trade.qty),
        float(trade.theo),
        trade.user,
        trade.countPart,
        trade.comment,
        trade.prompt,
    ]
    cursor.execute("insert into trades values (?, ?, ?, ?, ?, ?, ?, ?, ?);", data)
    cursor.commit()

    id = cursor.execute("SELECT @@IDENTITY AS id;").fetchone()[0]
    cursor.close()

    trades = pd.read_sql("trades", PostGresEngine())
    trades.columns = trades.columns.str.lower()
    trades = pickle.dumps(trades)
    conn.set(trades, "trades")

    return id


# executes SP on sql server that adds/updates position table
def updatePos(trade):
    cursor = Cursor("Sucden-sql-soft", "LME")

    sql = "select upsert_position ( {}, '{}', '{}')".format(
        float(trade.qty), str(trade.name), trade.timestamp
    )

    cursor.execute(sql)
    cursor.commit()
    cursor.close()

    pos = pd.read_sql("positions", PostGresEngine())
    pos.columns = pos.columns.str.lower()
    pos = pickle.dumps(pos)
    conn.set("positions", pos)


def delete_trade(id):
    cursor = Cursor("Sucden-sql-soft", "LME")

    sql = "select public.delete_trade ({})".format(int(id))
    cursor.execute(sql)
    cursor.commit()
    cursor.close()

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


# pulls SQL position table for given product and updates redis server
def updateRedisPos(product):
    # pull position from SQL
    cnxn = Connection("Sucden-sql-soft", "LME")
    sql = "SELECT * FROM positions"
    df = pd.read_sql(sql, cnxn)
    df.to_dict("index")
    # pickle it and send it to redis.
    df = pickle.dumps(df, protocol=-1)
    # conn.set(product.lower()+'Pos',df)
    conn.set("positions", df)

    cnxn.close()


def updateRedisTrade(product):
    product = (product)[:3]
    cnxn = Connection("Sucden-sql-soft", "LME")
    # sql = "SELECT * FROM trades where left(instrument,3) = '"+product+"'  and dateTime >= CAST(GETDATE() AS DATE)"
    sql = "SELECT * FROM trades"

    df = pd.read_sql(sql, cnxn)
    df.to_dict("index")
    df = pickle.dumps(df, protocol=-1)

    conn.set("trades", df)
    cnxn.close()


def updateRedisDelta(product):
    product = (product)[:3]
    cnxn = Connection("Sucden-sql-soft", "LME")
    sql = "SELECT * FROM positions where left(instrument,4) = '" + product + " '"
    df = pd.read_sql(sql, cnxn)
    df.to_dict("index")
    df = pickle.dumps(df, protocol=-1)
    conn.set(product.lower() + "Delta", df)
    cnxn.close()


# load dleta from sql and fit to redis
def updateRedisCurve(product):
    # load from SQL
    cnxn = Connection("Sucden-sql-soft", "LME")
    sql = "SELECT * FROM positions where left(instrument,4) = '" + product[0:3] + " '"
    df = pd.read_sql(sql, cnxn)

    # convert to dict
    df.to_dict("index")

    # pull curve back out of redis
    product1 = productToPortfolio(product[0:3])
    curve = conn.get(product1 + "Curve")
    curve = pickle.loads(curve)

    # delete all positons in curve to remove zerod out positions
    curve["POSITION"] = 0

    # loop over instruments in DF and add to open pos
    for instruments in df["instrument"]:
        # convert date to pull spread
        date = datetime.strptime(instruments[-10:], "%Y-%m-%d")
        date = date.strftime("%Y%m%d")

        # position from SQL
        position = df[df["instrument"] == instruments]["quanitity"].values[0]

        # insert position into dataframe where date matches
        curve.loc[curve["forward_date"] == int(date), "position"] = position

    # send curve back to redis
    curve = pickle.dumps(curve, protocol=-1)
    conn.set(product1 + "Curve", curve)

    # pickle and send to redis
    df = pickle.dumps(df, protocol=-1)
    conn.set(product.lower()[0:3] + "Delta", df)
    cnxn.close()


def updateRedisPosOnLoad(product):
    # product = (trade.name)[:6]
    cnxn = Connection("Sucden-sql-soft", "LME")
    sql = "SELECT * FROM positions where left(instrument,6) = '" + product + "'"
    df = pd.read_sql(sql, cnxn)
    df.to_dict("index")
    df = pickle.dumps(df, protocol=-1)
    conn.set(product.lower() + "Pos", df)
    cnxn.close()


def deleteTrades(date):
    cursor = Cursor("Sucden-sql-soft", "LME")
    cursor.execute("""delete from trades where dateTime > '{}' """.format(date))
    cursor.commit()
    cursor.close()


def storeTradeSend(trade, response):
    # parse response message
    status = response["Status"]
    message = response["ErrorMessage"]

    # data to send
    data = (
        trade.timestamp,
        trade.product,
        trade.strike,
        trade.cop,
        trade.price,
        trade.qty,
        trade.countPart,
        trade.user,
        trade.venue,
        status,
        message,
    )

    # send message to SQL
    cursor = Cursor("Sucden-sql-soft", "LME")
    cursor.execute(
        "insert into routeStatus values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", data
    )
    cursor.commit()

    cursor.close()


def pullRouteStatus():
    cnxn = Connection("Sucden-sql-soft", "LME")
    sql = "SELECT TOP 100 * FROM routeStatus order by saveddate desc"
    df = pd.read_sql(sql, cnxn)
    cnxn.close()
    return df


def histroicParams(product):
    cnxn = Connection("Sucden-sql-soft", "LME")
    sql = "SELECT * FROM params where product = '" + product + "'"
    df = pd.read_sql(sql, cnxn)
    cnxn.close()
    return df
