# RecSQl is a group of funstions to pull data from the SQL database using strings of stroed procedures
import pyodbc, calendar, csv, sys, multiprocessing, pickle, redis, os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, insert, delete
import pandas as pd

# connect to redis (default to localhost).
redisLocation = os.getenv("REDIS_LOCATION", default="localhost")
conn = redis.Redis(redisLocation)

# connect a cursor to the desried DB
def Connection(server, DB):
    # sql softs DB connection details
    server = str(server)
    database = str(DB)
    driver = "{ODBC Driver 13 for SQL Server}"
    cnxn = pyodbc.connect(
        "DRIVER="
        + driver
        + ";SERVER="
        + server
        + ";PORT=1433;DATABASE="
        + database
        + ";Trusted_connection=yes"
    )
    cursor = cnxn.cursor()

    return cnxn


# postgresql connection detials amd create connection
def PostGresEngine():
    engine = create_engine(
        "postgres://jtadtrdlaubnst:2916130e4ae2957b033115855a31fe3ec126d217fdeab4121d104154909a2430@ec2-54-247-113-90.eu-west-1.compute.amazonaws.com:5432/ddb17pbfj9e7cv"
    )
    return engine


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


def Cursor(server, DB):
    # sql softs DB connection details
    server = str(server)
    database = str(DB)
    driver = "{ODBC Driver 13 for SQL Server}"
    cnxn = pyodbc.connect(
        "DRIVER="
        + driver
        + ";SERVER="
        + server
        + ";PORT=1433;DATABASE="
        + database
        + ";Trusted_connection=yes"
    )
    cursor = cnxn.cursor()

    return cursor


def pulltrades(date):
    cnxn = PostGresEngine()
    sql = "SELECT*  FROM trades where dateTime > '" + date + "' order by dateTime desc"
    df = pd.read_sql_query(sql, cnxn)

    return df


def pullCodeNames():
    cnxn = PostGresEngine()
    sql = "SELECT*  FROM codenames"
    df = pd.read_sql_query(sql, cnxn)

    return df


def pullPosition(product, date):
    cnxn = PostGresEngine()
    sql = (
        "SELECT *  FROM positions where left(instrument, 3) = '"
        + product
        + "' and quanitity <> 0"
    )
    df = pd.read_sql_query(sql, cnxn)
    return df


def pullAllPosition(date):
    cnxn = PostGresEngine()
    sql = "SELECT *  FROM positions where  quanitity <> 0 and dateTime > '" + date + "'"
    df = pd.read_sql_query(sql, cnxn)
    return df


# pull position from F2 DB
def pullF2Position(date, product):
    cnxn = PostGresEngine()
    sql = """select productId, prompt, optionTypeId, strike,  (buyLots - sellLots) as quanitity from DBO.OpenPositionCOB 
                where positionHolderId in ('90601', '90602', '90603', '90604', '90605') and cobDate = '{}' and (buyLots - sellLots) <>0 and left(productId,3) = '{}'""".format(
        date, product
    )
    df = pd.read_sql_query(sql, cnxn)
    return df


def pullAllF2Position(date):
    cnxn = PostGresEngine()
    sql = """select productId, prompt, optionTypeId, strike,  (buyLots - sellLots) as quanitity from DBO.OpenPositionCOB 
                where positionHolderId in ('90601', '90602', '90603', '90604', '90605') and cobDate = '{}' and (buyLots - sellLots) <>0""".format(
        date
    )
    df = pd.read_sql_query(sql, cnxn)
    return df


def deletePositions(date, product):
    cnxn = PostGresEngine()
    cursor.execute(
        """delete FROM positions where left(instrument,3)='{}'""".format(product)
    )
    cursor.commit()


def deleteAllPositions():
    cnxn = PostGresEngine()
    cursor.execute("""delete FROM positions """)
    cursor.commit()
    cursor.close()


# insert trade in trades sql then update other sources
def sendTrade(trade):
    cnxn = PostGresEngine()
    data = [
        trade.timestamp,
        trade.name,
        abs(float(trade.price)),
        trade.qty,
        trade.theo,
        trade.user,
        trade.countPart,
        trade.comment,
        trade.prompt,
        trade.venue,
    ]
    print(data)
    # cursor.execute('insert into trades values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', data)
    cnxn.execute(trades.insert(), data)
    return id


def sendPosition(trade):
    cnxn = PostGresEngine()
    data = [
        trade.timestamp,
        trade.name,
        abs(trade.price),
        trade.qty,
        trade.theo,
        trade.user,
        trade.countPart,
        trade.comment,
        trade.prompt,
    ]
    cnxn.execute(trades.insert(), data)
    return id


# executes SP on sql server that adds/updates position table
def updatePos(trade):
    cursor = Cursor("Sucden-sql-soft", "LME")
    cursor.execute(
        "execute updatePos '{}', {}, '{}', '{}'".format(
            str(trade.name), trade.qty, trade.timestamp, trade.prompt
        )
    )
    cursor.commit()
    cursor.close()


# pulls SQL position table for given product and updates redis server
def updateRedisPos(product):
    print("redis update")
    print(product)

    # pull position from SQL
    cnxn = PostGresEngine()
    sql = (
        "SELECT * FROM positions where left(instrument,6) = '" + product + "'"
    )  # and dateTime >= CAST(GETDATE() AS DATE)"
    df = pd.read_sql_query(sql, cnxn)
    df.to_dict("index")
    # pickle it and send it to redis.
    df = pickle.dumps(df, protocol=-1)
    conn.set(product.lower() + "Pos", df)


def updateRedisTrade(product):
    product = (product)[:3]
    cnxn = PostGresEngine()
    sql = (
        "SELECT * FROM trades where left(instrument,3) = '"
        + product
        + "'  and dateTime >= CAST(GETDATE() AS DATE)"
    )
    df = pd.read_sql_query(sql, cnxn)
    df.to_dict("index")
    df = pickle.dumps(df, protocol=-1)

    conn.set(product.lower() + "Trade", df)


def updateRedisDelta(product):
    product = (product)[:3]
    cnxn = PostGresEngine()
    sql = "SELECT * FROM positions where left(instrument,4) = '" + product + " '"
    df = pd.read_sql_query(sql, cnxn)
    df.to_dict("index")
    df = pickle.dumps(df, protocol=-1)
    conn.set(product.lower() + "Delta", df)


# load dleta from sql and fit to redis
def updateRedisCurve(product):
    # load from SQL
    cnxn = PostGresEngine()
    sql = "SELECT * FROM positions where left(instrument,4) = '" + product[0:3] + " '"
    df = pd.read_sql_query(sql, cnxn)

    # convert to dict
    df.to_dict("index")

    # pull curve back out of redis
    product1 = productToPortfolio(product[0:3])
    curve = conn.get(product1 + "Curve")
    curve = pickle.loads(curve)

    # loop over instruments in DF and add to open pos
    for instruments in df["instrument"]:

        # convert date to pull spread
        date = datetime.strptime(instruments[-10:], "%Y-%m-%d")
        date = date.strftime("%Y%m%d")

        # position from SQL
        position = df[df["instrument"] == instruments]["quanitity"].values[0]

        # insert position into dataframe where date matches
        curve.loc[curve["FORWARD_DATE"] == int(date), "POSITION"] = position

    # send curve back to redis
    curve = pickle.dumps(curve, protocol=-1)
    conn.set(product1 + "Curve", curve)

    # pickle and send to redis
    df = pickle.dumps(df, protocol=-1)
    conn.set(product.lower()[0:3] + "Delta", df)


def updateRedisPosOnLoad(product):
    cnxn = PostGresEngine()
    sql = "SELECT * FROM positions where left(instrument,6) = '" + product + "'"
    df = pd.read_sql_query(sql, cnxn)
    df.to_dict("index")
    df = pickle.dumps(df, protocol=-1)
    conn.set(product.lower() + "Pos", df)


def deletePosRedis(portfolio):
    data = conn.get("staticData")
    data = pd.read_json(data)
    products = data.loc[data["portfolio"] == portfolio]["product"]
    for product in products:
        conn.delete(product.lower() + "Pos")


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
    cnxn = PostGresEngine()
    sql = "SELECT TOP 100 * FROM routeStatus"
    df = pd.read_sql_query(sql, cnxn)

    return df


def histroicParams(product):
    cnxn = PostGresEngine()
    sql = "SELECT * FROM params where product = '" + product + "'"
    df = pd.read_sql_query(sql, cnxn)

    return df
