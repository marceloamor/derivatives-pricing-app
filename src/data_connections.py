from sqlalchemy import create_engine
import sqlalchemy
import sqlalchemy.orm as orm
import pandas as pd
import pyodbc, redis, os, psycopg2

from dotenv import load_dotenv

load_dotenv()

# Georgia official postgres connection
georgia_postgres_location = os.getenv("GEORGIA_POSTGRES_LOCATION")
georgia_postgres_username = os.getenv("GEORGIA_POSTGRES_USERNAME")
georgia_postgres_password = os.getenv("GEORGIA_POSTGRES_PASSWORD")
georgia_postgres_database = os.getenv("GEORGIA_POSTGRES_DATABASE")

# georgia postgres engine engine and session
# import engine for non-ORM queries
# import session for ORM queries
engine = create_engine(
    f"postgresql+psycopg2://{georgia_postgres_username}:{georgia_postgres_password}@{georgia_postgres_location}/{georgia_postgres_database}"
)
Session = orm.sessionmaker(bind=engine)

# sql softs DB connection details
postgresLocation = os.getenv(
    "POSTGRES_LOCATION", default="georgiatest.postgres.database.azure.com"
)
postgresuserid = os.getenv("POST_USER", default="gareth")
postgrespassword = os.getenv("POST_PASSWORD", default="CVss*bsh3T")

riskAPi = os.getenv("RISK_LOCATION", default="localhost")

f2server = os.getenv("F2_SERVER", default="bulldogmini.postgres.database.azure.com")
f2database = os.getenv("F2_DATABASE", default="bulldogmini")
f2userid = os.getenv("F2_USER", default="gareth")
f2password = os.getenv("F2_PASSWORD", default="Wolve#123")

georgiaserver = os.getenv(
    "GEORGIA_SERVER", default="georgiatest.postgres.database.azure.com"
)
georgiadatabase = os.getenv("GEORGIA_DATABASE", default="LME_test")
georgiauserid = os.getenv("GEORGIA_USER", default="georgia_test")
georgiapassword = os.getenv("GEORGIA_PASSWORD", default="georgia123")

# redis connection details
redisLocation = os.getenv(
    "REDIS_LOCATION", default="georgiatest.redis.cache.windows.net"
)
redis_key = os.getenv(
    "REDIS_KEY", default="GSJLRhrptLSXWUA0QyMiuF8fLsKnaFXu4AzCaCgjcx8="
)
redis_port = os.getenv("REDIS_PORT", default="6380")

Base = orm.declarative_base()


class HistoricalVolParams(Base):
    __tablename__ = "vol_model_param_history"

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    datetime = sqlalchemy.Column(sqlalchemy.DateTime)
    product = sqlalchemy.Column(sqlalchemy.Text)
    vol_model = sqlalchemy.Column(sqlalchemy.Text)
    spread = sqlalchemy.Column(sqlalchemy.Float)
    var1 = sqlalchemy.Column(sqlalchemy.Float)
    var2 = sqlalchemy.Column(sqlalchemy.Float)
    var3 = sqlalchemy.Column(sqlalchemy.Float)
    var4 = sqlalchemy.Column(sqlalchemy.Float)
    var5 = sqlalchemy.Column(sqlalchemy.Float)
    var6 = sqlalchemy.Column(sqlalchemy.Float)
    ref = sqlalchemy.Column(sqlalchemy.Float)
    saved_by = sqlalchemy.Column(sqlalchemy.Text)


def getRedis():
    if redisLocation == "localhost":
        r = redis.StrictRedis(redisLocation)
        return r
    else:
        r = redis.StrictRedis(
            host=redisLocation, port=redis_port, password=redis_key, db=0, ssl=True
        )
        return r


# redis
conn = getRedis()


# connect a cursor to the desried DB
def ConnectionAzure(server, DB):
    try:
        # for change to prod
        if DB == "FuturesIICOB":
            DB = f2database

        driver = "{ODBC Driver 17 for SQL Server}"
        conn_string = "DRIVER={driver};SERVER={server};DATABASE={db};UID={UID};PWD={pwd};Trusted_Connection=No".format(
            driver=driver, db=f2database, server=f2server, UID=f2userid, pwd=f2password
        )

        cnxn = pyodbc.connect(conn_string)

        return cnxn
    except Exception as e:
        print("Azure Error")
        print(e)


# connect a cursor to the desried DB
def Connection(server, DB):
    # redirect to new azure server
    if server in ["LIVE-ACCSQL", "LIVE-BOSQL1"]:
        return ConnectionAzure(server, DB)

    # redriect to postgres in docker
    if server in ["Sucden-sql-soft"]:
        driver = "PostgreSQL ANSI"
        conn_str = "sslmode=require;DRIVER=PostgreSQL ANSI;DATABASE={db};UID={username};PWD={password};SERVER={server};PORT=5432;".format(
            password=georgiapassword,
            username=georgiauserid,
            db=georgiadatabase,
            server=georgiaserver,
        )
        conn = pyodbc.connect(conn_str)
        # conn.setencoding(encoding='utf-8')
        conn.setdecoding(pyodbc.SQL_CHAR, encoding="utf-8")
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-8")
        return conn

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

    return cnxn


def Cursor(server, DB):
    # sql softs DB connection details
    cnxn = Connection(server, DB)
    cursor = cnxn.cursor()

    return cursor


def connect():
    # Connect to PostgreSQL DBMS
    conn = psycopg2.connect(
        dbname="LME",
        host=postgresLocation,
        user=postgresuserid,
        password=postgrespassword,
        sslmode="require",
    )
    return conn


def PostGresEngine():
    postGresUrl = "postgresql://{username}:{password}@{location}:5432/{db}".format(
        location=postgresLocation,
        db=georgiadatabase,
        password=georgiapassword,
        username=georgiauserid,
    )
    engine = create_engine(postGresUrl)
    return engine


def call_function(function, params=None):
    conn = connect()
    cur = conn.cursor()
    cur.callproc(function, (params))
    response = cur.fetchone()[0]
    return response


def select_from(function, params=None):
    sql = "SELECT * FROM {}()".format(function)
    df = pd.read_sql(sql, PostGresEngine())
    return df


def get_new_postgres_db_engine():
    georgia_frontend_location = os.getenv("GEORGIA_FRONTEND_NEW_TABLE_LOCATION")
    georgia_frontend_username = os.getenv("GEORGIA_FRONTEND_NEW_TABLE_USERNAME")
    georgia_frontend_password = os.getenv("GEORGIA_FRONTEND_NEW_TABLE_PASSWORD")

    connection_url = sqlalchemy.engine.URL(
        "postgresql+psycopg2",
        georgia_frontend_username,
        georgia_frontend_password,
        georgia_frontend_location,
        5432,
        "upe_trading",
    )

    return create_engine(connection_url, connect_args={"sslmode": "require"})
