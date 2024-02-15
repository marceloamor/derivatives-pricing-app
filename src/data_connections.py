import os
from io import BytesIO

import pandas as pd
import redis
import sqlalchemy
import sqlalchemy.orm as orm
from dotenv import load_dotenv
from flask import g
from redis.backoff import ExponentialBackoff
from redis.retry import Retry
from sqlalchemy import create_engine

load_dotenv()


db = g.db

engine = db.engine
Session = db.session


# sql softs DB connection details
postgresLocation = os.getenv(
    "POSTGRES_LOCATION", default="georgiatest.postgres.database.azure.com"
)
postgresuserid = os.getenv("POST_USER", default="gareth")
postgrespassword = os.getenv("POST_PASSWORD", default="CVss*bsh3T")

riskAPi = os.getenv("RISK_LOCATION", default="localhost")

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


def getRedis(redisLocation, redis_port=redis_port, redis_key=redis_key):
    if redisLocation == "localhost":
        r = redis.StrictRedis(redisLocation, decode_responses=True)
        return r
    else:
        r = redis.StrictRedis(
            host=redisLocation,
            port=redis_port,
            password=redis_key,
            db=0,
            ssl=True,
            ssl_cert_reqs="none",
            retry_on_timeout=True,
            retry=Retry(ExponentialBackoff(cap=0.512, base=0.008), 20),
        )
        return r


# redis
conn = getRedis(redisLocation, redis_port, redis_key)


# keep, already using sqlalchemy!!!
def PostGresEngine():
    postGresUrl = (
        "postgresql+psycopg://{username}:{password}@{location}:5432/{db}".format(
            location=postgresLocation,
            db=georgiadatabase,
            password=georgiapassword,
            username=georgiauserid,
        )
    )
    engine = create_engine(postGresUrl)
    return engine


# keep, used once, can be replaced with sqlalchemy, or not be a function at all
def select_from(function, params=None):
    sql = "SELECT * FROM {}()".format(function)
    df = pd.read_sql(sql, PostGresEngine())
    return df


def redis_set_with_pd_pickle(key: str, value):
    """
    Use BytesIO to pandas_pickle a pandas object and store it in redis
    Forwards and backwards compatible between pandas and python versions
    """
    # pickle to bytes io object
    bytesio = BytesIO()
    value.to_pickle(bytesio)

    # bytes to string
    pandas_pickled_string = bytesio.getvalue()

    # set on redis
    conn.set(key, pandas_pickled_string)


def redis_get_with_pd_pickle(key: str):
    """
    Pull pickled pandas object from redis and unpickle it
    Uses BytesIO to pickle and unpickle
    Forwards and backwards compatible between pandas and python versions
    """
    # get pickled string from redis
    pandas_pickled_string = conn.get(key)

    # sdtring back to bytesio
    pandas_pickled_bytesio = BytesIO(pandas_pickled_string)

    # pandas to read pickle
    df_restored = pd.read_pickle(pandas_pickled_bytesio)

    return df_restored
