from flask_sqlalchemy import SQLAlchemy
from flask import current_app as app
from sqlalchemy import create_engine
from dotenv import load_dotenv
import sqlalchemy.orm as orm
import pandas as pd
import sqlalchemy
import redis

import os

load_dotenv()


# Flask global data connection
postgresURI = os.environ.get("GEORGIA_POSTGRES_URI")
app.config["SQLALCHEMY_DATABASE_URI"] = postgresURI
# necessary to suppress console warning
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# import engine for non-ORM queries
# import session for ORM queries
db = SQLAlchemy(app)
engine = db.engine
Session = db.session

# Georgia official postgres connection (deprecated after flask global)
georgia_postgres_location = os.getenv("GEORGIA_POSTGRES_LOCATION")
georgia_postgres_username = os.getenv("GEORGIA_POSTGRES_USERNAME")
georgia_postgres_password = os.getenv("GEORGIA_POSTGRES_PASSWORD")
georgia_postgres_database = os.getenv("GEORGIA_POSTGRES_DATABASE")

# Old SQLAlchemy db connection, deprecated after flask global connection standard
# new_db_url = sqlalchemy.engine.URL(
#     "postgresql+psycopg2",
#     georgia_postgres_username,
#     georgia_postgres_password,
#     georgia_postgres_location,
#     5432,
#     georgia_postgres_database,
#     query={},
# )
# engine = create_engine(new_db_url, connect_args={"sslmode": "require"})
# Session = orm.sessionmaker(bind=engine)

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


def getRedis(redisLocation, redis_port=redis_port, redis_key=redis_key):
    if redisLocation == "localhost":
        r = redis.StrictRedis(redisLocation)
        return r
    else:
        r = redis.StrictRedis(
            host=redisLocation, port=redis_port, password=redis_key, db=0, ssl=True
        )
        return r


# redis
conn = getRedis(redisLocation, redis_port, redis_key)


# keep, already using sqlalchemy!!!
def PostGresEngine():
    postGresUrl = "postgresql://{username}:{password}@{location}:5432/{db}".format(
        location=postgresLocation,
        db=georgiadatabase,
        password=georgiapassword,
        username=georgiauserid,
    )
    engine = create_engine(postGresUrl)
    return engine


# keep, used once, can be replaced with sqlalchemy, or not be a function at all
def select_from(function, params=None):
    sql = "SELECT * FROM {}()".format(function)
    df = pd.read_sql(sql, PostGresEngine())
    return df
