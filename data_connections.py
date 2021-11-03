from sqlalchemy import create_engine
import pyodbc, redis, os, psycopg2

#connect to redis (default to localhost).
redisLocation = os.getenv('REDIS_LOCATION', default = 'localhost')

postgresLocation = os.getenv('POSTGRES_LOCATION', default = 'georgiatest.postgres.database.azure.com')
postgresuserid = os.getenv('POST_USER', default = 'gareth')
postgrespassword = os.getenv('POST_PASSWORD', default = 'Corona2022!')

riskAPi = os.getenv('RISK_LOCATION', default = 'localhost')
# f2server = os.getenv('F2_SERVER', default = 'f2sqlprod1.761424d6536a.database.windows.net')
# f2database = os.getenv('F2_DATABASE', default = 'FuturesII')
# f2userid = os.getenv('F2_USER', default = 'Georgia')
# f2password = os.getenv('F2_PASSWORD', default = 'j4KYAg!8c]sf5f8Q')

f2server = os.getenv('F2_SERVER', default = 'bulldogmini.postgres.database.azure.com')
f2database = os.getenv('F2_DATABASE', default = 'bulldogmini')
f2userid = os.getenv('F2_USER', default = 'gareth')
f2password = os.getenv('F2_PASSWORD', default = 'Wolve#123')

georgiaserver = os.getenv('GEORGIA_SERVER', default = 'georgiatest.postgres.database.azure.com')
georgiadatabase = os.getenv('GEORGIA_DATABASE', default = 'LME')
georgiauserid = os.getenv('GEORGIA_USER', default = 'gareth')
georgiapassword = os.getenv('GEORGIA_PASSWORD', default = 'Corona2022!')

#redis
conn = redis.Redis(redisLocation)

#connect a cursor to the desried DB
def ConnectionAzure(server, DB):
    try:
        #for change to prod
        if DB == 'FuturesIICOB':
            DB = f2database

        driver= '{ODBC Driver 17 for SQL Server}'    
        conn_string = 'DRIVER={driver};SERVER={server};DATABASE={db};UID={UID};PWD={pwd};Trusted_Connection=No'.format(driver=driver, db=f2database, server=f2server, UID=f2userid, pwd=f2password)

        cnxn = pyodbc.connect(conn_string)
    
        return cnxn 
    except Exception as e:
        print('Azure Error')
        print(e)

#connect a cursor to the desried DB
def Connection(server, DB):
    #redirect to new azure server
    if server in ['LIVE-ACCSQL', 'LIVE-BOSQL1']:    
        return ConnectionAzure(server, DB)

    #redriect to postgres in docker
    if server in ['Sucden-sql-soft']:
        driver = 'PostgreSQL ANSI'
        conn_str="sslmode=require;DRIVER=PostgreSQL ANSI;DATABASE={db};UID={username};PWD={password};SERVER={server};PORT=5432;".format(password=georgiapassword,username=georgiauserid, db=georgiadatabase, server=georgiaserver)             
        conn = pyodbc.connect(conn_str)
        #conn.setencoding(encoding='utf-8')
        conn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')       
        return conn

    #sql softs DB connection details 
    server = str(server)
    database = str(DB)
    driver= '{ODBC Driver 13 for SQL Server}'
    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';Trusted_connection=yes')
      
    return cnxn

def Cursor(server, DB):
    #sql softs DB connection details
    cnxn = Connection(server, DB)
    cursor = cnxn.cursor()

    return cursor

def connect():
    # Connect to PostgreSQL DBMS
    conn = psycopg2.connect(host=postgresLocation, user=postgresuserid, password=postgrespassword,  sslmode='require')
    return conn

def PostGresEngine():
    postGresUrl = 'postgresql://{username}:{password}@{location}:5432/{db}'.format(location=postgresLocation, db=georgiadatabase, password=georgiapassword, username=georgiauserid)
    engine = create_engine(postGresUrl)
    return engine