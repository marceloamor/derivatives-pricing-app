from sqlalchemy import create_engine
import pyodbc, redis, os

#connect to redis (default to localhost).
redisLocation = os.getenv('REDIS_LOCATION', default = 'localhost')
postgresLocation = os.getenv('POSTGRES_LOCATION', default = 'localhost')
F2Location = os.getenv('F2_LOCATION', default = 'localhost')
riskAPi = os.getenv('RISK_LOCATION', default = 'localhost')
postgresLocation = os.getenv('POSTGRES_LOCATION', default = 'localhost')

#redis location
conn = redis.Redis(redisLocation)

#connect a cursor to the desried DB
def ConnectionAzure(server, DB):

    #azure connection details 
    server = 'f2sqlprod1.761424d6536a.database.windows.net'
       
    database = str(DB)

    #for change to prod
    if database == 'FuturesIICOB':
        database = 'FuturesII'

    driver= '{ODBC Driver 17 for SQL Server}'    
    userId = 'Georgia'
    password= 'j4KYAg!8c]sf5f8Q' 
    conn_string = 'DRIVER={driver};SERVER={server};DATABASE={db};UID={UID};PWD={pwd};Trusted_Connection=No'.format(driver=driver, db=database, server=server, UID=userId, pwd=password)

    cnxn = pyodbc.connect(conn_string)
   
    return cnxn    

#connect a cursor to the desried DB
def Connection(server, DB):

    #redirect to new azure server
    if server in ['LIVE-ACCSQL', 'LIVE-BOSQL1']:    
        return ConnectionAzure(server, DB)

    #redriect to postgres in docker
    if server in ['Sucden-sql-soft']:
        conn_str="DRIVER=PostgreSQL ANSI;DATABASE=LME;UID=postgres;PWD=password;SERVER={server};PORT=5432;".format(server=postgresLocation)             
        conn = pyodbc.connect(conn_str)

        #set encoding prefreances for docker instance
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

def PostGresEngine():
    postGresUrl = 'postgresql://postgres:password@{location}:5432/LME'.format(location=postgresLocation)
    engine = create_engine(postGresUrl)
    return engine