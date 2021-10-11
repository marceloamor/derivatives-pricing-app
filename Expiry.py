import time, pyodbc
from time import sleep
from sql import pullAllF2Position
from parts import loadLiveF2Trades
import pandas as pd
from datetime import datetime

from parts import allF2Trades, monthSymbol, loadStaticData
from sql import pulltrades, pullPosition

def expiryProcess(product, ref):
    #inputs to be entered from the page
    now = datetime.now().strftime('%Y-%m-%d')

    #load positions for product
    pos = pullPosition(product[:3].lower(), now)

    #filter for just the month we are looking at
    pos = pos[(pos['instrument'].str)[:6]==product]

    # new data frame with split value columns 
    split= pos['instrument'].str.split(" ", n = 2, expand = True) 
  
    # making separate first name column from new data frame 
    pos["strike"]= split[1]
    pos["optionTypeId"]= split[2] 

    #drop futures
    pos =  pos[pos['optionTypeId'].isin(['C','P'])]

    #convert strike to float
    pos["strike"]= pos["strike"].astype(float)

    #seperate into calls and puts
    posC =  pos[pos['optionTypeId']=='C']
    posP =  pos[pos['optionTypeId']=='P']

    #seperate into ITM and OTM
    posIC = posC[posC['strike']<ref]
    posOC = posC[posC['strike']>ref]
    posIP = posP[posP['strike']>ref]
    posOP = posP[posP['strike']<ref]

    #Create Df for out only
    out = pd.concat([posOC, posOP])
    out['action'] = 'Abandon'

    #reverse qty so it takes position out
    out['quanitity'] = out['quanitity'] *-1

    #set price to Zero
    out['price'] = 0
    print(out)

    #go find 3month for underling 
    staticData = loadStaticData()
    staticData = pd.read_json(staticData)
    thirdWed = staticData[staticData['product']== product]
    thirdWed = thirdWed['third_wed'].values[0]
    thirdWed = datetime.strptime(thirdWed, '%d/%m/%Y').strftime('%Y-%m-%d')

    #build future name
    futureName = product[:3] + ' '+ thirdWed

    #build expiry futures trade df
    futC = posIC.reset_index()
    futC['instrument'] = futureName
    futC['prompt'] = thirdWed
    futC['action'] = 'Exercise Future'
    futC['price'] = futC['strike']
    futC['strike'] = None
    futC['optionTypeId'] = None

    futP = posIP.reset_index()
    futP['instrument'] = futureName
    futP['prompt'] = thirdWed
    futP['quanitity'] = futP['quanitity'] *-1
    futP['action'] = 'Exercise Future'
    futP['price'] = futP['strike']
    futP['strike'] = None
    futP['optionTypeId'] = None

    #build conteracting options position df
    posIP['quanitity'] = posIP['quanitity'].values*-1
    posIC['quanitity'] = posIC['quanitity'].values*-1
    posIP['action'] = 'Exercised'
    posIC['action'] = 'Exercised'
    posIP['price'] = 0
    posIC['price'] = 0

    #pull it all together
    all = out.append([futC, futP, posIP, posIC])

    #add trading venue
    all['tradingVenue'] = 'Exercise Process'

    #add trading time
    all['tradeDate'] = pd.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    #drop the columns we dont need
    all.drop(['delta', 'index', 'settlePrice', 'index', 'ID', 'dateTime'],axis=1, inplace=True)

    return all


all = expiryProcess('LCUOQ9', 5989)
all.to_csv(r'P:\Options Market Making\LME\expiry.csv')
