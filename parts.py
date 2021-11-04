import pandas as pd
from pysimplesoap.client import SoapClient
import pickle, math, os, time
from time import sleep
from TradeClass import TradeClass, VolSurface
import ujson as json
import numpy as np
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from sql import sendTrade, deleteTrades, sendPosition, updatePos, updateRedisDelta, updateRedisPos, updateRedisTrade, updateRedisCurve, pulltrades, pullPosition
from data_connections import Connection, Cursor, conn

sdLocation = os.getenv('SD_LOCAITON', default = 'staticdata')
positionLocation = os.getenv('POS_LOCAITON', default = 'greekpositions')

def loadStaticData():
    #pull staticdata from redis
    i=0
    while i<5:
        try:
            staticData = conn.get(sdLocation)   
            staticData = pd.read_json(staticData)
            break         
        except Exception as e:
            time.sleep(1)
            i = i+1    

    #filter for non expired months
    today = datetime.now()+ timedelta(days=1)
    today = today.strftime('%Y-%m-%d')     
    staticData = staticData[pd.to_datetime(staticData['expiry'],format='%d/%m/%Y').dt.strftime('%Y-%m-%d') > today]

    return staticData

def loadRedisData(product):
       new_data = conn.get(product) 
       return new_data

def retriveParams(product):
       paramData = conn.get(product+'Vola') 
       paramData =buildParamsList(paramData)
       return paramData

def loadVolaData(product):
       product = product +'Vola'
       new_data = conn.get(product)
       new_data = json.loads(new_data)
       return new_data

#take inputs and include spot and time to buildvolsurface inputs
def buildSurfaceParams(params,  spot,  exp_date ,eval_date):
    volParams = VolSurface(spot,params['vola'],params['skew'],params['calls'],params['puts'],params['cmax'] ,params['pmax'],exp_date ,eval_date, ref = params['ref'], k = None)
    return volParams

def buildParamsList(Params):
    paramslist = {'spread': 0,'vol':0, 'skew':0, 'call': 0, 'put': 0, 'cmax':0, 'pmax':0, 'ref':0}
    if Params:
        Params = json.loads(Params)
        paramslist['spread'] = Params['spread']
        paramslist['vol'] = Params['vola']
        paramslist['skew'] = Params['skew']
        paramslist['call'] = Params['calls']
        paramslist['put'] = Params['puts']
        paramslist['cmax'] = Params['cmax']
        paramslist['pmax'] = Params['pmax']
        paramslist['ref'] = Params['ref']
    return paramslist

def buildParamMatrix(portfolio):
    staticData = loadStaticData()
    products = staticData[staticData['name']== portfolio]['product']

    params = {}
    for product in products:
        
        #load each month into the params list
        param = retriveParams(product.lower())
        #find prompt
        prompt = staticData[staticData['product']== product]['expiry'].values
        #add prompt to df for sorting later
        param['prompt'] = prompt[0]

        params[product] = (param)


    return pd.DataFrame.from_dict(params, orient='index')
#retive data for a certain product returning dataframe
def retriveTickData(product):
       tickData = conn.get(product.lower()+'MD') 
       if tickData:
           tickData =json.loads(tickData)
           cols =('TimeStamp', 'Price')
           tickData = pd.DataFrame(columns=cols, data=tickData)
           return tickData

def buildTableData(data):
    cols = ( "Cpos", "Ctheo","Cdelta","strikes", "Ppos", "Ptheo", "Vega", "Gamma", "Theta","Volas", "SettleVol", "Skew", "Call", "Put", "Vp", "FD")
    greeks = []
    #for each strike load data from json file
    for strike in data["strikes"]: 
        #oull product mult to convert greeks later
        mult = float(data["mult"])
        Ctheo = float(data["strikes"][strike]['C']['theo'])
        Ptheo = float(data["strikes"][strike]['P']['theo'])
        Cdelta = float(data["strikes"][strike]['C']['delta'])
        vega = float(data["strikes"][strike]['C']['vega'])*mult
        Cposition = float(data["strikes"][strike]['C']['position'])
        Pposition = float(data["strikes"][strike]['P']['position'])
        vola = float(data["strikes"][strike]['P']['vola'])
        #add call or put settlement vol is exists
        if data["strikes"][strike]['P']['settlevol']:
            settleVola =  ((data["strikes"][strike]['P']['settlevol']))
        elif data["strikes"][strike]['C']['settlevol']:
            settleVola =  ((data["strikes"][strike]['C']['settlevol']))
        else: settleVola = 0

        if type(settleVola)== float:
            settleVola = round(settleVola, 4)*100
        else: settleVola = ''
        gamma = (float(data["strikes"][strike]['P']['gamma']))
        skew = (float(data["strikes"][strike]['P']['skewSense']))*vega
        call = (float(data["strikes"][strike]['P']['callSense']))*vega
        put = (float(data["strikes"][strike]['P']['putSense']))*vega
        theta = float(data["strikes"][strike]['P']['theta'])
        vp = float(data["strikes"][strike]['P']['vp'])*100
        fd = float(data["strikes"][strike]['C']['fullDelta'])
      
       
        greeks.append(( Cposition, "%.2f" % Ctheo, "%.2f" % Cdelta,float(strike), Pposition, "%.2f" % Ptheo, "%.2f" % vega, "%.4f" % gamma,  "%.2f" % theta, "%.2f" % (vola*100), settleVola, "%.2f" % skew, "%.2f" % call, "%.2f" % put, "%.4f" % vp, "%.6f" % fd ))
        strikeGreeks = pd.DataFrame(columns=cols, data=greeks)
        strikeGreeks = strikeGreeks.sort_values(["strikes"], ascending = True)
    return strikeGreeks

def buildTradesTableData(data):
    cols = ( "Instrument", "Qty", "Theo", "Prompt", "Forward", "IV","Delta","Gamma", "Vega", "Theta", "Counterparty")
    greeks = []
    Ttheo = Tdelta = Tgamma = Tvega = Ttheta = 0
   
    #for each strike load data from json file
    for instrument in data: 
        qtyCalc = math.fabs((data[instrument]['qty']))
        qty = (data[instrument]['qty'])
        theo = float(data[instrument]['theo'])
        prompt = data[instrument]['prompt']
        forward = data[instrument]['forward']
        IV = float(data[instrument]['iv'])
        delta = round(float(data[instrument]['delta']),2)
        gamma = round(float(data[instrument]['gamma']),3)
        vega = round(float(data[instrument]['vega']),2)
        theta = round(float(data[instrument]['theta']),2)
        counterparty = data[instrument]['counterparty']

        Ttheo = Ttheo +   theo
        Tdelta =Tdelta +delta
        Tgamma = Tgamma + gamma
        Tvega = Tvega + vega
        Ttheta = Ttheta + theta
        
        greeks.append((instrument, qty, theo, prompt, forward, IV, delta, gamma, vega, theta, counterparty ))
    
    greeks.append(('Total', ' ', Ttheo,'', '', '', Tdelta, Tgamma, Tvega, Ttheta, '' ))    
    trades = pd.DataFrame(columns=cols, data=greeks)
    
    return trades

def buildOptionsBoard(data, volPrem):

    if volPrem == 'vol':
        cols = ("Call Bid", "Call Offer", "Strike", "Put Bid", "Put Offer")
        greeks = []
        #for each strike load data from json file
        for strike in data: 
        
            cBidVol = "%.2f" % (data[strike]['C']['bidVol']*100)
            cAskVol = "%.2f" % (data[strike]['C']['askVol']*100)
            pBidVol = "%.2f" % (data[strike]['P']['bidVol']*100)
            pAskVol = "%.2f" % (data[strike]['P']['askVol']*100)     
      
            greeks.append((cBidVol, cAskVol, strike, pBidVol,pAskVol))
            strikeGreeks = pd.DataFrame(columns=cols, data=greeks)
            strikeGreeks = strikeGreeks.sort_values(["Strike"], ascending = True)
        return strikeGreeks
    elif volPrem == 'prem':
        cols = ("Call Bid", "Call Offer", "Strike", "Put Bid", "Put Offer")
        greeks = []
        #for each strike load data from json file
        for strike in data: 
        
            cBidPrem = "%.2f" % (data[strike]['C']['bidPrice'])
            cAskPrem = "%.2f" % (data[strike]['C']['askPrice'])
            pBidPrem = "%.2f" % (data[strike]['P']['bidPrice'])
            pAskPrem = "%.2f" % (data[strike]['P']['askPrice'])     
      
            greeks.append((cBidPrem, cAskPrem, strike, pBidPrem,pAskPrem))
            strikeGreeks = pd.DataFrame(columns=cols, data=greeks)
            strikeGreeks = strikeGreeks.sort_values(["Strike"], ascending = True)
        return strikeGreeks

#go to redis and get theo for instrument
def get_theo(instrument):
    instrument = instrument.split(' ')
    data = conn.get(instrument[0].lower())
    data = json.loads(data)
    if data != None:
        theo = data['strikes'][instrument[1]][instrument[2]]['theo']
        return float(theo)

def productOptions():
    
    df = loadStaticData()
    values = df.name.unique()
    options=[{'label': i, 'value': i} for i in values]
    return options

def pullRates(currency):
    curve = json.loads(conn.get(currency.upper() + 'Rate'))
    curve = pd.DataFrame.from_dict(curve, orient='index')
    return curve

def pullPrompts(product):
    rates = pickle.loads(conn.get(product.lower()+ 'Curve'))
    
    return rates

def redistrades(trade):
    product = trade[1]
    trade  = json.loads(conn.get(product +'trades'))

def pullPortfolioGreeks():
    data = conn.get(positionLocation)
    if data != None:
        greeks = pd.read_json(data)
        return greeks

def monthSymbol(prompt):
    
    month =  prompt.month
    year =  prompt.year
    year = str(year)[-1:]

    if month == 1 : return 'F' + str(year)
    elif month == 2: return 'G' + str(year)
    elif month == 3: return 'H' + str(year)
    elif month == 4: return 'J' + str(year)
    elif month == 5: return 'K' + str(year)
    elif month == 6: return 'M' + str(year)
    elif month == 7: return 'N' + str(year)
    elif month == 8: return 'Q' + str(year)
    elif month == 9: return 'U' + str(year)
    elif month == 10: return 'V' + str(year)
    elif month == 11: return 'X' + str(year)
    elif month == 12: return 'Z' + str(year)

def timeStamp():
    now = datetime.datetime.now()
    now.strftime('%Y-%m-%d %H:%M:%S')
    return now

def sumbiSettings(product, settings):
    
    settings = json.dumps(settings)
    conn.set(product+'Settings', settings)

def retriveSettings(product):
       settings = conn.get(product+'Settings') 
       
       return settings

def callRedis(query):
    data = conn.get(query)
    return data

def loadDeltaPosition(product):
    data = conn.get(product.lower()+'Delta')
    data = pickle.loads(data)
    return data

def portfolioToProduct(portfolio):
    if portfolio.lower() == 'copper':
        return 'lcu'
    elif portfolio.lower() == 'aluminium':
        return 'lad'
    elif portfolio.lower() == 'lead':
        return 'pbd'
    elif portfolio.lower() == 'nickel':
        return 'lnd'
    elif portfolio.lower() == 'zinc':
        return 'lzh'
    else: return 'unkown'

def productToPortfolio(product):
    if product.lower() == 'lcu':
        return 'copper'
    elif product.lower() == 'lad':
        return 'aluminium'
    elif product.lower() == 'pbd':
        return 'lead'
    elif product.lower() == 'lnd':
        return 'nickel'
    elif product.lower() == 'lzh':
        return 'zinc'
    else: return 'unkown'

def pullPnl():
    data = conn.get('pnl')
    return data

def PortfolioPnlTable(data):
    pnl = []
    pTrade = pPos = Total = 0

    for portfolio in data:

        totalPnl = (data[portfolio]['pPos']+data[portfolio]['pTrade'])
        pnl.append({'Portfolio': portfolio.capitalize(), 'Trade Pnl': round(data[portfolio]['pTrade'],2), 'Position PNL':round(data[portfolio]['pPos'],2), 'Total PNL': round(totalPnl,2)})
        pTrade += data[portfolio]['pTrade']
        pPos += data[portfolio]['pPos']
        Total += data[portfolio]['pPos']+data[portfolio]['pTrade']
    pnl.append({'Portfolio': 'Total', 'Trade Pnl': round(pTrade,2), 'Position PNL': round(pPos, 2), 'Total PNL': round(Total,2)}) 
    return pnl

def productPnlTable(data, portfolio):
    pnl = []
    if data:
        for product in data[portfolio]['product']:
            product = product.upper()
            totalPnl = float(data[portfolio]['product'][product]['tPos']+data[portfolio]['product'][product]['tTrade'])


            pnl.append({
             'Product': product,
             'Trade Pnl': round(float(data[portfolio]['product'][product]['tTrade']),2), 
             'Position PNL':round(float(data[portfolio]['product'][product]['tPos']),2),
             'Total PNL': round(float(totalPnl),2) 
            })
           
        return pnl

def strikePnlTable(data, portfolio, product):
    pnl = []
    if data:
        #data = json.loads(data)
        for strike in data[portfolio]['product'][product]['strikes']:

            for cop in ['C','P']:
                pnl.append({'Strike': strike, 'CoP': cop, 'Trade Pnl': data[portfolio]['product'][product]['strikes'][strike][cop]['tradePnl'], 'Position PNL':data[portfolio]['product'][product]['strikes'][strike][cop]['posPnl'], 'Total PNL': (data[portfolio]['product'][product]['strikes'][strike][cop]['posPnl']+data[portfolio]['product'][product]['strikes'][strike][cop]['tradePnl']) })
        return pnl

def unpackRisk(data, greek):
    output = []
    
    for i in data:
        greeks = {}
        greeks['Underlying\Volatility'] = i
        for j in data[i]: 
            greeks[j]= data[i][j][greek]

        output.append(greeks)
    return output

def unpackPriceRisk(data, tm):
    greeks = []
   
    for risk in data[list(data.keys())[0]][' 0.0']:
        greek= {}
        #add greek name to table
        greek['Greek'] = risk.capitalize()
        #iter over prices changes and add greeks value
        for i in data:
            #add three month price to convert from change to absolute
            price = float(i) + tm

            greek[price] = round(data[i][' 0.0'][risk],2)
        
        greeks.append(greek)
        
    return greeks
   
def heatunpackRisk(data, greek):
    output = []
    underlying = []
    
    for i in data:

        greeks = []
        underlying.append(i)
        volaility = []
        for j in data[i]:          
            greeks.append(round(float(data[i][j][greek]),2))
            volaility.append(float(j)*100)
        output.append(greeks)

    return output, underlying, volaility

heampMapColourScale = [
        [0, 'rgb(255, 0, 0)'],
        [0.1, 'rgb(255, 0, 0)'],

        [0.1, 'rgb(255, 0, 0)'],
        [0.2, 'rgb(226, 28, 0)'],

        [0.2, 'rgb(226, 28, 0)'],
        [0.3, 'rgb(198, 56, 0)'],

        [0.3, 'rgb(198, 56, 0)'],
        [0.4, 'rgb(170, 85, 0)'],

        [0.4, 'rgb(170, 85, 0)'],
        [0.5, 'rgb(141, 113, 0)'],

        [0.5, 'rgb(141, 113, 0)'],
        [0.6, 'rgb(113, 141, 0)'],

        [0.6, 'rgb(113, 141, 0)'],
        [0.7, 'rgb(85, 170, 0)'],

        [0.7, 'rgb(85, 170, 0)'],
        [0.8, 'rgb(56, 198, 0)'],

        [0.8, 'rgb(56, 198, 0)'],
        [0.9, 'rgb(28, 226, 0)'],

        [0.9, 'rgb(28, 226, 0)'],
        [1.0, 'rgb(0, 255, 0)']
       ]

def productsFromPortfolio(portfolio):
    staticData = loadStaticData()
    staticData = staticData.loc[staticData['portfolio'] == portfolio]
    staticData = staticData.sort_values('expiry')
    product = staticData['product']

    return product

def curren3mPortfolio(portfolio):
    products = productsFromPortfolio(portfolio)
    product = products.values[0].lower()
    data = loadRedisData(product)
    if data:
        jData = json.loads(data)
        und = list(jData.values())[0]['und_calc_price']
        return und

def getDeltaPrompt(portfolio):
    data = conn.get(portfolio.lower())
    if data:
        data = json.loads(data)
        return data
    
def getOptionDelta(portfolio):
    data = getDeltaPrompt(portfolio)
    deltas = {}
    for product in data:
        if product =='Total' or product == portfolio: continue
        delta = data[product]['delta']
        prompt = datetime.datetime.strptime(data[product]['und_prompt'],'%d/%m/%Y').strftime('%d-%m-%Y')
        deltas[prompt] = {'delta': delta}

    return deltas

def optionPrompt(product):
    staticData = loadStaticData()
    #staticData = pd.read_json(staticData)
    staticdata = staticData.loc[staticData['product'] == product.upper()]
    staticdata = staticdata['third_wed'].values[0]
    date = staticdata.split('/')
    prompt = date[2]+'-'+date[1]+'-'+date[0]
    return prompt

def saveF2Trade(df, user):

    #create st to record which products to update in redis 
    redisUpdate = set([])    
    for row in df.iterrows():

        if row[1]['optionTypeId'] in ['C', 'P'] :
            #is option
            product = row[1]['productId']+monthSymbol(row[1]['prompt'])  
            redisUpdate.add(product[:6])
            prompt = optionPrompt(product)
            trade = TradeClass(0, row[1]['tradeDate'], product, int(row[1]['strike']) , row[1]['optionTypeId'], prompt, float(row[1]['price']), row[1]['quanitity'], 'Trade Transfer', '', user, row[1]['tradingVenue'])
            #send trade to SQL and update all redis parts
            trade.id = sendTrade(trade)
            updatePos(trade)
        else: 
            #is underlying
            product = row[1]['productId']+ ' '+ row[1]['prompt'].strftime('%Y-%m-%d')
            redisUpdate.add(product[:6])
            trade = TradeClass(0, row[1]['tradeDate'], product, None , None, row[1]['prompt'].strftime('%Y-%m-%d'), float(row[1]['price']), row[1]['quanitity'], 'Trade Transfer', '', user, row[1]['tradingVenue'])
            #send trade to SQL and update all redis parts
            trade.id = sendTrade(trade)
            updatePos(trade)

  # update redis for delta, pos trades and the prompt curve
    for update in redisUpdate: 
        updateRedisDelta(update)
        updateRedisPos(update)
        updateRedisTrade(update)
        updateRedisCurve(update)

def saveF2Pos(df, user):

    redisUpdate = set([])
    
    for row in df.iterrows():
        
        if row[1]['optionTypeId'] in ['C', 'P'] :
            #is option
            product = row[1]['productId']+monthSymbol(row[1]['prompt'])  
            redisUpdate.add(product[:6])
            prompt = optionPrompt(product)
            trade = TradeClass(0, timeStamp(), product, int(row[1]['strike']) , row[1]['optionTypeId'], prompt, float(row[1]['price']), row[1]['quanitity'], 'F2 Transfer', '', user, '')
            #send trade to SQL and update all redis parts
            updatePos(trade)
        else: 
            #is underlying
            product = row[1]['productId']+ ' '+ row[1]['prompt'].strftime('%Y-%m-%d')
            redisUpdate.add(product[:6])
            trade = TradeClass(0, timeStamp(), product, None , None, row[1]['prompt'].strftime('%Y-%m-%d'), float(row[1]['price']), row[1]['quanitity'], 'F2 Transfer', '', user,'')
            #send trade to SQL and update all redis parts

            updatePos(trade)

    for update in redisUpdate: 
        updateRedisDelta(update)
        updateRedisPos(update)
        updateRedisTrade(update)        
    
def loadLiveF2Trades():
    #todays date in F2 format
    today = pd.datetime.now().strftime("%Y-%m-%d")

    #delete trades from sql
    deleteTrades(today)
 
    #import .csv F2 value
    csvLocation = 'P:\\Options Market Making\\LME\\F2 reports\\Trading Activity - LME Internal.csv'
    df = pd.read_csv(csvLocation, thousands = ',', parse_dates=['prompt'], dayfirst = True)

    #list of tapos to ignore
    tapos = ['TCAO', 'TADO', 'TZSO', 'TNDO']

    #remove tapos
    df = df[~df['productid'].isin(tapos)]

    #convert tradedate column to datetime
    df['tradeDate'] = pd.to_datetime(df['tradeDate'], format="%d/%m/%Y %H:%M:%S")

    #filter for tradedates we want
    df = df[df['tradeDate']>=today]

    #filter for columns we want
    df = df[['tradeDate', 'productid', 'prompt', 'type', 'strike', 'originalLots', 'price', 'tradingVenue']]

    #split df into options and futures
    dfOpt =  df[df['type'].isin(['C','P'])]
    dfFut = df[df['type'].isin(['C','P'])==  False]

    #change prompt to datetime 
    dfFut['prompt'] = pd.to_datetime(dfFut['prompt'], dayfirst=True)
    dfOpt['prompt'] = pd.to_datetime(dfOpt['prompt'], dayfirst=True)
    
    #round strike to 2 dp
    dfOpt['strike'] = dfOpt['strike'].astype(float)
    dfOpt.strike = dfOpt.strike.round(2)

    #append dataframes together
    df = pd.concat([dfFut,dfOpt]) 

    #filter for columns we want
    df = df[['tradeDate', 'productid', 'prompt', 'type', 'strike', 'originalLots', 'price', 'tradingVenue']]

    #convert price to float from string with commas
    df['price'] = df['price'].astype(float)

    #return to camelcase
    df = df.rename(columns={'productid': 'productId', 'type': 'optionTypeId', 'originalLots': 'quanitity'})
  
    saveF2Trade(df, 'system')

def allF2Trades():
    #todays date in F2 format
    today = pd.datetime.now().strftime("%Y-%m-%d")

    #import .csv F2 value
    csvLocation = 'P:\\Options Market Making\\LME\\F2 reports\\Trading Activity - LME Internal.csv'
    df = pd.read_csv(csvLocation, thousands = ',', parse_dates=['prompt'], dayfirst = True)

    #convert tradedate column to datetime
    df['tradeDate'] = pd.to_datetime(df['tradeDate'], format="%d/%m/%Y %H:%M:%S")

    #filter for tradedates we want
    df = df[df['tradeDate']>=today]

    #filter for columns we want
    df = df[['productid', 'prompt', 'type', 'strike', 'originalLots', 'price', 'tradingVenue']]

    #split df into options and futures
    dfOpt =  df[df['type'].isin(['C','P'])]
    dfFut = df[df['type'].isin(['C','P'])==  False]

    #change prompt to datetime 
    dfFut['prompt'] = pd.to_datetime(dfFut['prompt'], dayfirst=True, format='%d/%m/%Y')
    dfOpt['prompt'] = pd.to_datetime(dfOpt['prompt'], dayfirst=True, format='%d/%m/%Y')

    #round strike to 2 dp
    dfOpt['strike'] = dfOpt['strike'].astype(float)
    dfOpt.strike = dfOpt.strike.round(2)

    #append dataframes together
    df = pd.concat([dfFut,dfOpt]) 

    #filter for columns we want
    df = df[['productid', 'prompt', 'type', 'strike', 'originalLots', 'price', 'tradingVenue']]

    #convert price to float from string with commas
    df['price'] = df['price'].astype(float)

    #return to camelcase
    df = df.rename(columns={'productid': 'productId', 'type': 'optionTypeId', 'originalLots': 'quanitity'})
    
    return df

def convertInstrumentName(row):
    if row['optionTypeId'] in ['C', 'P'] :
        #is option
        product = row['productId']+monthSymbol(row['prompt']) + ' '+ str(int(row['strike'])) + ' ' +  row['optionTypeId']
    else: 
        #is underlying
        product = row['productId']+ ' '+ str(row['prompt'].strftime('%Y-%m-%d'))
    return product

def recTrades():
    #compare F2 and Georgia trades and show differences 

    #pull f2 trades
    #todays date in F2 format
    today = pd.datetime.now().strftime("%Y-%m-%d")
    gTrades = pulltrades(today)

    #convert quanitity to int
    gTrades['quanitity'] = gTrades['quanitity'].astype(int)

    #round price to 2 dp
    gTrades['price'] = gTrades['price'].round(2)

    #filter for columns we want
    gTrades = gTrades[['instrument', 'price', 'quanitity', 'prompt', 'venue']]

    #filter out expiry trades
    #gTrades = gTrades[gTrades['venue'] !='Expiry Process']

    #pull F2 trades
    fTrades = allF2Trades()

    #convert prompt to date
    fTrades['prompt'] = pd.to_datetime(fTrades['prompt'],  dayfirst=True, format='%d/%m/%Y')

    #build instrument name from parts
    fTrades['instrument'] = fTrades.apply(convertInstrumentName, axis = 1)
    #filter for columns we want
    fTrades = fTrades[['instrument', 'price', 'quanitity', 'prompt', 'tradingVenue']]

    #rename tradingVenue as venue
    fTrades = fTrades.rename(columns={'tradingVenue': 'venue'})

    #concat and group then take only inputs with groups of 1
    all = pd.concat([gTrades, fTrades])
    all = all.reset_index(drop= True)
    all_gpby = all.groupby(list(['instrument', 'price', 'quanitity']))
    idx = [x[0] for x in all_gpby.groups.values() if len(x) % 2 != 0]
    all.reindex(idx)

    return all.reindex(idx)

def loadSelectTrades():
    #todays date in F2 format
    today = pd.datetime.now().strftime("%Y-%m-%d")

    #find time of last select trade pulled
    trades = pulltrades(today)
    trades = trades[trades['venue']== 'Select']
    trades.sort_values('dateTime', ascending = False)
    lastTime = trades['dateTime'].values[0]
 
    #import .csv F2 value
    csvLocation = 'P:\\Options Market Making\\LME\\F2 reports\\Trading Activity - LME Internal.csv'
    df = pd.read_csv(csvLocation, thousands = ',')

    #convert tradedate column to datetime
    df['tradeDate'] = pd.to_datetime(df['tradeDate'], format="%d/%m/%Y %H:%M:%S")
    #df['tradeDate'] = df['tradeDate'].astype(str)
    
    #filter for tradedates we want
    df = df[df['tradeDate']>lastTime]

    #filter for just sleect trades
    df = df[df['tradingVenue']== 'Select']

    #filter for columns we want
    df = df[['tradeDate', 'productid', 'prompt', 'type', 'strike', 'originalLots', 'price', 'tradingVenue']]

    #split df into options and futures
    dfOpt =  df[df['type'].isin(['C','P'])] 
    dfFut = df[df['type'].isin(['C','P'])==  False]

    #change prompt to datetime 
    dfFut['prompt'] = pd.to_datetime(dfFut['prompt'], format='%d/%m/%y')
    dfOpt['prompt'] = pd.to_datetime(dfOpt['prompt'], format='%d/%m/%y')

    #round strike to 2 dp
    dfOpt['strike'] = dfOpt['strike'].astype(float)
    dfOpt.strike = dfOpt.strike.round(2)

    #append dataframes together
    df = pd.concat([dfFut,dfOpt]) 

    #filter for columns we want
    df = df[['tradeDate', 'productid', 'prompt', 'type', 'strike', 'originalLots', 'price', 'tradingVenue']]

    #convert price to float from string with commas
    df['price'] = df['price'].astype(float)

    #return to camelcase
    df = df.rename(columns={'productid': 'productId', 'type': 'optionTypeId', 'originalLots': 'quanitity'})
    
    
    saveF2Trade(df, 'system')

def loadLiveF2Trades2():
    #todays date in F2 format
    today = pd.datetime.now().strftime("%Y-%m-%d")

    #delete trades from sql
    deleteTrades(today)
 
    #import .csv F2 value
    csvLocation = 'P:\\Options Market Making\\LME\\F2 reports\\Detailed Open Position.csv'
    df = pd.read_csv(csvLocation)

    #convert tradedate column to datetime
    df['tradedate'] = pd.to_datetime(df['tradedate'], format="%d/%m/%y")

    #filter for tradedates we want
    df = df[df['tradedate']>=today]

    #filter for columns we want
    df = df[['tradeDate','productid', 'prompt', 'optiontypeid', 'strike', 'lots', 'price']]

    #split df into options and futures
    dfOpt =  df[df['optiontypeid'].isin(['C','P'])]
    dfFut = df[df['optiontypeid'].isin(['C','P'])==  False]

    #change prompt to datetime 
    dfFut['prompt'] = pd.to_datetime(dfFut['prompt'], format="%d/%m/%Y")
    dfOpt['prompt'] = pd.to_datetime(dfOpt['prompt'], format="%d/%m/%Y")

    #round strike to 2 dp
    dfOpt['strike'] = dfOpt['strike'].astype(float)
    dfOpt.strike = dfOpt.strike.round(2)

    #append dataframes together
    df = pd.concat([dfFut,dfOpt]) 

    #filter for columns we want
    df = df[['tradeDate','productid', 'prompt', 'optiontypeid', 'strike', 'lots', 'price']]

    #convert price to float from string with commas
    df['price'] = df['price'].str.replace(",", "").astype(float)

    #return to camelcase
    df = df.rename(columns={'productid': 'productId', 'optiontypeid': 'optionTypeId', 'lots': 'quanitity'})

    saveF2Trade(df, 'system')

def readModTime():
    filePath = 'P:\\Options Market Making\\LME\\F2 reports\\Trading Activity - LME Internal.csv'
    modTimesinceEpoc = os.path.getmtime(filePath)
 
    # Convert seconds since epoch to readable timestamp
    modificationTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(modTimesinceEpoc))
    return modificationTime

def SetModTime(modificationTime):
    #save time to redis for comparison later. 
    conn.set('f2LiveUpdate',modificationTime)

def sendFIXML(fixml):
    #take WSDL and create connection to soap server
    WSDL_URL = 'http://live-boapp1.sucden.co.uk/services/Sucden.TradeRouter.svc?singleWsdl'
    client = SoapClient(wsdl=WSDL_URL, ns="web", trace=True)

    #Discover operations
    list_of_services = [service for service in client.services]
    
    #Discover params
    method = client.services['TradeRouterService']

    #send fixml to canroute trade and print the response
    response = client.RouteTrade(source='MetalVolDesk', fixml= fixml)
    
    return response['RouteTradeResult']

def tradeID():
    epoch_time = time.time()*(10**6)
    epoch_time = str(int(epoch_time))[-12:] 
    tradeID = 'MVD'+epoch_time
    sleep(0.000001)
    return tradeID

def productToPortfolio(product):
    if product.lower() == 'lcu':
        return 'copper'
    elif product.lower() == 'lad':
        return 'aluminium'
    elif product.lower() == 'pbd':
        return 'lead'
    elif product.lower() == 'lnd':
        return 'nickel'
    elif product.lower() == 'lzh':
        return 'zinc'
    else: return 'unkown'

def is_between(time, time_range):
    if time_range[1] < time_range[0]:
        return time >= time_range[0] or time <= time_range[1]
    return time_range[0] <= time <= time_range[1]

def ringTime():
    now = datetime.now().time()
    if is_between(str(now), ("12:30", "12:35")):
        return 'Copper Ring 2'
    elif is_between(str(now), ("12:35", "12:40")):
        return 'Aluminium alloy Ring 2'
    elif is_between(str(now), ("12:40", "12:45")):
        return 'Tin Ring 2'
    elif is_between(str(now), ("12:45", "12:50")):
        return 'Lead Ring 2'
    elif is_between(str(now), ("12:50", "12:55")):
        return 'Zinc Ring 2'
    elif is_between(str(now), ("12:55", "13:00")):
        return 'Aluminium Ring 2'
    elif is_between(str(now), ("13:00", "13:05")):
        return 'Nickel Ring 2'
    elif is_between(str(now), ("13:05", "13:10")):
        return 'Aluminium Premiums Ring 2'
    elif is_between(str(now), ("13:10", "13:15")):
        return 'Steel Billet Ring 2'
    elif is_between(str(now), ("13:15", "13:25")):
        return 'Interval'
    elif is_between(str(now), ("13:25", "13:35")):
        return 'Kerb Trading'
    elif is_between(str(now), ("13:35", "14:55")):
        return 'Lunch Interval'
    elif is_between(str(now), ("14:55", "16:15")):
        return 'Afternoon Rings'
    elif is_between(str(now), ("16:15", "16:25")):
        return 'Zinc Kerb'
    elif is_between(str(now), ("16:25", "16:30")):
        return 'Tin Kerb'
    elif is_between(str(now), ("16:30", "16:35")):
        return 'Lead Kerb'
    elif is_between(str(now), ("16:35", "16:40")):
        return 'Cobalt Kerb'
    elif is_between(str(now), ("16:40", "16:45")):
        return 'Aluminium Kerb'
    elif is_between(str(now), ("16:45", "16:50")):
        return 'Aluminium Derivatives Kerb'
    elif is_between(str(now), ("16:50", "16:55")):
        return 'Copper Kerb'
    elif is_between(str(now), ("16:55", "17:02")):
        return 'Nickel Kerb'

#inters over all keys in redis and deletes all with "pos" in key
def deleteRedisPos():
    for key in conn.keys():
        key = key.decode('utf-8')
        if key[-3:] == 'Pos':
            conn.delete(key)

def onLoadPortFolio():
    staticData = loadStaticData()
    portfolios = []
    for portfolio in staticData.portfolio.unique() :
        portfolios.append({'label': portfolio.capitalize(), 'value': portfolio})
    return portfolios

def onLoadPortFolioAll():
    staticData = loadStaticData()
    portfolios = []
    for portfolio in staticData.portfolio.unique() :
        portfolios.append({'label': portfolio.capitalize(), 'value': portfolio})
    portfolios.append({'label': 'All', 'value': 'all'})    
    return portfolios

def strikeRisk(portfolio, riskType, relAbs):
    #pull list of porducts from static data
    static = conn.get('staticData')
    #static = json.loads(static)
    df = pd.read_json(static)
    products =  df[df['portfolio'] == portfolio]['product'].values

    #setup greeks and products bucket to collect data
    greeks = []
    productset = []

    if relAbs == 'strike':

        #for each product collect greek per strike
        for product in products:
            productset.append(product)
            data = conn.get(product.lower())
            if data:
                data = json.loads(data)
                strikegreeks = []
                #go over strikes and uppack greeks 
                strikeset = []

                for strike in data["strikes"]: 
                    #pull product mult to convert greeks later
                    strikeset.append(strike)
                    mult = float(data["mult"])

                    Cposition = float(data["strikes"][strike]['C']['position'])
                    Pposition = float(data["strikes"][strike]['P']['position'])

                    netPos = Cposition +Pposition

                    if netPos != 0:
                        #calc combinded greeks
                        if riskType == 'delta':
                            risk = float(data["strikes"][strike]['C'][riskType])*netPos
                        elif riskType in ['skew', 'call', 'put']:
                            risk = float(data["strikes"][strike]['C'][riskType])*netPos * float(data["strikes"][strike]['C']['vega'])
                        elif riskType =='position':
                            risk = netPos
                        else:
                            risk = float(data["strikes"][strike]['C'][riskType])*mult *netPos
                        risk = round(risk, 2)
                    else:
                        risk = 0
                    strikegreeks.append(risk)
                greeks.append(strikegreeks)

        return greeks, productset, strikeset
    elif relAbs == 'bucket':
        for product in products:
            productset.append(product)
            data = conn.get(product.lower())
            if data:
                data = json.loads(data)
                strikegreeks = []
                #go over strikes and uppack greeks 
                strikeset = []
                
def newstrikeRisk(portfolio, riskType, relAbs):
    #pull list of porducts from static data
    static = conn.get('staticData')
    df = pd.read_json(static)
 
    products =  df[df['portfolio'] == portfolio]['product'].values

    #setup greeks and products bucket to collect data
    greeks = []
    productset = []
    strikegreeks = []
    if relAbs == 'strike':
        strikeset = []
        #for each product collect greek per strike
        for product in products:            
            data = conn.get(product.lower())
            
            if data:
                data = json.loads(data)
                strikegreeks = []
                #go over strikes and uppack greeks 

                #turn strikes into DF
                #df= pd.Panel(data["strikes"]).to_frame().reset_index()    
                df= pd.Panel.from_dict(data["strikes"], orient = 'minor').to_frame().reset_index()                    
                #check if postion is empty for calls nad puts 
                call = all( df[df['major']=='position']['C'].astype(float).values == 0)
                put = all( df[df['major']=='position']['P'].astype(float).values == 0)
                #if no position then skip the product
                if  call and  put:

                    continue
                else:
                    #add prodcut to list
                    productset.append(product)
                
                    #add net pos column
                    netPos = (df[df['major']=='position']['C'].astype(float).add(df[df['major']=='position']['P'].astype(float))).values

                    #get mult
                    mult = float(data["mult"])

                    # convert just columns "C" and "P"
                    df[["C", "P"]] = df[["C", "P"]].apply(pd.to_numeric)

                    #calculate required risktype
                    if riskType == 'delta':
                        risk= (df[df['major']==riskType]['C'].values*df[df['major']=='position']['C'].values)+(df[df['major']=='delta']['P'].values*df[df['major']=='position']['P'].values)
                        strikeset = df[df['major']=='riskType']['minor'].values

                    elif riskType == 'fullDelta':
                        risk = (df[df['major']==riskType]['C'].values*df[df['major']=='fullDelta']['C'].values)+(df[df['major']=='fullDelta']['P'].values*df[df['major']=='fullDelta']['P'].values)
                        strikeset = df[df['major']==riskType]['minor'].values
                
                    elif riskType == 'position':
                        risk = netPos
                        strikeset = df[df['major']=='position']['minor'].values

                    elif riskType in ['skew', 'call', 'put']:
                        risk = df[df['major']==riskType]['C'].values *  netPos * df[df['major']=='vega']['C'].values * mult
                        strikeset = df[df['major']==riskType]['minor'].values
                    elif riskType in ['gamma']:
                        risk = df[df['major']==riskType]['C'].values *  netPos 
                        strikeset = df[df['major']==riskType]['minor'].values
                    else:
                        risk = df[df['major']==riskType]['C'].values *  netPos * mult                
                        strikeset = df[df['major']==riskType]['minor'].values
                    risk = list(np.around(np.array(risk),2))

                #combine risk with other products 
                greeks.append(risk)

        return greeks, productset, list(strikeset)

def timeStamp():
    now = datetime.now()
    now.strftime('%Y-%m-%d %H:%M:%S')
    return now

def sendMessage(text, user, messageTime):
    message = {'user': user, 'message': text, 'prority': 'open'}
    
    #pull current messages if not return empty dict
    oldMessages = conn.get('messages')
    if oldMessages:
        oldMessages = json.loads(oldMessages)
    else: oldMessages = {}
    #combine old and new messages 
    oldMessages[str(messageTime)] = message
    
    oldMessages = json.dumps(oldMessages)
    conn.set('messages', oldMessages)

def pullMessages():
    #get messages from redis 
    messages = conn.get('messages')
    #if messages unpack and send back
    if messages:
        #messages = pd.read_json(messages)
        messages = json.loads(messages)
        
        return messages
    
#takes SD and gives vola
def volCalc(a, atm, skew, call, put, cMax, pMax):
    vol = atm + (a*skew)
    if a <0:
        kurt = a*a*call
        vol = vol +kurt
        vol = min(vol, cMax)
    elif a>0:
        kurt = a*a*put
        vol = vol +kurt
        vol = min(vol, pMax)

    return round(vol*100,2)

def sumbitVolas(product, data):  
    #send new data to redis     
    dict = json.dumps(data)
    conn.set(product+'Vola', dict)
    #inform options engine about update
    pic_data = pickle.dumps([product, 'update'])
    conn.publish('compute',pic_data)

def expiryProcess(product, ref):
    ##inputs to be entered from the page
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

    #remove partials
    posPartial = pos[pos['strike']==ref]
    posPartial['action'] = 'Partial'
    #reverse qty so it takes position out
    posPartial['quanitity'] = posPartial['quanitity'] *-1
    posPartial['price'] = 0

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

    #go find 3month for underling 
    staticData = loadStaticData()
    #staticData = pd.read_json(staticData)
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
    all = out.append([futC, futP, posIP, posIC, posPartial])

    #add trading venue
    all['tradingVenue'] = 'Exercise Process'

    #add trading time
    all['tradeDate'] = pd.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    #drop the columns we dont need
    all.drop(['delta', 'index', 'settlePrice', 'index', 'ID', 'dateTime'],axis=1, inplace=True)

    return all

def deletePosRedis(portfolio):
    data =loadStaticData
    products = data.loc[data['portfolio'] == portfolio]['product']
    for product in products:
        conn.delete(product.lower()+'Pos')

def sendEmail(product):

    # create message object instance
    msg = MIMEMultipart()

    #message destination
    strFrom ='gareth.upe@sucfin.com'
    strTo = 'metalsoptions@sucfin.com'

    msg['Subject'] = product + 'Risk'
    msg['From'] = strFrom
    msg['To'] = strTo

    # attach image to message body
    msg.attach((file(r'P:\Options Market Making\LME\images\fig1.jpeg').read()))
    msg.attach((file(r'P:\Options Market Making\LME\images\data1.html').read()))

    #create server instance
    smtp = smtplib.SMTP()
    smtp.connect('stimpy', 25)
    smtp.login("gareth.upe@sucfin.com", "Sucden2021!")
    smtp.sendmail(strFrom, strTo, msgRoot.as_string())
    smtp.quit()

def pullCurrent3m():
    date = conn.get('3m')
    date = pickle.loads(date)
    return date

def codeToName(product):
    product = product[0:3]
    if product== 'LCU':
        return 'Copper'
    elif product== 'LZH':
        return 'Zinc'
    elif product== 'LAD':
        return 'Aluminium'
    elif product== 'PBD':
        return 'Lead'
    elif product== 'LND':
        return 'Nickel'    

def codeToMonth(product):
    product = product[4]
    if product== 'F':
        return 'Jan'
    elif product== 'G':
        return 'Feb'
    elif product== 'H':
        return 'Mar'
    elif product== 'J':
        return 'Apr'
    elif product== 'K':
        return 'May'    
    elif product== 'M':
        return 'Jun' 
    elif product== 'N':
        return 'Jul' 
    elif product== 'Q':
        return 'Aug' 
    elif product== 'U':
        return 'Sep' 
    elif product== 'V':
        return 'Oct' 
    elif product== 'X':
        return 'Nov' 
    elif product== 'Z':
        return 'Dec' 

def onLoadPortfolio():
    staticData = loadStaticData()

    portfolios = []
    for portfolio in staticData.portfolio.unique() :
        portfolios.append({'label': portfolio, 'value': portfolio})
    return portfolios

def onLoadProduct():
    try:
        staticData = loadStaticData()
        products = []
        for product in set(staticData['product']):
            products.append({'label': product, 'value': product})
        return  products
    except Exception as e:
        return {'label': 'Error', 'value': 'Error'}

def onLoadProductMonths(product):
    #load staticdata
    staticData = loadStaticData()
    #convert to shortname
    staticData = staticData.loc[staticData['f2_name'] == product]
    #sort data
    staticData['expiry'] = pd.to_datetime(staticData['expiry'], dayfirst = True)
    staticData = staticData.sort_values(by=['expiry'])
    #create month code from product code
    productNames = set()
    productNames = [x[4:] for x in staticData['product']]

    products = []
    for product in productNames:
        products.append({'label': product, 'value': product})
    products.append({'label': '3M', 'value': '3M'})
    return  products, products[0]['value']

