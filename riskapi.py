#the risk matrix will take a set of varibles via the API and return a JSON message of the risk matrix for that product.
import pandas as pd
from datetime import date
from calculators import Option, VolSurface
import pickle, datetime, time, math
import orjson as json
from datetime import datetime
import numpy as np

#from TradeClass import VolSurface
from data_connections import conn

def loadStaticData():
    #pull staticdata from redis
    staticData = conn.get('staticData')
    staticData = pd.read_json(staticData)

    #filter for non expired months
    today = datetime.now()
    today = today.strftime('%Y-%m-%d')     
    staticData = staticData[pd.to_datetime(staticData['expiry'],format='%d/%m/%Y').dt.strftime('%Y-%m-%d') > today]

    return staticData

#load delta from redis
def getDelta(product):
    
    delta = conn.get(product.lower()[0:3]+'Delta')
    if delta:
        delta = pickle.loads(delta)
        return delta['quanitity'].sum()
    else: return 0

def getData(product):
    data = conn.get(product.lower())
    return data

#take inputs and include spot and time to buildvolsurface inputs
def buildSurfaceParams(params, product, spot,  exp_date ,eval_date):
    volParams = VolSurface(spot,params['vola'],params['skew'],params['calls'],params['puts'],params['cmax'] ,params['pmax'],exp_date ,eval_date, ref = params['ref'], k = None)
    return volParams

#go to redis and get params for volsurface
def loadVolaData(product):
       product = product.lower() +'Vola'
       new_data = conn.get(product)
       new_data = json.loads(new_data)
       return new_data

def getPortfolioData(portfolio, static):
    products =  static.loc[static['portfolio'] == portfolio, 'product']
    data = {}
    for product in products:
        productData = getData(product)
        if productData:
            productData= json.loads(productData)
            data[product] = productData
    return data

def getParamsList(portfolio, static):
    products =  static.loc[static['portfolio'] == portfolio, 'product']
    data = {}
    for product in products:
        productData =  loadVolaData(product)
        if productData:
            data[product.lower()] = productData
    return data

def getVol(model, strike):
    vol= model.get_basicVol(strike)
    return vol    

def getGreeks(option, strike):
    if not math.isnan(float(strike)):
        in_greeks = option.get_all()
        
        return in_greeks

def resolveGreeks(product, positionGreeks, eval_date, undShock, volShock):
    #filter greeks to required underlying    
    greeks = positionGreeks.loc[positionGreeks.portfolio==product]

    if not greeks.empty: 

        #add eval date
        eval_date = datetime.strptime(eval_date, '%d/%m/%Y')
        greeks.loc[:,'eval_date']= eval_date

        #convert expiry to datetime
        greeks.loc[greeks.index.str[-1:].isin(['c', 'p']), 'expiry']=greeks.loc[greeks.index.str[-1:].isin(['c', 'p']), 'expiry'].apply(lambda x:date.fromtimestamp(x / 1e3))
        greeks.loc[greeks.index.str[-1:].isin(['c', 'p']), 'third_wed']=greeks.loc[greeks.index.str[-1:].isin(['c', 'p']), 'third_wed'].apply(lambda x:date.fromtimestamp(x / 1e3))         

        #fill in greeks for futures
        greeks.loc[~greeks.index.str[-1:].isin(['c', 'p']), ["delta", 'fullDelta']] = 1
        greeks.loc[~greeks.index.str[-1:].isin(['c', 'p']),  ['theta', 'gamma', 'vega', 'skewSense',
        'callSense', 'putSense', 'deltaDecay', 'gammaDecay', 'vegaDecay']] = 0

        def apply_VolSurface(und, vol,sk,c,p,cmax,pmax,exp_date,eval_date):
            if not math.isnan(float(vol)):
                volModel =  VolSurface(s=und, vol=vol, sk=sk, c=c, p=p, cmax=cmax, pmax =pmax,
                        exp_date =exp_date, eval_date = eval_date,k = 0, ref = None)    
                return volModel

        #applt volsurface build         
        greeks['volModel']=np.vectorize(apply_VolSurface)(greeks['und_calc_price'],
         greeks['vol'], greeks['skew'], greeks['calls'], greeks['puts'], greeks['cmax'], greeks['pmax'], greeks['expiry'],
         greeks['eval_date'])

        def apply_option(row):
            model = Option(row['cop'], float(row['und_calc_price'])+float(row['i']), row['strike'], row['eval_date'],
                                     row['expiry'], row['interest_rate'], float(row['vol'])+float(row['j']),
                                     params = row['volModel'])     
            return model        
    
        #greeks.loc[greeks.index.str[-1:].isin(['c', 'p']),"option"] = greeks[greeks.index.str[-1:].isin(['c', 'p'])].apply(apply_option, axis=1)

        #iterate over shocks
        results = {}  
 
        for i in range(len(undShock)):
            volResults = {}
            greeks['i']=undShock[i]
            for j in range(len(volShock)):  
                #calculate greeks for each strikes                 
                greeks['j']=volShock[j]

                greeks.loc[greeks.index.str[-1:].isin(['c', 'p']),"option"] = greeks[greeks.index.str[-1:].isin(['c', 'p'])].apply(apply_option, axis=1)


                greeks['calc_price', 'delta','theta','gamma','vega', 'skewSense', 'callSense',
                'putSense', 'deltaDecay', 'gammaDecay', 'vegaDecay', 'fullDelta', 't'] = np.vectorize(getGreeks)(greeks['option'], greeks['strike'])

                calc_greeks = ['delta', 'theta', 'gamma', 'vega', 'skewSense',
                'callSense', 'putSense', 'deltaDecay', 'gammaDecay', 'vegaDecay',
                'fullDelta']

                multiple_greeks=['theta','vega']

                #calc total greeks including multiplier if required
                for calc_greek in calc_greeks:
                    if calc_greek in multiple_greeks:
                        greeks['total_{}'.format(calc_greek)] = greeks[calc_greek]*greeks['quanitity']*greeks['multiplier']
                    else:     
                        greeks['total_{}'.format(calc_greek)] = greeks[calc_greek]*greeks['quanitity']


                volResults[volShock[j]] = greeks[['total_delta','total_theta','total_gamma','total_vega', 'total_skewSense', 'total_callSense',
                'total_putSense', 'total_deltaDecay', 'total_gammaDecay', 'total_vegaDecay', 'total_fullDelta']].sum().to_dict()
                print(volShock[j])    
                print(greeks[['total_delta']].sum().to_dict())
                
            results[undShock[i]] = volResults      
        
        return results

# #function to calcualte risk
# def risk(static, tdata, vol, und, portfolio, level, eval_date, rel, paramsList):
#     greeks = {}
#     products =  static.loc[static['portfolio'] == portfolio, 'product']
#     deltaProduct = products.values[0]
#     multiplier = static.loc[static['portfolio'] == portfolio, 'multiplier'].values[0]
#     eval_date = datetime.strptime(eval_date[:10], '%m/%d/%Y')  
    
#     tvega= tdelta = ttheta = tgamma = tdeltaDecay = tgammaDecay =  tvegaDecay = tfullDelta =  0
#     for product in products:
#         data = tdata[product]
#         #round now so table only shows 0 dp and futures gets passed to every calculation
#         future = round(float(data['calc_und']),0) + und
#         expiry = data['m_expiry']
#         rf = data['interest_rate']
#         delta = vega = theta = gamma = deltaDecay = gammaDecay =  vegaDecay = fullDelta =0

#         #pull vol params
#         volaParams = paramsList[product.lower()]

#         #build volsurface class
#         volParams = buildSurfaceParams(volaParams, product, future, expiry ,eval_date)
#         #adjust the vol
#         volParams.vol = volParams.vol + vol

#         for strike in data['strikes']:
#                 for CoP in ['C', 'P']:
#                     position = float(data['strikes'][strike][CoP]['position'])
#                     #if no positon then skip instrument
#                     if position != 0:
#                         #add in vola shock to current vol and params
#                         vola = float(data['strikes'][strike][CoP]['vola']) + vol
                        
#                         #build option object
#                         option = Option(CoP, future, strike, eval_date, expiry, rf, vola, now = False, params = volParams)

#                         if level == 'low':
#                             price, cdelta, ctheta, cgamma, cvega = option.get_all_light()
#                             #if relative then remove current greek
#                             if rel == 'rel':
#                                 price = price - float(data['strikes'][strike][CoP]['theo'])
#                                 cdelta =  cdelta - float(data['strikes'][strike][CoP]['delta'])
#                                 ctheta = ctheta - float(data['strikes'][strike][CoP]['theta'])
#                                 cgamma = cgamma - float(data['strikes'][strike][CoP]['gamma'])
#                                 cvega = cvega - float(data['strikes'][strike][CoP]['vega'])
#                         #if looking for high level then calculate extra
#                         elif level == 'high':
#                             price, cdelta, ctheta, cgamma, cvega, cskewSense, ccallSense, cputSense, cdeltaDecay, cgammaDecay, cvegaDecay, cfullDelta = option.get_all()
#                             #if relative then remove current greek
#                             if rel == 'rel':
#                                 price = price - float(data['strikes'][strike][CoP]['theo'])
#                                 cdelta =  cdelta - float(data['strikes'][strike][CoP]['delta'])
#                                 ctheta = ctheta - float(data['strikes'][strike][CoP]['theta'])
#                                 cgamma = cgamma - float(data['strikes'][strike][CoP]['gamma'])
#                                 cvega = cvega - float(data['strikes'][strike][CoP]['vega'])
#                                 cskewSense = cskewSense - float(data['strikes'][strike][CoP]['skewSense'])
#                                 ccallSense = ccallSense -  float(data['strikes'][strike][CoP]['callSense'])
#                                 cputSense = cputSense - float(data['strikes'][strike][CoP]['putSense'])
#                                 cdeltaDecay = cdeltaDecay - float(data['strikes'][strike][CoP]['deltaDecay'])
#                                 cgammaDecay = cgammaDecay - float(data['strikes'][strike][CoP]['gammaDecay'])
#                                 cvegaDecay = cvegaDecay - float(data['strikes'][strike][CoP]['vegaDecay'])
#                                 cfullDelta = cfullDelta - float(data['strikes'][strike][CoP]['fullDelta'])

#                         #add greeks multiplied by postion to rolling totals
#                         delta = delta + (position * cdelta)
#                         vega = vega + (position * cvega) 
#                         theta = theta + (position * ctheta)
#                         gamma = gamma + (position * cgamma)
#                         #if we calculated high level greeks then add them to the rolling totals
#                         if level == 'high':
#                             deltaDecay = deltaDecay + (position * cdeltaDecay)
#                             vegaDecay = vegaDecay + (position * cvegaDecay)
#                             gammaDecay = gammaDecay + (position * cgammaDecay)
#                             fullDelta = fullDelta + (position * cfullDelta)
#                     else: 
#                         continue
                            
#         #apply multipliers
#         vega = float(vega)*multiplier
#         theta = float(theta)*multiplier
#         vegaDecay = float(vegaDecay)*multiplier
#         if level == 'low':
#             greeks[product] = {'delta': "%.2f" % delta, 'vega': "%.2f" % vega, 'theta': "%.2f" % theta, 'gamma': "%.2f" % gamma}
#         elif level == 'high':
#             greeks[product] = {'fullDelta': "%.2f" % fullDelta, 'delta': "%.2f" % delta, 'vega': "%.2f" % vega, 'theta': "%.2f" % theta, 'gamma': "%.2f" % gamma, 'deltaDecay': "%.2f" % deltaDecay, 'vegaDecay': "%.2f" % vegaDecay, 'gammaDecay': "%.2f" % gammaDecay}
#         tvega = tvega + vega
#         tdelta = tdelta + delta
#         ttheta = ttheta + theta
#         tgamma = tgamma + gamma

#         if level == 'high':
#             tdeltaDecay = tdeltaDecay + deltaDecay
#             tvegaDecay = tvegaDecay + vegaDecay
#             tgammaDecay = tgammaDecay + gammaDecay
#             tfullDelta = tfullDelta + fullDelta
#     #if not relative then add in futures delta
#     if rel == 'rel':
#         futDelta = 0
#     else:
#         futDelta = getDelta(deltaProduct)

#     #build outputs to send out.
#     if level =='low': 
#         greeks['Total'] = {'delta': "%.2f" % (tdelta + futDelta), 'vega': "%.2f" % tvega, 'theta': "%.2f" % ttheta, 'gamma': "%.2f" % tgamma}
#     elif level == 'high':
#         greeks['Total'] = {'fullDelta': "%.2f" % (tfullDelta+futDelta), 'delta': "%.2f" % (tdelta + futDelta), 'vega': "%.2f" % tvega, 'theta': "%.2f" % ttheta, 'gamma': "%.2f" % tgamma, 'deltaDecay': "%.2f" % tdeltaDecay, 'vegaDecay': "%.2f" % tvegaDecay, 'gammaDecay': "%.2f" % tgammaDecay, } 
        

#     return greeks['Total']

def runRisk(ApiInputs):
    starttime = time.time()
    #pull in inputs from api call
    portfolio = ApiInputs['portfolio']
    vol = ApiInputs['vol']
    und = ApiInputs['und']
    level  = ApiInputs['level']
    eval  = ApiInputs['eval']
    rel = ApiInputs['rel']

    positionGreeks=pd.read_json(conn.get('greekpositions'))

    results =resolveGreeks(portfolio, positionGreeks, eval, und, vol)

    #print('That took {} seconds'.format(time.time() - starttime))
    results = json.dumps(results)
    return results
    
# def runRisk(ApiInputs):
#     starttime = time.time()
#     #load static data
#     static = loadStaticData()

#     #pull in inputs from api call
#     portfolio = ApiInputs['portfolio']
#     vol = ApiInputs['vol']
#     und = ApiInputs['und']
#     level  = ApiInputs['level']
#     eval  = ApiInputs['eval']
#     rel = ApiInputs['rel']

#     results = {}
#     #get all the data for all the products in the portfolio
#     data = getPortfolioData(portfolio, static)

#     #go get all the vola params for the portfolio
#     paramsList = getParamsList(portfolio, static)

#     positionGreeks=pd.read_json(conn.get('greekpositions'))

#     for i in range(len(und)):
#         volResults = {}
#         for j in range(len(vol)):            
#             volResults[vol[j]] = risk(portfolio, positionGreeks, float(und[i]), float(vol[j]))

#         results[und[i]] = volResults

#     print('That took {} seconds'.format(time.time() - starttime))
#     results = json.dumps(results)
#     return results
    




    
