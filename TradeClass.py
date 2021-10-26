import math, datetime, time, redis, os
from scipy.stats import norm
import pandas as pd
from datetime import date
from datetime import datetime as dt
import numpy as np
import json
from jinja2 import Template

#connect to redis (default to localhost).
redisLocation = os.getenv('REDIS_LOCATION', default = 'localhost')
conn = redis.Redis(redisLocation)

#Comm="{{Comm}}" 

#fixml Template to add values to
fixmlFuture = """
<FIXML>
  <TrdCaptRpt TxnTm="{{TxnTm}}" TrdId="{{TrdId}}" MLegRptTyp="{{MLegRptTyp}}" TrdDt="{{TrdDt}}" InvSCode="{{InvSCode}}" ExeSCode="{{ExeSCode}}" TrdCap="DEAL" CdyDerivInd="Spec" ExeCCode="{{ExeCCode}}"  SCode="{{SCode}}" CliSCode="{{CliSCode}}" CtryBranchCli="{{CtryBranchCli}}" OrdOrigTrader="{{OrdOrigTrader}}" EnterTrader="{{EnterTrader}}">
    <Instrmt Exch="{{InstrmtExch}}" SecTyp="{{SecTyp}}" ID="{{ID}}" />
    <Pty ID="{{Account}}" R="24" />
    <Pty ID="{{PtyID37}}" R="37" />
    <RptSide Side="{{RptSide}}" MMY="{{MMY}}" LastQty="{{LastQty}}" LastPx="{{LastPx}}" TradedPx="{{TradedPx}}" Markup="{{Markup}}" />
  </TrdCaptRpt>
</FIXML>
"""

fixmlOption = """
<FIXML>
  <TrdCaptRpt TrdId="{{TrdId}}" TxnTm="{{TxnTm}}" MLegRptTyp="{{MLegRptTyp}}" TrdDt="{{TrdDt}}" InvSCode="{{InvSCode}}" ExeSCode="{{ExeSCode}}" TrdCap="DEAL" CdyDerivInd="Spec" ExeCCode="{{ExeCCode}}"  SCode="{{SCode}}" CliSCode="{{CliSCode}}" OrdOrigTrader="{{OrdOrigTrader}}" EnterTrader="{{EnterTrader}}" >
   <Undly UnderlyingPx="{{underlyingPrice}}" />
    <Instrmt Exch="{{InstrmtExch}}" StrkPx="{{StrkPx}}" PutCall="{{PutCall}}" SecTyp="{{SecTyp}}" ID="{{ID}}" />
    <Pty ID="{{Account}}" R="24" />
    <Pty ID="{{PtyID37}}" R="37" />
    <RptSide Side="{{RptSide}}" MMY="{{MMY}}" LastQty="{{LastQty}}" LastPx="{{LastPx}}" Volatility= "{{Volatility}}" Markup="{{Markup}}" />
  </TrdCaptRpt>
</FIXML>
"""

mifidCodes = {'raf' : '12952', 'gareth':'12379', 'alan' : '13404', 'system': '','tom' : '13378','cooey' : '12046'}

#go to redis and get theo for instrument
def get_theo(instrument):
    product = instrument.split(' ')
    data = conn.get(product[0].lower())
    data = json.loads(data)
    if data != None:
        #theo = data['strikes'][instrument[1]][instrument[2]]['theo']
        #data.set_index('instrument', inplace=True)
        
        theo = data[instrument.lower()]['option']['calc_price']

        return float(theo)

class TradeClass(object):
    """holds info on trades entered into the system."""
    
    def __init__(self, id, timestamp, product, strike, CoP, prompt, Price, qty, counterparty, comment, user, venue, theo = None, delta = None, underlying = None):
        self.id = id
        self.timestamp = timestamp
        self.product = product
        self.user = user
        self.qty = float(qty)
        if float(self.qty) > 0: self.BS = 1
        elif float(self.qty) < 0: self.BS = 2
        
        if strike:
            self.strike = strike
        else : self.strike = None
        if CoP:
            self.cop = CoP.upper()
            self.productType = 'O'
        else: 
            self.cop = None
            self.productType = 'F'
        self.prompt = prompt 
        self.price = Price
        self.countPart = counterparty
        self.comment = comment
        self.theo = theo
        self.delta = delta
        self.name = self.buildProductName()
        self.exchange = 'LME'
        self.markUp = '0'
        #self.comm = ''
        self.type = '1'
        self.venue = venue
        self.underlying = underlying
        if self.underlying == None:
            self.underlying = '0.01'
        
        self.mifid = mifidCodes[user]
        #if strike not proved go and find it
        if theo == None and self.strike: 
            try:
                self.theo = get_theo(self.name)
            except KeyError:
                print('Strike {} missing from {}'.format(self.strike,self.product))
        #Set account number based on product or if backend
        if self.countPart == 'BACKBOOK':
            self.account = 90600
        else:
            if product[0:3]== 'LCU':
                self.account = 90601
            elif product[0:3]== 'LZH':
                self.account = 90602
            elif product[0:3]== 'LAD':
                self.account = 90603
            elif product[0:3]== 'PBD':
                self.account = 90604
            elif product[0:3]== 'LND':
                self.account = 90605            

    def buildProductName(self):
        if self.strike == None and self.cop == None:
            return self.product[:3] + ' ' + str(self.prompt)
        else:
            return self.product+' '+str(self.strike)+' '+self.cop

    def fixml(self):
        prompt = datetime.datetime.strptime(self.prompt, '%Y-%m-%d')
        if self.productType == 'F':
            #is future so load future template and add required inputs
            template = Template(fixmlFuture)



            fixml = template.render(
                TxnTm=self.timestamp 
                ,TrdId= self.id
                ,MLegRptTyp= self.type
                ,TrdDt= self.timestamp.strftime('%Y-%m-%d')
                ,InvSCode= self.mifid
                ,ExeSCode= self.mifid
                ,SCode= self.mifid
                ,CliSCode= '11848'
                ,OrdOrigTrader= self.mifid
                ,EnterTrader= self.mifid
                ,InstrmtExch= self.exchange
                ,SecTyp= self.productType
                ,ID= self.product[:3]
                ,PtyID37= self.countPart
                ,RptSide= self.BS
                ,MMY= prompt.strftime('%Y%m%d')
                ,LastQty= int(abs(self.qty))
                ,LastPx = self.price
                ,Markup = self.markUp
                #,Comm = self.comm
                ,Account = self.account
                )
            
        elif self.productType == 'O':
                #is options so load future template and add required inputs
                template = Template(fixmlOption)

                fixml = template.render(
                TxnTm=self.timestamp 
                ,TrdId= self.id  
                ,MLegRptTyp= self.type
                ,TrdDt= self.timestamp.strftime('%Y-%m-%d')
                ,InvSCode= self.mifid
                ,ExeSCode= self.mifid
                ,SCode= self.mifid
                ,CliSCode= '11848'
                ,OrdOrigTrader= self.mifid
                ,EnterTrader= self.mifid
                ,noLegs = '0'
                ,InstrmtExch= self.exchange
                ,StrkPx= self.strike
                ,PutCall= self.cop
                ,SecTyp= self.productType
                ,ID= self.product[:4]
                ,PtyID37= self.countPart
                ,RptSide= self.BS
                #default to first of month for F2
                ,MMY= prompt.strftime('%Y%m')+'01'
                ,LastQty= int(abs(self.qty))                
                ,LastPx = self.price
                ,Volatility= abs(float(self.vol))
                ,Markup = self.markUp
                #,Comm = self.comm
                ,underlyingPrice = self.underlying
                ,Account = self.account
                )
        return fixml

def normcdf(x):
        "cdf for standard normal"
        q = math.erf(x / math.sqrt(2.0))
        return (1.0 + q) / 2.0 

#===============================================================================
# CLASS OPTION  
#===============================================================================
class Option:
    """
    This class will group the different black-shcoles calculations for an opion
    """
    def __init__(self, right, s, k, eval_date, exp_date, rf, vol, price = None, days = None, now=None, params = None):
        self.k = float(k)
        self.s = float(s)
        self.rf = float(rf)
        self.vol = float(vol)
        if self.vol <= 0 or self.vol == None : self.vol = 0.0001 ## Case valuation in zero vol
        
        self.eval_date = eval_date
        self.exp_date = exp_date
        if days == 'b/b':
            self.t = self.businessDays()
            self.dt = (1/250)
            self.trf = self.t + (14/365)
        else:
            self.t = self.calculate_t()
            self.trf = self.t + (14/365)
            self.dt = (1/365)
        if self.t <= 0 : self.t = 0.0001 ## Case valuation in expiration date
        if now:
            now = datetime.datetime.now()
            midnight = datetime.datetime.combine(now.date(), datetime.time())
            seconds = (now - midnight).seconds
            diff = seconds / (365*24*60*60)
            self.t = self.t - diff
        self.price = price
        self.right = right.upper()    
        #volsurface object
        self.params = params        
                 
    def calculate_t(self):
        if isinstance(self.eval_date, str):
            if '/' in self.eval_date:
                (day, month, year) = self.eval_date.split('/')
            else:
                (day, month, year) = self.eval_date[6:8], self.eval_date[4:6], self.eval_date[0:4]
            d0 = date(int(year), int(month), int(day))
        elif type(self.eval_date)==float or type(self.eval_date)==int or type(self.eval_date)==np.float64:
            (day, month, year) = (str(self.eval_date)[6:8], str(self.eval_date)[4:6], str(self.eval_date)[0:4])
            d0 = date(int(year), int(month), int(day))
        elif isinstance(self.eval_date, datetime.date):
            (day, month, year) = self.eval_date.day, self.eval_date.month, self.eval_date.year
            d0 = date(int(year), int(month), int(day))

        else:
            d0 = self.eval_date 
 
        if isinstance(self.exp_date, str):
            if '/' in self.exp_date:
                (day, month, year) = self.exp_date.split('/')
            else:
                (day, month, year) = self.exp_date[6:8], self.exp_date[4:6], self.exp_date[0:4]
            d1 = date(int(year), int(month), int(day))
        elif type(self.exp_date)==float or type(self.exp_date)==int or type(self.exp_date)==np.float64:
            (day, month, year) = (str(self.exp_date)[6:8], str(self.exp_date)[4:6], str(self.exp_date)[0:4])
            d1 = date(int(year), int(month), int(day))
        else:
            d1 = self.exp_date
        #print((d1 - d0).days)
        
        return (d1 - d0).days  / 365

    def businessDays(self):
        if isinstance(self.eval_date, str):
            if '/' in self.eval_date:
                (day, month, year) = self.eval_date.split('/')
            else:
                (day, month, year) = self.eval_date[6:8], self.eval_date[4:6], self.eval_date[0:4]
            d0 = date(int(year), int(month), int(day))
        elif type(self.eval_date)==float or type(self.eval_date)==int or type(self.eval_date)==np.float64:
            (day, month, year) = (str(self.eval_date)[6:8], str(self.eval_date)[4:6], str(self.eval_date)[0:4])
            d0 = date(int(year), int(month), int(day))
        elif isinstance(self.eval_date, datetime.date):
            (day, month, year) = self.eval_date.day, self.eval_date.month, self.eval_date.year
            d0 = date(int(year), int(month), int(day))

        else:
            d0 = self.eval_date 
 
        if isinstance(self.exp_date, str):
            if '/' in self.exp_date:
                (day, month, year) = self.exp_date.split('/')
            else:
                (day, month, year) = self.exp_date[6:8], self.exp_date[4:6], self.exp_date[0:4]
            d1 = date(int(year), int(month), int(day))
        elif type(self.exp_date)==float or type(self.exp_date)==int or type(self.exp_date)==np.float64:
            (day, month, year) = (str(self.exp_date)[6:8], str(self.exp_date)[4:6], str(self.exp_date)[0:4])
            d1 = date(int(year), int(month), int(day))
        else:
            d1 = self.exp_date
        days = np.busday_count( d0, d1 )
       
        return days  / 252
 #black 76
    def get_price_delta(self):
        d1 = ( math.log(self.s/self.k) + (math.pow( self.vol, 2)/2 ) * self.t ) / ( self.vol * math.sqrt(self.t) )
        d2 = d1 - (self.vol * math.sqrt(self.t))
        if self.right == 'C':
            self.calc_price = ( (normcdf(d1) * self.s)  - (normcdf(d2) * self.k ) )* math.exp( -self.rf * self.trf )
            self.delta = normcdf(d1)
        elif self.right == 'P':
            self.calc_price =  ( (-normcdf(-d1) * self.s)  + (normcdf(-d2) * self.k ) )* math.exp( -self.rf * self.trf )
            self.delta = -normcdf(-d1) 

    def get_price_deltaVP(self):

         #if params then find current vola i.e to include change due to vol surface
        if self.params:
            vol = self.params.get_vola(self.k)
            
        else: vol = self.vol

        d1 = ( math.log(self.s/self.k) + (math.pow(vol, 2)/2 ) * self.t ) / (vol * math.sqrt(self.t) )
        d2 = d1 - (vol * math.sqrt(self.t))
        if self.right == 'C':
            self.calc_price = ( (normcdf(d1) * self.s)  - (normcdf(d2) * self.k ) )* math.exp( -self.rf * self.trf )
            self.delta = normcdf(d1)
        elif self.right == 'P':
            self.calc_price =  ( (-normcdf(-d1) * self.s)  + (normcdf(-d2) * self.k ) )* math.exp( -self.rf * self.trf )
            self.delta = -normcdf(-d1) 
         
    def get_theta(self):
        self.t -= self.dt
        if self.t <= 0 : self.t = 0.000001 #for expiry day
        self.get_price_delta()
        self.get_vega()
        self.get_gamma()
        after_gamma = self.gamma
        after_vega = self.vega
        after_price = self.calc_price
        after_delta = self.delta
        self.t += self.dt
        self.get_price_delta()
        self.get_vega()
        self.get_gamma()
        orig_gamma = self.gamma
        orig_vega = self.vega
        orig_price = self.calc_price
        orig_delta = self.delta
        self.theta = (after_price - orig_price) 
        self.deltaDecay = (after_delta - orig_delta) 
        self.gammaDecay = (after_gamma - orig_gamma) 
        self.vegaDecay = (after_vega - orig_vega) 
 
    def get_sensitvities(self):
        a=math.log(self.s/self.k)/(self.vol*math.sqrt(self.t))
        self.skewSense = a
        if a < 0:
            self.callSense = a*a
            self.putSense = 0
        elif a > 0:
            self.putSense = a*a
            self.callSense = 0
        else:
            self.putSense = 0
            self.callSense = 0

    #shock up and down to calc gamma and fulldelta
    def get_gamma(self, ds = 1):
        if self.params:
            self.s += ds
            #store vol pre move to avoid ratchet effect 
            vol =self.vol
            self.get_price_delta()
            after_delta = self.delta
            after_price = self.calc_price
               
            #recalc the vol for the pricing 
            self.params.s = self.s
            after_vol = VolSurface.get_basicVol(self.params,self.k)
        
            self.s -= ds
            #re apply orginal vol
            self.vol = vol
            self.get_price_delta()
            orig_delta = self.delta
            orig_price = self.calc_price

            #recalc the vol for the pricing 
            self.params.s = self.s         
            orig_vol = VolSurface.get_basicVol(self.params,self.k)
        
            #orig_vol = self.vol
            self.gamma = (after_delta - orig_delta) / ds
            deltaAdjust =(after_vol - orig_vol)*100/ds  * (self.vega)
            self.fullDelta = self.delta + deltaAdjust


        else: 
            self.s += ds
            self.get_price_deltaVP()
            after_delta = self.delta
            after_price = self.calc_price
            self.s -= ds
            self.get_price_deltaVP()
            orig_delta = self.delta
            orig_price = self.calc_price
            self.gamma = (after_delta - orig_delta) / ds
            self.fullDelta = (after_price - orig_price) / ds

    def get_vega(self, ds = 0.01):
        self.vol += ds
        self.get_price_delta()
        after_price = self.calc_price
        self.vol -= ds
        self.get_price_delta()
        orig_price = self.calc_price
        self.vega = (after_price - orig_price) / ds/100

    def get_params(self):
        self.skew =math.log(self.s/self.k)/(self.vol*math.sqrt(self.t)) * self.vega
        self.call = math.pow( math.log(self.s/self.k)/(self.vol*math.sqrt(self.t)),2) * self.vega
        self.put = math.pow( math.log(self.s/self.k)/(self.vol*math.sqrt(self.t)),2) * self.vega
 
    def get_all(self):
        #self.get_price_delta()
        self.get_theta()
        #self.get_gamma()
        #self.get_vega()
        self.get_params()
        self.get_sensitvities()
        return self.calc_price, self.delta, self.theta, self.gamma, self.vega, self.skewSense, self.callSense, self.putSense, self.deltaDecay, self.gammaDecay, self.vegaDecay, self.fullDelta 
        
    def get_impl_vol(self):
        """
        This function will iterate until finding the implied volatility
        """
        ITERATIONS = 150
        ACCURACY = 0.005
        low_vol = 0
        high_vol = 1
        self.vol = 0.25  ## It will try mid point and then choose new interval
        self.get_price_delta()
        for i in range(ITERATIONS):
            if self.calc_price > self.price + ACCURACY:
                high_vol = self.vol
            elif self.calc_price < self.price - ACCURACY:
                low_vol = self.vol
            else:
                break
            self.vol = low_vol + (high_vol - low_vol)/2.0
            self.get_price_delta()
 
        return self.vol
 
    def get_price_by_binomial_tree(self):
        """
        This function will make the same calculation but by Binomial Tree
        """
        i =0
        n=30
        deltaT=self.t/n
        u = math.exp(self.vol*math.sqrt(deltaT))
        d=1.0/u
        # Initialize our f_{i,j} tree with zeros
        fs = [[0.0 for j in range(i+1)] for i in range(n+1)]
        a = math.exp(self.rf*deltaT)
        p = (a-d)/(u-d)
        oneMinusP = 1.0-p 
        # Compute the leaves, f_{N,j}
        for j in range(i+1):
            fs[n][j]=max(self.s * u**j * d**(n-j) - self.k, 0.0)
        
 
        for i in range(n-1, -1, -1):
            for j in range(i+1):
                fs[i][j]=math.exp(-self.rf * deltaT) * (p * fs[i+1][j+1] +
                                                        oneMinusP * fs[i+1][j])
        
 
        return fs[0][0]

#===============================================================================
# CLASS OPTIONS STRATEGY
#=============================================================================== 
class Options_strategy:
    """
    This class will calculate greeks for a group of options (called Options Strategy)
    """
    def __init__(self, df_options): 
        self.df_options = df_options       #It will store the different options in a pandas dataframe
 
    def get_greeks(self):
        """
        For analysis underlying (option chain format)
        """
        self.delta = 0
        self.gamma = 0
        self.theta = 0
        for k,v in self.df_options.iterrows():
 
            ## Case stock or future
            if v['m_secType']=='STK':
                self.delta += float(v['position']) * 1
 
            ## Case option
            elif v['m_secType']=='OPT':    
                opt = Option(s=v['underlying_price'], k=v['m_strike'], eval_date=date.today(), # We want greeks for today
                             exp_date=v['m_expiry'], rf = v['interest'], vol = v['volatility'],
                             right = v['m_right'])
 
                price, delta, theta, gamma = opt.get_all()
 
                self.delta += float(v['position']) * delta
                self.gamma += float(v['position']) * gamma
                self.theta += float(v['position']) * theta
 
            else:
                print("ERROR: Not known type")
 
        return self.delta, self.gamma, self.theta 
 
    def get_greeks2(self):
        """
        For analysis_options_strategy
        """
        self.delta = 0
        self.gamma = 0
        self.theta = 0
        for k,v in self.df_options.iterrows():
 
            ## Case stock or future
            if v['m_secType']=='STK':
                self.delta += float(v['position']) * 1
 
            ## Case option
            elif v['m_secType']=='OPT':    
                opt = Option(s=v['underlying_price'], k=v['m_strike'], eval_date=date.today(), # We want greeks for today
                             exp_date=v['m_expiry'], rf = v['interest'], vol = v['volatility'],
                             right = v['m_right'])
 
                price, delta, theta, gamma = opt.get_all()
 
                if v['m_side']=='BOT':
                    position = float(v['position'])
                else:
                    position = - float(v['position']) 
                self.delta += position * delta
                self.gamma += position * gamma
                self.theta += position * theta
 
            else:
                print("ERROR: Not known type")
 
        return self.delta, self.gamma, self.theta 

#===============================================================================
# CLASS VOLATILITY SURFACE
#=============================================================================== 

class VolSurface:
    #This class will calculate vola for strikes given set model inputs
        
    def __init__(self, s=0, vol=0, sk=0, c=0, p=0, cmax=1, pmax =1, exp_date = 0 ,eval_date = 0,k = 0, ref = None):
        self.k = k
        self.ref = float(ref)
        self.s = float(s)
        self.eval_date = eval_date
        self.exp_date = exp_date
        self.vol = float(vol)
        self.t = self.calculate_t()
        self.sk = float(sk)
        self.c = float(c)
        self.p = float(p)
        self.cmax = float(cmax)
        self.pmax = float(pmax)
        if self.vol <= 0: self.vol = 0.000001 ## Case valuation in zero vol
        if self.t <= 0: self.t = 0.000001 ## Case valuation in expiration date

    def calculate_t(self):
        if isinstance(self.eval_date, str):
            if '/' in self.eval_date:
                (day, month, year) = self.eval_date.split('/')
            else:
                (day, month, year) = self.eval_date[6:8], self.eval_date[4:6], self.eval_date[0:4]
            d0 = date(int(year), int(month), int(day))
        elif type(self.eval_date)==float or type(self.eval_date)==int or type(self.eval_date)==np.float64:
            (day, month, year) = (str(self.eval_date)[6:8], str(self.eval_date)[4:6], str(self.eval_date)[0:4])
            d0 = date(int(year), int(month), int(day))
        elif isinstance(self.eval_date, datetime.date):
            (day, month, year) = self.eval_date.day, self.eval_date.month, self.eval_date.year
            d0 = date(int(year), int(month), int(day))

        else:
            d0 = self.eval_date 
 
        if isinstance(self.exp_date, str):
            if '/' in self.exp_date:
                (day, month, year) = self.exp_date.split('/')
            else:
                (day, month, year) = self.exp_date[6:8], self.exp_date[4:6], self.exp_date[0:4]
            d1 = date(int(year), int(month), int(day))
        elif type(self.exp_date)==float or type(self.exp_date)==int or type(self.exp_date)==np.float64:
            (day, month, year) = (str(self.exp_date)[6:8], str(self.exp_date)[4:6], str(self.exp_date)[0:4])
            d1 = date(int(year), int(month), int(day))
        else:
            d1 = self.exp_date
 
        return (d1 - d0).days / 365.0


    def get_basicVol(self, strike):
        a=math.log(self.s/strike)/(self.vol*math.sqrt(self.t))

        if strike > self.s:
            wing = self.c * a**2
            strikeVola = self.vol + (a*self.sk) + wing
            return min(self.cmax, strikeVola)
        elif strike<self.s :
            wing = self.p * a**2
            strikeVola = self.vol + (a*self.sk) + wing
            return min(self.pmax, strikeVola)
        else: 
            wing =0
            strikeVola = self.vol + (a*self.sk) + wing
            return min(self.cmax, strikeVola)

    def get_volPath(self, strike):
        a=math.log(self.ref/strike)/(self.vol*math.sqrt(self.t))

        if strike > self.ref:
            wing = self.c * a**2
            strikeVola = self.vol + (a*self.sk) + wing
            return min(self.cmax, strikeVola)
        elif strike<self.ref :
            wing = self.p * a**2
            strikeVola = self.vol + (a*self.sk) + wing
            return min(self.pmax, strikeVola)
        else: 
            wing =0
            strikeVola = self.vol + (a*self.sk) + wing
            return min(self.cmax, strikeVola)

    def get_vola(self, strike):
        #if a ref has been supplied then calculate the new atm from the VolPath by sending spot to volpath
        if self.ref:
            vol = self.get_volPath(self.s)
        else:
           vol = self.vol
        if vol <= 0: vol = 0.000001

        self.k = strike
        a=math.log(self.s/strike)/(vol*math.sqrt(self.t))

        if self.k > self.s:
            wing = self.c * a**2
            strikeVola = vol + (a*self.sk) + wing
            return min(self.cmax, strikeVola)
        elif self.k<self.s :
            wing = self.p * a**2
            strikeVola = vol + (a*self.sk) + wing
            return min(self.pmax, strikeVola)
        else: 
            wing =0        
            strikeVola = vol + (a*self.sk) + wing
            return min(self.cmax, strikeVola)

