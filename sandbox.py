import redis, pickle 
from data_connections import conn, call_function, select_from
from parts import loadStaticData, onLoadProductMonths
from parts import pullPortfolioGreeks, loadStaticData, pullPrompts, onLoadPortFolio
from datetime import date

def lme_to_georgia(product, series):
    products = {'ah':'lad', 'zs':'lzh',
     'pb':'pbd', 'ca':'lcu', 'ni':'lnd'}
    months={'jan':'f',
            'feb':'g',
            'mar':'h',
            'apr':'j',
            'may':'k',
            'jun':'m',
            'jul':'n',
            'aug':'q',
            'sep':'u',
            'oct':'v',
            'nov':'x',
            'dec':'z' }

    return products[product.lower()]+'o'+months[series[:3].lower()]+series[-1:]

def settleVolsProcess():    
    #pull vols from postgres
    vols = select_from('get_settlement_vols')

    #convert lme names
    vols['instrument'] = vols.apply(lambda row : lme_to_georgia(row['Product'], 
    row['Series']), axis = 1)
    
    #set instrument to index
    vols.set_index('instrument', inplace=True)

    #send to redis
    pick_vols = pickle.dumps(vols)
    conn.set('lme_vols',pick_vols )
    

settleVolsProcess()

