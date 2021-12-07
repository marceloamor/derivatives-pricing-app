import redis, pickle, json 
from data_connections import conn, call_function, select_from, PostGresEngine
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

# data= conn.get('positions')
# data = pickle.loads(data)

# print(data)

def get_theo(instrument):
    product = instrument.split(' ')
    product =  product[0].lower()
    data = json.loads(conn.get(product))

    if data != None:
        #theo = data['strikes'][instrument[1]][instrument[2]]['theo']
        #data.set_index('instrument', inplace=True)
        
        theo = data[instrument.lower()]['calc_price']

        return float(theo)
    else: return 0

print(get_theo('LADOF2 2625 C'))    