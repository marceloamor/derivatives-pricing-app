import redis 
from data_connections import conn, call_function
from parts import loadStaticData, onLoadProductMonths
from parts import pullPortfolioGreeks, loadStaticData, pullPrompts, onLoadPortFolio
from datetime import date

portfolio = 'copper'
#pull prompt curve
rates = pullPrompts(portfolio)
positions = pullPortfolioGreeks()
positions['third_wed'] = positions['third_wed'].apply(lambda x: date.fromtimestamp(x/ 1e3).strftime('%Y%m%d'))
positions.set_index('third_wed', inplace=True)

rates.merge(positions[~positions['cop'].isin(['c', 'p']), "quanitity"],how='inner', left_index=True, right_index=True)

#remove underlying column
rates.drop(['underlying'], axis =1, inplace=True)

#add extra columns
#rates['total delta']= 

rates['forward_date'] = rates.index
rates = rates.round(2)
print(rates.head(2))
print(positions.head(3))

