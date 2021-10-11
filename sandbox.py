from data_connections import conn
from parts import loadStaticData, onLoadProductMonths, loadStaticData
import pandas as pd
from datetime import date
import json, colorlover

def strikeRisk(portfolio, riskType, relAbs):
    #pull list of porducts from static data
    portfolioGreeks = conn.get('greekpositions')
    portfolioGreeks = json.loads(portfolioGreeks)
    portfolioGreeks = pd.DataFrame.from_dict(portfolioGreeks)

    print(portfolioGreeks.columns)

    products =  portfolioGreeks[portfolioGreeks.portfolio==portfolio]['underlying'].unique()

    #setup greeks and products bucket to collect data
    greeks = []
    dfData =[]

    if relAbs == 'strike':
         #for each product collect greek per strike
        for product in products:
               
                data = portfolioGreeks[portfolioGreeks.underlying==product]
                strikegreeks = []
                #go over strikes and uppack greeks 

                strikeRisk = {}
                for strike in data["strike"]: 
                    #pull product mult to convert greeks later
                    risk=data.loc[data.strike==strike][riskType].values[0]

                    strikegreeks.append(risk)
                    strikeRisk[strike]= risk
                greeks.append(strikegreeks)
                dfData.append(strikeRisk)
 
        df = pd.DataFrame(dfData, index=products)
        df.fillna(0, inplace=True)

        return df.round(3)

def discrete_background_color_bins(df, n_bins=4, columns='all'):
    bounds = [i * (1.0 / n_bins) for i in range(n_bins + 1)]

    if columns == 'all':
        if 'id' in df:
            df_numeric_columns = df.select_dtypes('number').drop(['id'], axis=1)
        else:
            df_numeric_columns = df.select_dtypes('number')
    else:
        df_numeric_columns = df[columns]

    df_max = df_numeric_columns.max().max()
    df_min = df_numeric_columns.min().min()

    styles = []

    #build ranges
    ranges = [
        (df_max  * i) 
        for i in bounds
    ]

    for i in range(1, len(ranges)):
        min_bound = ranges[i - 1]
        max_bound = ranges[i]
        half_bins = str(len(bounds))
        backgroundColor = colorlover.scales[half_bins]['seq']['Greens'][i - 1] 
        print(backgroundColor)
        color = 'black'
        for column in df_numeric_columns:
            styles.append({
                'if': {
                    'filter_query': (
                        '{{{column}}} >= {min_bound}' +
                        (' && {{{column}}} < {max_bound}' if (i < len(ranges) - 1) else '')
                    ).format(column=column, min_bound=min_bound, max_bound=max_bound),
                    'column_id': column
                },
                'backgroundColor': backgroundColor,
                'color': color
            })
    
    #build ranges
    ranges = [
        (df_min * i) 
        for i in bounds
    ]
    for i in range(1, len(ranges)):
        min_bound = ranges[i - 1]
        max_bound = ranges[i]
        half_bins = str(len(ranges))
        backgroundColor = colorlover.scales[half_bins]['seq']['Reds'][i - 1]
        color = 'black'
        for column in df_numeric_columns:
            styles.append({
                'if': {
                    'filter_query': (
                        '{{{column}}} <= {min_bound}' +
                        (' && {{{column}}} > {max_bound}' if (i < len(ranges) - 1) else '')
                    ).format(column=column, min_bound=min_bound, max_bound=max_bound),
                    'column_id': column
                },
                'backgroundColor': backgroundColor,
                'color': color
            })

    return styles


df = strikeRisk('copper', 'total_delta', 'strike')
styles = discrete_background_color_bins(df, n_bins=4, columns='all')
print(styles)