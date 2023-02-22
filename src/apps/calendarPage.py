from dash.dependencies import Input, Output, State
from dash import dcc, html
from dash import dcc
from dash import no_update
from datetime import datetime as dt
import dash_bootstrap_components as dbc
from dash import dash_table as dtable
import pandas as pd
import sqlalchemy

from parts import topMenu


import sqlalchemy
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import upestatic


engine = create_engine(
    "postgresql+psycopg://georgia:#dogs#dogs#dogs@georgia-db-test.postgres.database.azure.com/staticdata"
)


# products = session.query(upestatic.Product).all()
# products = [product.holidays for product in products]
# #print(products)
# # for holiday in products:
# #     for day in holiday:
# #         print(day.holiday_id, day.holiday_date)
#     #print(holiday.holiday_date)

#     #sqlalchemy.org.get(product)

# #for product in product:
# # for holiday in wheat.holidays:
# #     print(holiday.holiday_id, holiday.holiday_date, holiday.holiday_weight)

# holidays = session.query(upestatic.Holiday).all()

# session = Session()
# holidays = session.query(upestatic.Holiday).all()


# print(df)
# for holiday in holidays:
#     print(holiday.holiday_id, holiday.holiday_date, holiday.products)

# headers = [column["name"] for column in columns]

# print(holidays)
# for holiday in holidays:
#     print(holiday.holiday_id)

# def get_data():
#     session = Session()
#     holidays = session.query(upestatic.Holiday).all() #\
#     #.join(upestatic.Product, upe)
#     for holiday in holidays:
#         print(holiday.holiday_id, holiday.holiday_date, holiday.products)

# columns = session.execute(holidays).keys()
# columns = holidays.statement.columns.keys()
# print(columns)


# query = select(upestatic.Holiday).where(upestatic.Holiday.holiday_id == 1)
# data = session.execute(query).fetchall()
# # for i in data:
# #     print(i.holiday_id)
# # session.close()
# print(data)
# return data

# data = get_data()

# columns = [
#     {"name": "{}", "id": "{}"}.format(i.holiday_id, i.holiday_id) for i in holidays
# ]
# print(columns)

columns = [
    #{"name": "Holiday ID", "id": "holiday_id"},
    {"name": "Holiday Date", "id": "holiday_date"},
    {"name": "Holiday Weight", "id": "holiday_weight"},
]

table = dtable.DataTable(
    id="holidayTable",
    columns=columns,
    data=[{}],
    # fixed_rows=[{'headers': True, 'data': 0 }],
    style_data_conditional=[
        {
            "if": {"row_index": "odd"},
            "backgroundColor": "rgb(137, 186, 240)",
        }
    ],
)



loadProductsTemp = [{"label": "Milling Wheat", "value": "xext-ebm-eur"}]  # currently hardcoded, to be replaced with new onLoadProducts()
productDropdown = dcc.Dropdown(
    id="products", value="xext-ebm-eur", options=loadProductsTemp, clearable=False
)
productLabel = html.Label(
    ["Product:"], style={"font-weight": "bold", "text-align": "left"}
)
options = dbc.Col(html.Div(children=[productLabel, productDropdown]), width=4)



layout = html.Div(
    [
        topMenu("Calendar"),
        options,
        table
        #html.H1("This is a calendar page"),
    ]
)


def initialise_callbacks(app):
    @app.callback(Output("holidayTable", "data"), [Input("products", "value")])
    def update_trades(product):
        # start engine and load the data
        Session = sessionmaker(bind=engine)
        session = Session()

        product = (
            session.query(upestatic.Product)
            .where(upestatic.Product.symbol == product)
            .first()
        )

        df = pd.DataFrame(
            [
                {
                    #"holiday_id": holiday.holiday_id,
                    "holiday_date": holiday.holiday_date,
                    "holiday_weight": str(holiday.holiday_weight),
                }
                for holiday in product.holidays
            ]
        )
        df = df.sort_values(by=["holiday_date"])
        
        # figure out which button triggered the callback
        return df.to_dict("records")
