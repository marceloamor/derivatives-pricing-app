from dash.dependencies import Input, Output
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash import dash_table as dtable
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from parts import topMenu
from data_connections import Session
import upestatic


columns = [
    # {"name": "Holiday ID", "id": "holiday_id"},
    {"name": "Holiday Date", "id": "holiday_date"},
    {"name": "Holiday Weight", "id": "holiday_weight"},
]

table = dtable.DataTable(
    id="holidayTable",
    columns=columns,
    data=[{}],
    style_data={"textAlign": "right"},
    style_header_conditional=[
        {
            "if": {"column_id": "holiday_weight"},
            "textAlign": "left",
        }
    ],
    style_data_conditional=[
        {"if": {"column_id": "holiday_weight"}, "textAlign": "left"}
    ],
)

def loadProducts():
    with Session() as session:
        products = session.query(upestatic.Product).all()
        return products


productList = [
    {"label": product.long_name.title(), "value": product.symbol}
    for product in loadProducts()
]

productDropdown = dcc.Dropdown(
    id="products", options=productList, value=productList[0]["value"], clearable=False
)
productLabel = html.Label(
    ["Product:"], style={"font-weight": "bold", "text-align": "left"}
)
options = dbc.Col(html.Div(children=[productLabel, productDropdown]), width=4)


layout = html.Div([topMenu("Calendar"), options, table])


def initialise_callbacks(app):
    @app.callback(Output("holidayTable", "data"), [Input("products", "value")])
    def update_trades(product):
        # start engine and load the data
        if product:
            with Session() as session:
                product = (
                    session.query(upestatic.Product)
                    .where(upestatic.Product.symbol == product)
                    .first()
                )

                df = pd.DataFrame(
                    [
                        {
                            # "holiday_id": holiday.holiday_id,
                            "holiday_date": holiday.holiday_date,
                            "holiday_weight": str(holiday.holiday_weight),
                        }
                        for holiday in product.holidays
                    ]
                )
                df = df.sort_values(by=["holiday_date"])

                return df.to_dict("records")
