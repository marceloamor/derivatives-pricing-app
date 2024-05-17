import dash_bootstrap_components as dbc
import pandas as pd
from dash import dash_table as dtable
from dash import dcc, html
from dash.dependencies import Input, Output
from data_connections import shared_session
from parts import topMenu
from upedata import static_data as upe_static

columns = [
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
    with shared_session() as session:
        products = session.query(upe_static.Product).all()
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
options = dbc.Col(
    html.Div(children=[productLabel, productDropdown]), width=4, className="my-2"
)


layout = html.Div([topMenu("Calendar"), html.Div([options, table], className="mx-3")])


def initialise_callbacks(app):
    @app.callback(Output("holidayTable", "data"), [Input("products", "value")])
    def update_trades(product):
        # start engine and load the data
        if product:
            with shared_session() as session:
                product = (
                    session.query(upe_static.Product)
                    .where(upe_static.Product.symbol == product)
                    .first()
                )

                df = pd.DataFrame(
                    [
                        {
                            "holiday_date": holiday.holiday_date,
                            "holiday_weight": str(holiday.holiday_weight),
                        }
                        for holiday in product.holidays
                    ]
                )
                df = df.sort_values(by=["holiday_date"])

                return df.to_dict("records")
