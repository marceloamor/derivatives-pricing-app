from dash.dependencies import Input, Output
from dash import dcc, html, no_update
import dash_bootstrap_components as dbc
from dash import dash_table as dtable
import dash_daq as daq
import pandas as pd
import sqlalchemy, os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from parts import topMenu
from data_connections import engine
import upestatic


def loadProducts():
    Session = sessionmaker(bind=engine)

    with Session() as session:
        products = session.query(upestatic.Product).all()
        return products


productList = [
    {"label": product.long_name.title(), "value": product.symbol}
    for product in loadProducts()
]


# dropdowns and labels
productDropdown = dcc.Dropdown(
    id="products", options=productList, clearable=False, value=productList[0]["value"]
)
productLabel = html.Label(
    ["Product:"], style={"font-weight": "bold", "text-align": "left"}
)

productTypeDropdown = dcc.Dropdown(
    id="productType",
    options=[
        {"label": "Future", "value": "future"},
        {"label": "Option", "value": "option"},
    ],
    clearable=False,
    value="option",
)
productTypeLabel = html.Label(
    ["Type:"], style={"font-weight": "bold", "text-align": "left"}
)


options = (
    dbc.Col(html.Div(children=[productLabel, productDropdown]), width=4),
    dbc.Col(html.Div(children=[productTypeLabel, productTypeDropdown]), width=4),
)

layout = html.Div(
    [topMenu("Static Data"), dbc.Row(options), dbc.Row(html.Div(id="sdTable"))]
)


def initialise_callbacks(app):
    @app.callback(
        Output("sdTable", "children"),
        [Input("products", "value"), Input("productType", "value")],
    )
    def update_static_data(product, type):
        # start engine and load the data
        Session = sessionmaker(bind=engine)
        with Session() as session:
            if product and type:
                product = (
                    session.query(upestatic.Product)
                    .where(upestatic.Product.symbol == product)
                    .first()
                )
                if type == "future":
                    columns = [
                        {"name": "Symbol", "id": "symbol"},
                        {"name": "Expiry", "id": "expiry"},
                        {"name": "Multiplier", "id": "multiplier"},
                    ]
                    df = pd.DataFrame(
                        [
                            {
                                # "holiday_id": holiday.holiday_id,
                                "symbol": future.symbol.upper(),
                                "expiry": future.expiry,
                                "multiplier": future.multiplier,
                            }
                            for future in product.futures
                        ]
                    )

                elif type == "option":
                    columns = [
                        {"name": "Symbol", "id": "symbol"},
                        {"name": "Underlying Symbol", "id": "underlying_future_symbol"},
                        {"name": "Multiplier", "id": "multiplier"},
                        {"name": "Time Type", "id": "time_type"},
                        {"name": "Vol Type", "id": "vol_type"},
                        {"name": "Expiry", "id": "expiry"},
                        # {"name": "Underlying Expiry", "id": "underlying_expiry"},
                        {"name": "Strike Intervals", "id": "strike_intervals"},
                    ]
                    df = pd.DataFrame(
                        [
                            {
                                "symbol": option.symbol.upper(),
                                "underlying_future_symbol": option.underlying_future_symbol.upper(),
                                "multiplier": option.multiplier,
                                "time_type": str(option.time_type),
                                "vol_type": str(option.vol_type),
                                "expiry": option.expiry,
                                # "Underlying Expiry": option.underlying_future.expiry,#######
                                "strike_intervals": str(option.strike_intervals),
                            }
                            for option in product.options
                        ]
                    )

                table = dtable.DataTable(
                    columns=columns,
                    data=df.to_dict("records"),
                    style_data_conditional=[
                        {
                            "if": {"row_index": "odd"},
                            "backgroundColor": "rgb(137, 186, 240)",
                        }
                    ],
                )
                return table
