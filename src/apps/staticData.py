from datetime import datetime

import dash_bootstrap_components as dbc
import pandas as pd
from dash import dash_table as dtable
from dash import dcc, html
from dash.dependencies import Input, Output
from data_connections import shared_session
from dateutil.relativedelta import relativedelta
from parts import topMenu
from upedata import static_data as upe_static
from zoneinfo import ZoneInfo


def loadProducts():
    with shared_session() as session:
        products = session.query(upe_static.Product).all()
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
    [
        topMenu("Static Data"),
        html.Div(
            [dbc.Row(options, className="my-2"), dbc.Row(html.Div(id="sdTable"))],
            className="mx-3",
        ),
    ]
)


def initialise_callbacks(app):
    @app.callback(
        Output("sdTable", "children"),
        [Input("products", "value"), Input("productType", "value")],
    )
    def update_static_data(product, derivative_type):
        # start session and load the data
        if product and derivative_type:
            with shared_session() as session:
                product = (
                    session.query(upe_static.Product)
                    .where(upe_static.Product.symbol == product)
                    .first()
                )
                now_dt = datetime.now(ZoneInfo("UTC")) - relativedelta(days=1)
                if derivative_type == "future":
                    columns = [
                        {"name": "Symbol", "id": "symbol"},
                        {"name": "Display Name", "id": "display_name"},
                        {"name": "Expiry", "id": "expiry"},
                        {"name": "Multiplier", "id": "multiplier"},
                    ]
                    df = pd.DataFrame(
                        [
                            {
                                # "holiday_id": holiday.holiday_id,
                                "symbol": future.symbol.upper(),
                                "display_name": future.display_name,
                                "expiry": future.expiry,
                                "multiplier": future.multiplier,
                            }
                            for future in sorted(
                                product.futures, key=lambda future: future.expiry
                            )
                        ]
                    )
                    df = df[df["expiry"] > now_dt]

                elif derivative_type == "option":
                    columns = [
                        {"name": "Symbol", "id": "symbol"},
                        {"name": "Display Name", "id": "display_name"},
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
                                "display_name": option.display_name,
                                "underlying_future_symbol": option.underlying_future_symbol.upper(),
                                "multiplier": option.multiplier,
                                "time_type": str(option.time_type),
                                "vol_type": str(option.vol_type),
                                "expiry": option.expiry,
                                # "Underlying Expiry": option.underlying_future.expiry,#######
                                "strike_intervals": str(option.strike_intervals),
                            }
                            for option in sorted(
                                product.options, key=lambda option: option.expiry
                            )
                        ]
                    )
                    df = df[df["expiry"] > now_dt]

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
