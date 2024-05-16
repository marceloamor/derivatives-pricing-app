import datetime as dt

import dash_bootstrap_components as dbc
import display_names
import pandas as pd
import sqlalchemy
from dash import dash_table as dtable
from dash import dcc, html
from dash.dependencies import Input, Output
from data_connections import shared_engine
from pandas.tseries.offsets import BDay
from parts import loadPortfolios, loadProducts, ringTime, topMenu
from upedata import dynamic_data as upe_dynamic
from upedata import static_data as upe_static

interval = 1000 * 4


def LastBisDay():
    today = dt.datetime.today()
    date = today - BDay(1)
    return date


def shortName(product):
    if product == None:
        return "LCU"

    if product.lower() == "aluminium":
        return "LAD"
    elif product.lower() == "lead":
        return "PBD"
    elif product.lower() == "copper":
        return "LCU"
    elif product.lower() == "nickel":
        return "LND"
    elif product.lower() == "zinc":
        return "LZH"
    elif (
        product.lower() == "xext-ebm-eur"
    ):  # change this when more euronext products are added
        return "XEX"
    else:
        return "UNKNOWN"


posColumns = [
    # {"name": "Instrument", "id": "instrument_symbol"},
    {"name": "Display Name", "id": "instrument_display_name"},
    {"name": "Portfolio", "id": "display_name"},
    {"name": "Net Qty", "id": "net_quantity"},
    {"name": "Short Qty", "id": "short_quantity"},
    {"name": "Long Qty", "id": "long_quantity"},
]

position_table = dbc.Col(
    [
        dtable.DataTable(
            id="position",
            columns=posColumns,
            data=[{}],
            filter_action="native",
            sort_action="native",
            sort_mode="multi",
            # page_size=10,
            # fixed_rows=[{ 'headers': True, 'data': 0 }],
            style_table={"overflowY": "auto"},
            style_data_conditional=[
                {"if": {"row_index": "odd"}, "backgroundColor": "rgb(248, 248, 248)"},
            ],
        )
    ]
)


hidden = html.Div(
    [
        html.Div(id="hidden1-div", style={"display": "none"}),
        html.Div(id="hidden2-div", style={"display": "none"}),
        html.Div(id="hidden3-div", style={"display": "none"}),
        html.Div(id="hidden4-div", style={"display": "none"}),
        html.Div(id="hidden5-div", style={"display": "none"}),
    ],
    className="row",
)

# options = onLoadPortFolio()
# options.append({"label": "Milling Wheat", "value": "xext-ebm-eur"})


def pull_positions_new(product, portfolio):
    with shared_engine.connect() as session:
        if portfolio != "all":
            stmt = (
                sqlalchemy.select(
                    upe_dynamic.Position, upe_static.Portfolio.display_name
                )
                .join(
                    upe_static.Portfolio,
                    upe_dynamic.Position.portfolio_id
                    == upe_static.Portfolio.portfolio_id,
                )
                .where(
                    upe_dynamic.Position.instrument_symbol.startswith(product)
                    & (upe_dynamic.Position.net_quantity != 0)
                    & (upe_dynamic.Position.portfolio_id == portfolio)
                )
            )
        else:
            stmt = (
                sqlalchemy.select(
                    upe_dynamic.Position, upe_static.Portfolio.display_name
                )
                .join(
                    upe_static.Portfolio,
                    upe_dynamic.Position.portfolio_id
                    == upe_static.Portfolio.portfolio_id,
                )
                .where(
                    upe_dynamic.Position.instrument_symbol.startswith(product)
                    & (upe_dynamic.Position.net_quantity != 0)
                )
            )
        df = pd.read_sql(stmt, session)
    if len(df) > 0:
        df["instrument_display_name"] = display_names.map_symbols_to_display_names(
            df["instrument_symbol"].to_list()
        )
        df["instrument_display_name"] = df["instrument_display_name"].str.upper()
    return df


# dropdowns and labels
productDropdown = dcc.Dropdown(
    id="product", value="xlme-lcu-usd", options=loadProducts()
)
productLabel = html.Label(
    ["Product:"], style={"font-weight": "bold", "text-align": "left"}
)

# dropdown and label
portfolioDropdown = dcc.Dropdown(id="portfolio", value="all", options=loadPortfolios())
portfolioLabel = html.Label(
    ["Portfolio:"], style={"font-weight": "bold", "text-align": "left"}
)


selectors = dbc.Row(
    [
        dbc.Col(
            [productLabel, productDropdown],
            width=3,
        ),
        dbc.Col(
            [portfolioLabel, portfolioDropdown],
            width=3,
        ),
    ]
)


layout = html.Div(
    [
        topMenu("Positions"),
        dcc.Interval(id="live-update-portfolio", interval=interval),
        selectors,
        position_table,
    ]
)


def initialise_callbacks(app):
    # pulltrades
    @app.callback(
        Output("position", "data"),
        [
            Input("live-update-portfolio", "n_intervals"),
            Input("product", "value"),
            Input("portfolio", "value"),
        ],
    )
    def update_trades(interval, product, portfolio):
        # product = shortName(str(product))
        # dff = pullPosition(product, dt.datetime.today())

        dff = pull_positions_new(product, portfolio)

        return dff.to_dict("records")

    @app.callback(
        Output("ringPosition", "children"),
        [Input("live-update-portfolio", "n_intervals")],
    )
    def updareRing(interval):
        return ringTime()
