import bisect
import datetime as dt
import json
import os
import time
import traceback
from datetime import date, datetime
from datetime import time as dt_time
from io import BytesIO

import dash_bootstrap_components as dbc
import email_utils as email_utils
import numpy as np
import orjson
import pandas as pd
import sftp_utils as sftp_utils
import sql_utils
import sqlalchemy
from dash import dash_table as dtable
from dash import dcc, html, no_update
from dash.dependencies import ClientsideFunction, Input, Output, State
from data_connections import (
    PostGresEngine,
    conn,
    shared_engine,
    shared_session,
)
from dateutil.relativedelta import relativedelta
from flask import request
from parts import (
    buildTradesTableData,
    get_valid_counterpart_dropdown_options,
    loadProducts,
    loadRedisData,
    topMenu,
)
from scipy import interpolate
from upedata import dynamic_data as upe_dynamic
from upedata import static_data as upe_static
from zoneinfo import ZoneInfo
from icecream import ic
import hashlib

USE_DEV_KEYS = os.getenv("USE_DEV_KEYS", "false").lower() in [
    "true",
    "t",
    "1",
    "y",
    "yes",
]
if USE_DEV_KEYS:
    pass
dev_key_redis_append = "" if not USE_DEV_KEYS else ":dev"

legacyEngine = PostGresEngine()

months = {
    "01": "f",
    "02": "g",
    "03": "h",
    "04": "j",
    "05": "k",
    "06": "m",
    "07": "n",
    "08": "q",
    "09": "u",
    "10": "v",
    "11": "x",
    "12": "z",
}

frontend_colours = [
    "#49E704",  # light green
    "#E70404",  # red
    "#0420E7",  # blue
    "#0ad6f2",  # cyan
    "#008000",  # dark green
    "#dc1198",  # pink
    "#800080",  # purple
    "#FFA500",  # orange
    "#11dca8",  # turquoise
    "#2488bd",
    "#000000",
    "#FF5733",
    "#A52A2A",
    "#FFD700",
    "#00FFFF",
]


def generate_colour(identifier: str, colors: list[str]) -> str:
    # generate hash on product name and symbol and convert to colour in list
    hashed = hashlib.md5(identifier.encode()).hexdigest()
    hash_subset = hashed[1:14]  # 1:14 was working well
    color_index = int(hash_subset, 16)
    color_index %= len(colors)
    return colors[color_index]


# load products w user entitlements and hashed frontend colours
def loadProducts_with_entitlement(user_id: str) -> list[dict[str, str]]:
    with shared_engine.connect() as cnxn:
        try:
            stmt = sqlalchemy.text(
                "SELECT * FROM products WHERE exchange_symbol IN "
                "(SELECT exchange_symbol FROM trader_exchange_entitlements WHERE "
                "trader_id = (SELECT trader_id FROM traders WHERE email = :user))"
            ).bindparams(user=user_id)
            result = cnxn.execute(stmt).fetchall()
            if not result:
                raise ValueError("No products found")
        except Exception as e:
            # print(f"Error loading products for user {user_id}.", e)
            stmt = sqlalchemy.text("SELECT * FROM products")
            result = cnxn.execute(stmt).fetchall()
        productList = []

        for product in result:
            colour = generate_colour(
                product.long_name + product.symbol, frontend_colours
            )
            label_span = html.Span(
                [product.long_name.upper()],
                style={
                    "color": colour,
                    "fontWeight": "bold",
                },
            )
            product_dict = {"label": label_span, "value": product.symbol}
            productList.append(product_dict)
    return productList


def loadOptions(prod_symbol):
    with shared_session() as session:
        product_options = (
            session.execute(
                sqlalchemy.select(upe_static.Option)
                .where(upe_static.Option.product_symbol == prod_symbol.lower())
                .order_by(upe_static.Option.expiry.asc())
            )
            .scalars()
            .all()
        )
        return product_options


def getOptionInfo(optionSymbol):
    with shared_session() as session:
        option = (
            session.query(upe_static.Option)
            .where(upe_static.Option.symbol == optionSymbol)
            .first()
        )
        expiry = option.expiry
        expiry = expiry.timestamp()
        expiry = date.fromtimestamp(expiry)
        und_name = option.underlying_future_symbol
        currency_iso_symbol = option.product.currency.iso_symbol
        # this line will only work for the next 76 years
        und_expiry = "20" + und_name.split(" ")[-1]
        mult = int(option.multiplier)
        return (expiry, und_name, und_expiry, mult, currency_iso_symbol)


def pullSettleVolsEU(optionSymbol):
    with shared_session() as session:
        try:
            most_recent_date = (
                session.query(upe_dynamic.SettlementVol)
                .where(upe_dynamic.SettlementVol.option_symbol == optionSymbol)
                .order_by(upe_dynamic.SettlementVol.settlement_date.desc())
                .first()
                .settlement_date
            )
            settle_vols = (
                session.query(upe_dynamic.SettlementVol)
                .where(upe_dynamic.SettlementVol.option_symbol == optionSymbol)
                .where(upe_dynamic.SettlementVol.settlement_date == most_recent_date)
                .all()
            )
            data = [
                {"strike": int(vol.strike), "vol": vol.volatility}
                for vol in settle_vols
            ]
        except:
            data = []

        return data


clearing_email = os.getenv(
    "CLEARING_EMAIL", default="frederick.fillingham@upetrading.com"
)
clearing_cc_email = os.getenv("CLEARING_CC_EMAIL", default="lmeclearing@upetrading.com")

stratColColor = "#9CABAA"


def fetechstrikes(product):
    if product[-2:] == "3M":
        return {"label": 0, "value": 0}

    if product != None:
        strikes = []
        data = loadRedisData(product.lower())
        data = json.loads(data)
        for strike in data["strikes"]:
            strikes.append({"label": strike, "value": strike})
        return strikes
    else:
        return {"label": 0, "value": 0}


def timeStamp():
    now = dt.datetime.now()
    now.strftime("%Y-%m-%d %H:%M:%S")
    return now


stratOptions = [
    {"label": "Outright", "value": "outright"},
    {"label": "Spread", "value": "spread"},
    {"label": "Straddle/Strangle", "value": "straddle"},
    {"label": "Fly", "value": "fly"},
    {"label": "Condor", "value": "condor"},
    {"label": "Ladder", "value": "ladder"},
    {"label": "1*2", "value": "ratio"},
    {"label": "PSvC/CSvP", "value": "spreadvs"},
]

stratConverstion = {
    "outright": [1, 0, 0, 0],
    "spread": [1, -1, 0, 0],
    "straddle": [1, 1, 0, 0],
    "fly": [1, -2, 1, 0],
    "condor": [1, -1, -1, 1],
    "ladder": [1, -1, -1, 0],
    "ratio": [1, -2, 0, 0],
    "spreadvs": [1, -1, -1, 0],
}

# trades table layout
calculator = dbc.Col(
    [
        # top row lables
        dbc.Row(
            [
                dbc.Col(["Basis"], width=4),
                dbc.Col(["Forward"], width=4),
                dbc.Col(["Interest"], width=4),
            ]
        ),
        # top row values
        dbc.Row(
            [
                dbc.Col(
                    [dcc.Input(id="calculatorBasis-c2", type="text", debounce=True)],
                    width=4,
                ),
                dbc.Col([dcc.Input(id="calculatorForward-c2", type="text")], width=4),
                dbc.Col([dcc.Input(id="interestRate-c2", type="text")], width=4),
            ]
        ),
        # second row labels
        dbc.Row(
            [
                dbc.Col([html.Div("Spread")], width=4),
                dbc.Col([html.Div("Strategy")], width=4),
                # dbc.Col([html.Div("Days Convention")], width=4),
            ]
        ),
        # second row values
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.Div(
                            [
                                dcc.Input(
                                    type="text", id="calculatorSpread-c2", debounce=True
                                )
                            ]
                        )
                    ],
                    width=4,
                ),
                dbc.Col(
                    [
                        html.Div(
                            [
                                dcc.Dropdown(
                                    id="strategy-c2",
                                    value="outright",
                                    options=stratOptions,
                                )
                            ]
                        )
                    ],
                    width=4,
                ),
                dbc.Col(
                    [html.Br()],
                    width=4,
                ),
            ]
        ),
        # model settings
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.Div(
                            [
                                dcc.RadioItems(
                                    id="calculatorVol_price-c2",
                                    options=[
                                        {"label": "Vol", "value": "vol"},
                                        {"label": "Price", "value": "price"},
                                    ],
                                    value="vol",
                                )
                            ]
                        )
                    ],
                    width=2,
                ),
                dbc.Col(
                    [
                        html.Div(
                            [
                                dcc.RadioItems(
                                    id="calc-settle-internal-c2",
                                    options=[
                                        {"label": "Internal", "value": "internal"},
                                        {"label": "Settlement", "value": "settlement"},
                                    ],
                                    value="internal",
                                )
                            ]
                        )
                    ],
                    width=2,
                ),
                dbc.Col(
                    [
                        html.Div(
                            [
                                dcc.RadioItems(
                                    id="nowOpen-c2",
                                    options=[
                                        {"label": "Now", "value": "now"},
                                        {"label": "Open", "value": "open"},
                                    ],
                                    value="now",
                                )
                            ]
                        )
                    ],
                    width=2,
                ),
                dbc.Col([html.Div("Counterparty:")], width=2),
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="counterparty-c2",
                            value="",
                            options=[],
                        )
                    ],
                    width=2,
                ),
            ]
        ),
        # leg inputs and outputs
        # leg inputs
        # labels
        dbc.Row(
            [
                dbc.Col(["Strike: "], width=2),
                dbc.Col([dcc.Input(id="oneStrike-c2")], width=2),
                dbc.Col([dcc.Input(id="twoStrike-c2")], width=2),
                dbc.Col([dcc.Input(id="threeStrike-c2")], width=2),
                dbc.Col([dcc.Input(id="fourStrike-c2")], width=2),
                dbc.Col(
                    [dcc.Input(id="qty-c2", type="number", value=10, min=0)], width=2
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Price/Vol: "], width=2),
                dbc.Col([dcc.Input(id="oneVol_price-c2")], width=2),
                dbc.Col([dcc.Input(id="twoVol_price-c2")], width=2),
                dbc.Col([dcc.Input(id="threeVol_price-c2")], width=2),
                dbc.Col([dcc.Input(id="fourVol_price-c2")], width=2),
                dbc.Col(
                    [
                        dbc.Button(
                            "Buy", id="buy-c2", n_clicks_timestamp="0", active=True
                        )
                    ],
                    width=1,
                ),
                dbc.Col(
                    [
                        dbc.Button(
                            "Sell", id="sell-c2", n_clicks_timestamp="0", active=True
                        )
                    ],
                    width=1,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["C/P: "], width=2),
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="oneCoP-c2",
                            value="c",
                            options=[
                                {"label": "C", "value": "c"},
                                {"label": "P", "value": "p"},
                            ],
                            style={"height": "50%", "verticalAlign": "middle"},
                        )
                    ],
                    width=2,
                ),
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="twoCoP-c2",
                            value="c",
                            options=[
                                {"label": "C", "value": "c"},
                                {"label": "P", "value": "p"},
                            ],
                            style={"height": "50%", "verticalAlign": "middle"},
                        )
                    ],
                    width=2,
                ),
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="threeCoP-c2",
                            value="c",
                            options=[
                                {"label": "C", "value": "c"},
                                {"label": "P", "value": "p"},
                            ],
                            style={"height": "50%", "verticalAlign": "middle"},
                        )
                    ],
                    width=2,
                ),
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="fourCoP-c2",
                            value="c",
                            options=[
                                {"label": "C", "value": "c"},
                                {"label": "P", "value": "p"},
                            ],
                            style={"height": "50%", "verticalAlign": "middle"},
                        )
                    ],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Theo: "], width=2),
                dbc.Col([html.Div(id="oneTheo-c2")], width=2),
                dbc.Col([html.Div(id="twoTheo-c2")], width=2),
                dbc.Col([html.Div(id="threeTheo-c2")], width=2),
                dbc.Col([html.Div(id="fourTheo-c2")], width=2),
                dbc.Col(
                    [html.Div(id="stratTheo-c2", style={"background": stratColColor})],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["IV: "], width=2),
                dbc.Col([html.Div(id="oneIV-c2")], width=2),
                dbc.Col([html.Div(id="twoIV-c2")], width=2),
                dbc.Col([html.Div(id="threeIV-c2")], width=2),
                dbc.Col([html.Div(id="fourIV-c2")], width=2),
                dbc.Col(
                    [html.Div(id="stratIV-c2", style={"background": stratColColor})],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Settle IV:"], width=2),
                dbc.Col([html.Div(id="oneSettleVol-c2")], width=2),
                dbc.Col([html.Div(id="twoSettleVol-c2")], width=2),
                dbc.Col([html.Div(id="threeSettleVol-c2")], width=2),
                dbc.Col([html.Div(id="fourSettleVol-c2")], width=2),
                dbc.Col(
                    [
                        html.Div(
                            id="stratSettleVol-c2", style={"background": stratColColor}
                        )
                    ],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Delta: "], width=2),
                dbc.Col([html.Div(id="oneDelta-c2")], width=2),
                dbc.Col([html.Div(id="twoDelta-c2")], width=2),
                dbc.Col([html.Div(id="threeDelta-c2")], width=2),
                dbc.Col([html.Div(id="fourDelta-c2")], width=2),
                dbc.Col(
                    [html.Div(id="stratDelta-c2", style={"background": stratColColor})],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Gamma: "], width=2),
                dbc.Col([html.Div(id="oneGamma-c2")], width=2),
                dbc.Col([html.Div(id="twoGamma-c2")], width=2),
                dbc.Col([html.Div(id="threeGamma-c2")], width=2),
                dbc.Col([html.Div(id="fourGamma-c2")], width=2),
                dbc.Col(
                    [html.Div(id="stratGamma-c2", style={"background": stratColColor})],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Vega: "], width=2),
                dbc.Col([html.Div(id="oneVega-c2")], width=2),
                dbc.Col([html.Div(id="twoVega-c2")], width=2),
                dbc.Col([html.Div(id="threeVega-c2")], width=2),
                dbc.Col([html.Div(id="fourVega-c2")], width=2),
                dbc.Col(
                    [html.Div(id="stratVega-c2", style={"background": stratColColor})],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Theta: "], width=2),
                dbc.Col([html.Div(id="oneTheta-c2")], width=2),
                dbc.Col([html.Div(id="twoTheta-c2")], width=2),
                dbc.Col([html.Div(id="threeTheta-c2")], width=2),
                dbc.Col([html.Div(id="fourTheta-c2")], width=2),
                dbc.Col(
                    [html.Div(id="stratTheta-c2", style={"background": stratColColor})],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(html.Div("Vol Theta: ", id="volThetaLabel"), width=2),
                dbc.Col([html.Div(id="onevolTheta-c2")], width=2),
                dbc.Col([html.Div(id="twovolTheta-c2")], width=2),
                dbc.Col([html.Div(id="threevolTheta-c2")], width=2),
                dbc.Col([html.Div(id="fourvolTheta-c2")], width=2),
                dbc.Col(
                    [
                        html.Div(
                            id="stratvolTheta-c2", style={"background": stratColColor}
                        )
                    ],
                    width=2,
                ),
            ]
        ),
    ],
    width=9,
)


hidden = (
    dcc.Store(id="tradesStore-c2"),
    dcc.Store(id="paramsStore-c2"),
    dcc.Store(id="productInfo-c2"),
    dcc.Store(id="productHelperInfo-c2"),
    dcc.Store(id="strike-settlement-vols-c2"),
    dcc.Store(id="strike-settlement-vols-shifted-c2"),
    dcc.Store(id="underlying-closing-price-c2"),
    dcc.Interval(id="productDataRefreshInterval-c2", interval=600 * 1000),
    html.Div(id="trades_div-c2", style={"display": "none"}),
    html.Div(id="trade_div-c2", style={"display": "none"}),
    html.Div(id="trade_div2-c2", style={"display": "none"}),
    html.Div(id="productData-c2", style={"display": "none"}),
    html.Div(id="holsToExpiry-c2", style={"display": "none"}),
    html.Div(id="und_name-c2", style={"display": "none"}),
    html.Div(id="open-live-time-correction-c2", style={"display": "none"}),
    html.Button("Start", id="calc-hidden-start-button", style={"display": "none"}),
)

actions = dbc.Row(
    [
        dbc.Col([html.Button("Delete", id="delete-c2", n_clicks_timestamp=0)], width=3),
        dbc.Col([html.Button("Trade", id="trade-c2", n_clicks_timestamp=0)], width=3),
        dbc.Col(
            [html.Button("Client Recap", id="clientRecap-c2", n_clicks_timestamp=0)],
            width=3,
        ),
        dbc.Col(
            [
                dcc.ConfirmDialogProvider(
                    html.Button("Report", id="report-c2", n_clicks_timestamp=0),
                    id="report-confirm-c2",
                    submit_n_clicks_timestamp=0,
                    message="Are you sure you wish to report this trade? This cannot be undone.",
                )
            ],
            width=3,
        ),
    ]
)

columns = [
    {"id": "Instrument", "name": "Instrument", "editable": False},
    {"id": "Qty", "name": "Qty", "editable": True},
    {
        "id": "Theo",
        "name": "Theo",
        "editable": True,
    },
    {"id": "Prompt", "name": "Prompt", "editable": False},
    {"id": "Forward", "name": "Forward", "editable": False},
    {"id": "IV", "name": "IV", "editable": False},
    {"id": "Delta", "name": "Delta", "editable": False},
    {"id": "Gamma", "name": "Gamma", "editable": False},
    {"id": "Vega", "name": "Vega", "editable": False},
    {"id": "Theta", "name": "Theta", "editable": False},
    {"id": "Account", "name": "Account", "presentation": "dropdown"},
    {"id": "Counterparty", "name": "Counterparty", "presentation": "dropdown"},
]

trade_table_account_options = []
with shared_engine.connect() as db_conn:
    stmt = sqlalchemy.text(
        "SELECT portfolio_id, display_name FROM portfolios "
        "WHERE display_name != 'Error'"
    )
    result = db_conn.execute(stmt)
    for portfolio_id, display_name in result.fetchall():
        trade_table_account_options.append(
            {"label": display_name, "value": portfolio_id}
        )

tables = dbc.Col(
    dtable.DataTable(
        id="tradesTable-c2",
        data=[{}],
        columns=columns,
        row_selectable="multi",
        editable=True,
        dropdown={
            "Counterparty": {
                "clearable": False,
                "options": get_valid_counterpart_dropdown_options("all"),
            },
            "Account": {"clearable": False, "options": trade_table_account_options},
        },
        style_data_conditional=[
            {"if": {"column_id": "Instrument"}, "backgroundColor": "#f1f1f1"},
            {"if": {"column_id": "Prompt"}, "backgroundColor": "#f1f1f1"},
            {"if": {"column_id": "Forward"}, "backgroundColor": "#f1f1f1"},
            {"if": {"column_id": "IV"}, "backgroundColor": "#f1f1f1"},
            {"if": {"column_id": "Delta"}, "backgroundColor": "#f1f1f1"},
            {"if": {"column_id": "Gamma"}, "backgroundColor": "#f1f1f1"},
            {"if": {"column_id": "Vega"}, "backgroundColor": "#f1f1f1"},
            {"if": {"column_id": "Theta"}, "backgroundColor": "#f1f1f1"},
        ],
    )
)

toolTips = html.Div(
    [
        dbc.Tooltip("Theta in terms of vol change equivalent", target="volThetaLabel"),
        dbc.Tooltip(
            "Full price change of the option with underlying"
            "including both BS dleta and the option moving on the Vol surface",
            target="fullDeltaLabel",
        ),
    ]
)

sideMenu = dbc.Col(
    [
        dbc.Row(
            dbc.Col(
                [
                    dcc.Dropdown(
                        id="productCalc-selector-c2",
                        # options=productList,
                        # value=productList[0]["value"],
                    )
                ],
                width=12,
            )
        ),
        dbc.Row(dbc.Col([dcc.Dropdown(id="monthCalc-selector-c2")], width=12)),
        dbc.Row(dbc.Col(["Product:"], width=12)),
        dbc.Row(dbc.Col(["Option Expiry:"], width=12)),
        dbc.Row(dbc.Col([html.Div("expiry", id="calculatorExpiry-c2")], width=12)),
        dbc.Row(dbc.Col(["Underlying Expiry:"], width=12)),
        dbc.Row(dbc.Col([html.Div("und_expiry", id="3wed-c2")])),
        dbc.Row(dbc.Col(["Multiplier:"], width=12)),
        dbc.Row(dbc.Col([html.Div("mult", id="multiplier-c2")])),
        dbc.Row(dbc.Col(["Days per year:"], width=12)),
        dbc.Row(dbc.Col([html.Div("days_per_year", id="days-per-year-c2")])),
        dbc.Row(dbc.Col(["Years to expiry (6 d.p.):"], width=12)),
        dbc.Row(dbc.Col([html.Div("t_to_exp", id="t-to-expiry-c2")])),
    ],
    width=3,
)

output = dcc.Markdown(id="reponseOutput-c2")

alert = html.Div(
    [
        dbc.Alert(
            "Trade Saved",
            id="tradeSent-c2",
            dismissable=True,
            is_open=False,
            duration=5000,
            color="success",
        ),
        dbc.Alert(
            "Trade failed to save",
            id="tradeSentFail-c2",
            dismissable=True,
            is_open=False,
            duration=5000,
            color="danger",
        ),
        dbc.Alert(
            "Trade Routed",
            id="tradeRouted-c2",
            dismissable=True,
            is_open=False,
            duration=5000,
            color="success",
        ),
        dbc.Alert(
            "Trade Routing Failed",
            id="tradeRouteFail-c2",
            dismissable=True,
            is_open=False,
            duration=5000,
            color="danger",
        ),
        dbc.Alert(
            "Trade Routing Partially Failed",
            id="tradeRoutePartialFail-c2",
            dismissable=True,
            is_open=False,
            duration=5000,
            color="danger",
        ),
    ]
)

layout = html.Div(
    [
        topMenu("Calculator"),
        dbc.Row(alert),
        dbc.Row(hidden),
        dbc.Row([sideMenu, calculator]),
        dbc.Row(tables),
        actions,
        dbc.Row(output),
        toolTips,
    ]
)


def initialise_callbacks(app):
    @app.callback(
        Output("productCalc-selector-c2", "options"),
        Output("productCalc-selector-c2", "value"),
        [Input("calc-hidden-start-button", "n_clicks")],
    )
    def updateOptions(product):
        # invisible button to trigger the callback necessary for header request
        user_id = request.headers.get("X-MS-CLIENT-PRINCIPAL-NAME")
        if not user_id:
            user_id = "TEST"

        productList = loadProducts_with_entitlement(user_id)

        return productList, productList[0]["value"]

    # update months options on product change
    @app.callback(
        Output("monthCalc-selector-c2", "options"),
        [Input("productCalc-selector-c2", "value")],
    )
    def updateOptions(product):  # DONE!
        if product:
            optionsList = []
            for option in loadOptions(product):
                if (
                    conn.exists(
                        option.symbol + dev_key_redis_append,
                        option.symbol + ":frontend_helper_data" + dev_key_redis_append,
                    )
                    < 2
                ):
                    continue
                expiry = option.expiry
                # only show non-expired options +1 day
                if expiry >= datetime.now(tz=ZoneInfo("UTC")) - relativedelta(days=1):
                    # option is named after the expiry of the underlying
                    if product.startswith("xlme"):
                        date = option.underlying_future_symbol.split(" ")[2]
                        label = months[date[3:5]].upper() + date[1]
                    else:
                        option_date = option.expiry + relativedelta(months=1)
                        label = (
                            months[option_date.strftime("%m")].upper()
                            + option_date.strftime("%y")[-1]
                        )
                    optionsList.append({"label": label, "value": option.symbol})
            return optionsList

    # update months value on product change - DONE
    @app.callback(
        Output("monthCalc-selector-c2", "value"),
        [Input("monthCalc-selector-c2", "options")],
    )
    def updatevalue(options):
        if options:
            return options[0]["value"]

    # update static data on product/month change   DONE!
    @app.callback(
        Output("multiplier-c2", "children"),
        Output("und_name-c2", "children"),
        Output("3wed-c2", "children"),
        Output("calculatorExpiry-c2", "children"),
        Output("interestRate-c2", "placeholder"),
        Output("counterparty-c2", "options"),
        Output("tradesTable-c2", "dropdown"),
        [Input("monthCalc-selector-c2", "value"), State("tradesTable-c2", "dropdown")],
    )
    def updateOptionInfo(optionSymbol, trades_table_dropdown_state):
        if optionSymbol:
            (expiry, und_name, und_expiry, mult, currency_iso_symbol) = getOptionInfo(
                optionSymbol
            )

            # inr
            # new inr standard - xext to use option expiry date
            counterparty_dropdown_options = get_valid_counterpart_dropdown_options(
                optionSymbol.split(" ")[0].split("-")[0].lower()
            )
            inr_curve = orjson.loads(
                conn.get(
                    f"prep:cont_interest_rate:{currency_iso_symbol.lower()}"
                    + dev_key_redis_append
                )
            )
            inr = inr_curve.get(expiry.strftime("%Y%m%d")) * 100
            trades_table_dropdown_state["Counterparty"][
                "options"
            ] = counterparty_dropdown_options

            return (
                mult,
                und_name,
                und_expiry,
                expiry,
                round(inr, 3),
                counterparty_dropdown_options,
                trades_table_dropdown_state,
            )

    # update settlement vols store on product change - DONE!
    # @app.callback(
    #     Output("settleVolsStore-c2", "data"),
    #     [Input("monthCalc-selector-c2", "value")],
    # )
    # def updateOptionInfo(optionSymbol):
    #     if optionSymbol:
    #         settle_vols = pullSettleVolsEU(optionSymbol)
    #         if settle_vols:
    #             return settle_vols
    #         else:
    #             return None

    @app.callback(
        Output("strike-settlement-vols-shifted-c2", "data"),
        [
            Input("underlying-closing-price-c2", "data"),
            Input("calculatorForward-c2", "value"),
            Input("calculatorForward-c2", "placeholder"),
            Input("strike-settlement-vols-c2", "data"),
            State("productInfo-c2", "data"),
        ],
    )
    def update_sliding_settlements(
        und_close_price,
        calc_forward_val,
        calc_forward_val_placeholder,
        base_settlement_data,
        product_data,
    ):
        if calc_forward_val == "":
            calc_forward_val = calc_forward_val_placeholder

        calc_forward_val = float(calc_forward_val)
        if None in (
            und_close_price,
            calc_forward_val,
            base_settlement_data,
        ):
            return np.zeros_like(product_data["strikes"])
        intraday_move = calc_forward_val - und_close_price
        settlement_vols = interpolate.UnivariateSpline(
            np.array(base_settlement_data["strike"]) + intraday_move,
            base_settlement_data["volatility"],
            k=2,
            ext=3,
            s=0,
        )(product_data["strikes"])
        return settlement_vols

    # update business days to expiry (used for daysConvention) - DONE!
    @app.callback(
        Output("holsToExpiry-c2", "children"),
        [Input("calculatorExpiry-c2", "children")],
        [State("monthCalc-selector-c2", "value")],
        [State("productCalc-selector-c2", "value")],
    )
    def updateBis(expiry, month, product):
        if month and product:
            with shared_session() as session:
                product = (
                    session.query(upe_static.Product)
                    .where(upe_static.Product.symbol == product)
                    .first()
                )
                expiry = datetime.strptime(expiry, "%Y-%m-%d").date()
                today = datetime.now().date()
                holidaysToDiscount = []

                for holiday in product.holidays:
                    if holiday.holiday_date >= today and holiday.holiday_date < expiry:
                        # holidaysToDiscount += holiday.holiday_weight
                        holidaysToDiscount.append(str(holiday.holiday_date))
            return holidaysToDiscount

    # change the CoP dropdown options depning on if £m or not - DONE!
    @app.callback(
        [
            Output("oneCoP-c2", "options"),
            Output("twoCoP-c2", "options"),
            Output("threeCoP-c2", "options"),
            Output("fourCoP-c2", "options"),
            Output("oneCoP-c2", "value"),
            Output("twoCoP-c2", "value"),
            Output("threeCoP-c2", "value"),
            Output("fourCoP-c2", "value"),
        ],
        [Input("monthCalc-selector-c2", "value")],
    )
    def sendCopOptions(month):
        if month:
            options = [
                {"label": "C", "value": "c"},
                {"label": "P", "value": "p"},
                {"label": "F", "value": "f"},
            ]
            return options, options, options, options, "c", "c", "c", "c"

    # populate table on trade deltas change - DONE!
    @app.callback(Output("tradesTable-c2", "data"), [Input("tradesStore-c2", "data")])
    def loadTradeTable(data):
        if data != None:
            trades = buildTradesTableData(data)
            return trades.to_dict("records")

        else:
            return [{}]

    # change talbe data on buy/sell delete - SHOULD BE DONE!
    @app.callback(
        [Output("tradesStore-c2", "data"), Output("tradesTable-c2", "selected_rows")],
        [
            Input("buy-c2", "n_clicks_timestamp"),
            Input("sell-c2", "n_clicks_timestamp"),
            Input("delete-c2", "n_clicks_timestamp"),
        ],
        # standard trade inputs
        [
            State("tradesTable-c2", "selected_rows"),
            State("tradesTable-c2", "data"),
            State("calculatorVol_price-c2", "value"),
            State("tradesStore-c2", "data"),
            State("counterparty-c2", "value"),  # NOT USED FOR NOW
            State("und_name-c2", "children"),
            State("3wed-c2", "children"),
            # State('trades_div' , 'children'),
            State("productCalc-selector-c2", "value"),
            State("monthCalc-selector-c2", "value"),
            State("qty-c2", "value"),
            State("strategy-c2", "value"),
            # trade value inputs
            # one vlaues
            State("oneStrike-c2", "value"),
            State("oneStrike-c2", "placeholder"),
            State("oneCoP-c2", "value"),
            State("oneTheo-c2", "children"),
            State("oneIV-c2", "children"),
            State("oneDelta-c2", "children"),
            State("oneGamma-c2", "children"),
            State("oneVega-c2", "children"),
            State("oneTheta-c2", "children"),
            # two values
            State("twoStrike-c2", "value"),
            State("twoStrike-c2", "placeholder"),
            State("twoCoP-c2", "value"),
            State("twoTheo-c2", "children"),
            State("twoIV-c2", "children"),
            State("twoDelta-c2", "children"),
            State("twoGamma-c2", "children"),
            State("twoVega-c2", "children"),
            State("twoTheta-c2", "children"),
            # three values
            State("threeStrike-c2", "value"),
            State("threeStrike-c2", "placeholder"),
            State("threeCoP-c2", "value"),
            State("threeTheo-c2", "children"),
            State("threeIV-c2", "children"),
            State("threeDelta-c2", "children"),
            State("threeGamma-c2", "children"),
            State("threeVega-c2", "children"),
            State("threeTheta-c2", "children"),
            # four values
            State("fourStrike-c2", "value"),
            State("fourStrike-c2", "placeholder"),
            State("fourCoP-c2", "value"),
            State("fourTheo-c2", "children"),
            State("fourIV-c2", "children"),
            State("fourDelta-c2", "children"),
            State("fourGamma-c2", "children"),
            State("fourVega-c2", "children"),
            State("fourTheta-c2", "children"),
            State("calculatorForward-c2", "value"),
            State("calculatorForward-c2", "placeholder"),
        ],
    )
    def stratTrade(
        buy,
        sell,
        delete,
        clickdata,
        rows,
        pricevola,
        data,
        counterparty,
        und_name,
        tm,
        product,
        month,
        qty,
        strat,
        onestrike,
        ponestrike,
        onecop,
        onetheo,
        oneiv,
        onedelta,
        onegamma,
        onevega,
        onetheta,
        twostrike,
        ptwostrike,
        twocop,
        twotheo,
        twoiv,
        twodelta,
        twogamma,
        twovega,
        twotheta,
        threestrike,
        pthreestrike,
        threecop,
        threetheo,
        threeiv,
        threedelta,
        threegamma,
        threevega,
        threetheta,
        fourstrike,
        pfourstrike,
        fourcop,
        fourtheo,
        fouriv,
        fourdelta,
        fourgamma,
        fourvega,
        fourtheta,
        forward,
        pforward,
    ):
        if (int(buy) + int(sell) + int(delete)) == 0:
            return [], []

        # replace rows if nonetype
        if not clickdata:
            clickdata = []

        # reset buy sell signal
        bs = 0
        # convert qty to float to save multiple tims later
        qty = float(qty)

        # set counterparty to none for now
        # counterparty = "none"
        carry_link = None
        # build product from month and product dropdown
        if product and month:
            # product = product + "O" + month
            if data:
                trades = data
            else:
                trades = {}

            # on delete work over indices and delete rows then update trades dict
            if int(delete) > int(buy) and int(delete) > int(sell):
                if clickdata:
                    for i in clickdata:
                        instrument = rows[i]["Instrument"]
                        trades.pop(instrument, None)
                        clickdata = []
            else:
                # not delete so see ifs its a buy/sell button click
                # create name then go over buy/sell and action

                # find the stat mults
                statWeights = stratConverstion[strat]
                # clac forward and prompt
                if forward:
                    Bforward = forward
                else:
                    Bforward = pforward
                prompt = dt.datetime.strptime(tm, "%Y-%m-%d").strftime("%Y-%m-%d")
                futureName = und_name.upper()  # str(product)[:3] + " " + str(prompt)

                # calc strat for buy
                if int(buy) > int(sell) and int(buy) > int(delete):
                    bs = 1

                elif int(buy) < int(sell) and int(sell) > int(delete):
                    bs = -1

                if bs != 0:
                    deltaBucket = 0
                    # calc one leg weight
                    if statWeights[0] != 0:
                        weight = statWeights[0] * bs
                        # get strike from value and placeholder
                        if onecop == "f":
                            hedge = {
                                "qty": float(onedelta) * weight * qty,
                                "theo": Bforward,
                                "prompt": prompt,
                                "forward": Bforward,
                                "iv": 0,
                                "delta": float(onedelta) * weight * qty,
                                "gamma": 0,
                                "vega": 0,
                                "theta": 0,
                                "carry link": carry_link,
                                "counterparty": counterparty,
                            }
                            if futureName in trades:
                                trades[futureName]["qty"] = (
                                    trades[futureName]["qty"] + hedge["qty"]
                                )
                            else:
                                trades[futureName] = hedge
                        else:
                            if not onestrike:
                                onestrike = ponestrike
                            # onestrike = strikePlaceholderCheck(onestrike, ponestrike)
                            name = (
                                str(month)
                                + "-"
                                + str(onestrike)
                                + "-"
                                + str(onecop).upper()
                            ).upper()
                            trades[name] = {
                                "qty": qty * weight,
                                "theo": float(onetheo),
                                "prompt": prompt,
                                "forward": Bforward,
                                "iv": float(oneiv),
                                "delta": float(onedelta) * weight * qty,
                                "gamma": float(onegamma) * weight * qty,
                                "vega": float(onevega) * weight * qty,
                                "theta": float(onetheta) * weight * qty,
                                "carry link": carry_link,
                                "counterparty": counterparty,
                            }
                            # add delta to delta bucket for hedge
                            deltaBucket += float(onedelta) * weight * qty

                    # calc two leg weight
                    if statWeights[1] != 0:
                        weight = statWeights[1] * bs
                        if twocop == "f":
                            hedge = {
                                "qty": float(onedelta) * weight * qty,
                                "theo": Bforward,
                                "prompt": prompt,
                                "forward": Bforward,
                                "iv": 0,
                                "delta": float(twodelta) * weight * qty,
                                "gamma": 0,
                                "vega": 0,
                                "theta": 0,
                                "carry link": carry_link,
                                "counterparty": counterparty,
                            }
                            if futureName in trades:
                                trades[futureName]["qty"] = (
                                    trades[futureName]["qty"] + hedge["qty"]
                                )
                            else:
                                trades[futureName] = hedge
                        else:
                            # get strike from value and placeholder
                            if not twostrike:
                                twostrike = ptwostrike

                            name = (
                                str(month)
                                + "-"
                                + str(twostrike)
                                + "-"
                                + str(twocop).upper()
                            ).upper()
                            trades[name] = {
                                "qty": float(qty) * weight,
                                "theo": float(twotheo),
                                "prompt": prompt,
                                "forward": Bforward,
                                "iv": float(twoiv),
                                "delta": float(twodelta) * weight * qty,
                                "gamma": float(twogamma) * weight * qty,
                                "vega": float(twovega) * weight * qty,
                                "theta": float(twotheta) * weight * qty,
                                "carry link": carry_link,
                                "counterparty": counterparty,
                            }
                            # add delta to delta bucket for hedge
                            deltaBucket += float(twodelta) * weight * qty

                    # calc three leg weight
                    if statWeights[2] != 0:
                        weight = statWeights[2] * bs
                        if twocop == "f":
                            hedge = {
                                "qty": float(threedelta) * weight * qty,
                                "theo": Bforward,
                                "prompt": prompt,
                                "forward": Bforward,
                                "iv": 0,
                                "delta": float(threedelta) * weight * qty,
                                "gamma": 0,
                                "vega": 0,
                                "theta": 0,
                                "carry link": carry_link,
                                "counterparty": counterparty,
                            }
                            if futureName in trades:
                                trades[futureName]["qty"] = (
                                    trades[futureName]["qty"] + hedge["qty"]
                                )
                            else:
                                trades[futureName] = hedge
                        else:
                            # get strike from value and placeholder
                            if not threestrike:
                                threestrike = pthreestrike
                            name = (
                                str(month)
                                + "-"
                                + str(threestrike)
                                + "-"
                                + str(threecop).upper()
                            ).upper()
                            trades[name] = {
                                "qty": float(qty) * weight,
                                "theo": float(threetheo),
                                "prompt": prompt,
                                "forward": Bforward,
                                "iv": float(threeiv),
                                "delta": float(threedelta) * weight * qty,
                                "gamma": float(threegamma) * weight * qty,
                                "vega": float(threevega) * weight * qty,
                                "theta": float(threetheta) * weight * qty,
                                "carry link": carry_link,
                                "counterparty": counterparty,
                            }
                            # add delta to delta bucket for hedge
                            deltaBucket += float(threedelta) * weight * qty

                    # calc four leg weight
                    if statWeights[3] != 0:
                        weight = statWeights[3] * bs
                        if twocop == "f":
                            hedge = {
                                "qty": float(fourdelta) * weight * qty,
                                "theo": Bforward,
                                "prompt": prompt,
                                "forward": Bforward,
                                "iv": 0,
                                "delta": float(fourdelta) * weight * qty,
                                "gamma": 0,
                                "vega": 0,
                                "theta": 0,
                                "carry link": carry_link,
                                "counterparty": counterparty,
                            }
                            if futureName in trades:
                                trades[futureName]["qty"] = (
                                    trades[futureName]["qty"] + hedge["qty"]
                                )
                            else:
                                trades[futureName] = hedge
                        else:
                            # get strike from value and placeholder
                            if not fourstrike:
                                fourstrike = pfourstrike
                            name = (
                                str(month)
                                + "-"
                                + str(fourstrike)
                                + "-"
                                + str(fourcop).upper()
                            ).upper()
                            trades[name] = {
                                "qty": float(qty) * weight,
                                "theo": float(fourtheo),
                                "prompt": prompt,
                                "forward": Bforward,
                                "iv": float(fouriv),
                                "delta": float(fourdelta) * weight * qty,
                                "gamma": float(fourgamma) * weight * qty,
                                "vega": float(fourvega) * weight * qty,
                                "theta": float(fourtheta) * weight * qty,
                                "carry link": carry_link,
                                "counterparty": counterparty,
                            }
                            # add delta to delta bucket for hedge
                            deltaBucket += float(fourdelta) * weight * qty
                    # if vol trade then add hedge along side
                    if pricevola == "vol":
                        delta = round(float(deltaBucket), 0) * -1

                        hedge = {
                            "qty": delta,
                            "theo": Bforward,
                            "prompt": prompt,
                            "forward": Bforward,
                            "iv": 0,
                            "delta": delta,
                            "gamma": 0,
                            "vega": 0,
                            "theta": 0,
                            "carry link": carry_link,
                            "counterparty": counterparty,
                        }
                        if futureName in trades:
                            trades[futureName]["qty"] = (
                                trades[futureName]["qty"] + hedge["qty"]
                            )
                        else:
                            trades[futureName] = hedge
            return trades, clickdata

    # @app.callback(
    #     [],
    #     [Input("calc-settle-internal-c2", "disabled"),
    #     Input("calculatorVol_price-c2", "value"),
    # ])
    # def disable_int_settle_vol_radio_on_volprice(vol_price_value: str) -> bool:
    #     return vol_price_value == "vol"

    # delete all input values on product changes DONE
    @app.callback(
        [
            Output("oneStrike-c2", "value"),
            Output("oneVol_price-c2", "value"),
            Output("twoStrike-c2", "value"),
            Output("twoVol_price-c2", "value"),
            Output("threeStrike-c2", "value"),
            Output("threeVol_price-c2", "value"),
            Output("fourStrike-c2", "value"),
            Output("fourVol_price-c2", "value"),
        ],
        [
            Input("productCalc-selector-c2", "value"),
            Input("monthCalc-selector-c2", "value"),
        ],
    )
    def clearSelectedRows(product, month):
        return "", "", "", "", "", "", "", ""

    # send trade to system  DONE - double booking - (possibly need book w new name?)
    @app.callback(
        Output("tradeSent-c2", "is_open"),
        Output("tradeSentFail-c2", "is_open"),
        Output("tradeSentFail-c2", "children"),
        [Input("trade-c2", "n_clicks")],
        [State("tradesTable-c2", "selected_rows"), State("tradesTable-c2", "data")],
        prevent_initial_call=True,
    )
    def sendTrades(clicks, indices, rows):
        timestamp = timeStamp()
        # pull username from site header
        user = request.headers.get("X-MS-CLIENT-PRINCIPAL-NAME")
        if not user:
            user = "TEST"

        if indices:
            # set variables shared by all trades
            # start of borrowed logic from carry page
            packaged_trades_to_send_legacy = []
            packaged_trades_to_send_new = []
            trader_id = 0
            upsert_pos_params = []
            trade_time_ns = time.time_ns()
            booking_dt = datetime.utcnow()

            with shared_engine.connect() as cnxn:
                stmt = sqlalchemy.text(
                    "SELECT trader_id FROM traders WHERE email = :user_email"
                )
                result = cnxn.execute(
                    stmt, {"user_email": user.lower()}
                ).scalar_one_or_none()
                if result is None:
                    trader_id = -101
                else:
                    trader_id = result
            print(rows)
            for i in indices:
                # check that this is not the total line.
                if rows[i]["Instrument"] != "Total":
                    try:
                        portfolio_id = rows[i]["Account"]
                        if portfolio_id is None:
                            error_msg = (
                                f"No account selected for row {i+1} of trades table"
                            )
                            print(error_msg)
                            return False, True, [error_msg]
                    except KeyError:
                        error_msg = f"No account selected for row {i+1} of trades table"
                        print(error_msg)
                        return False, True, [error_msg]
                    # OPTIONS
                    if rows[i]["Instrument"][-1] in ["C", "P"]:
                        # is option in format: "XEXT-EBM-EUR O 23-04-17 A-254-C"
                        product = " ".join(rows[i]["Instrument"].split(" ")[:3])
                        product = (
                            product + " " + rows[i]["Instrument"].split(" ")[-1][0]
                        )
                        instrument = rows[i]["Instrument"]
                        info = rows[i]["Instrument"].split(" ")[3]
                        strike, CoP = info.split("-")[1:3]

                        prompt = rows[i]["Prompt"]
                        price = float(rows[i]["Theo"])
                        qty = int(rows[i]["Qty"])
                        counterparty = rows[i]["Counterparty"]
                        if counterparty is None or counterparty == "":
                            error_msg = f"No counterparty selected for row {i+1} of trades table"
                            print(error_msg)
                            return False, True, [error_msg]

                        # variables saved, now build class to send to DB twice
                        # trade_row = trade_table_data[trade_row_index]
                        processed_user = user.replace(" ", "").split("@")[0]
                        georgia_trade_id = f"calc2.{processed_user}.{trade_time_ns}:{i}"

                        packaged_trades_to_send_legacy.append(
                            sql_utils.LegacyTradesTable(
                                dateTime=booking_dt,
                                instrument=instrument,
                                price=price,
                                quanitity=qty,
                                theo=0.0,
                                user=user,
                                counterPart=counterparty,
                                Comment="CALC2",
                                prompt=prompt,
                                venue="Georgia",
                                deleted=0,
                                venue_trade_id=georgia_trade_id,
                            )
                        )
                        packaged_trades_to_send_new.append(
                            sql_utils.TradesTable(
                                trade_datetime_utc=booking_dt,
                                instrument_symbol=instrument.lower(),
                                quantity=qty,
                                price=price,
                                portfolio_id=portfolio_id,
                                trader_id=trader_id,
                                notes="CALC2",
                                venue_name="Georgia",
                                venue_trade_id=georgia_trade_id,
                                counterparty=counterparty,
                            )
                        )
                        upsert_pos_params.append(
                            {
                                "qty": qty,
                                "instrument": instrument,
                                "tstamp": booking_dt,
                            }
                        )
                    # FUTURES
                    elif rows[i]["Instrument"].split(" ")[1] == "F":  # done
                        # is futures in format: "XEXT-EBM-EUR F 23-05-10"
                        product = rows[i]["Instrument"]
                        prompt = rows[i]["Prompt"]
                        price = float(rows[i]["Theo"])
                        qty = int(rows[i]["Qty"])
                        counterparty = rows[i]["Counterparty"]

                        processed_user = user.replace(" ", "").split("@")[0]
                        georgia_trade_id = f"calc2.{processed_user}.{trade_time_ns}:{i}"

                        packaged_trades_to_send_legacy.append(
                            sql_utils.LegacyTradesTable(
                                dateTime=booking_dt,
                                instrument=product,
                                price=price,
                                quanitity=qty,
                                theo=0.0,
                                user=user,
                                counterPart=counterparty,
                                Comment="CALC2",
                                prompt=prompt,
                                venue="Georgia",
                                deleted=0,
                                venue_trade_id=georgia_trade_id,
                            )
                        )
                        packaged_trades_to_send_new.append(
                            sql_utils.TradesTable(
                                trade_datetime_utc=booking_dt,
                                instrument_symbol=product.lower(),
                                quantity=qty,
                                price=price,
                                portfolio_id=portfolio_id,
                                trader_id=trader_id,
                                notes="CALC2",
                                venue_name="Georgia",
                                venue_trade_id=georgia_trade_id,
                                counterparty=counterparty,
                            )
                        )
                        upsert_pos_params.append(
                            {
                                "qty": qty,
                                "instrument": product,
                                "tstamp": booking_dt,
                            }
                        )
                        # END OF FUTURES

            # send trades to db
            try:
                with sqlalchemy.orm.Session(
                    shared_engine, expire_on_commit=False
                ) as session:
                    session.add_all(packaged_trades_to_send_new)
                    session.commit()
            except Exception:
                error_msg = (
                    "Exception while attempting to book trade in new standard table"
                )
                print(error_msg)
                print(traceback.format_exc())
                return False, True, [error_msg]
            try:
                with sqlalchemy.orm.Session(legacyEngine) as session:
                    session.add_all(packaged_trades_to_send_legacy)
                    pos_upsert_statement = sqlalchemy.text(
                        "SELECT upsert_position(:qty, :instrument, :tstamp)"
                    )
                    _ = session.execute(pos_upsert_statement, params=upsert_pos_params)
                    session.commit()
            except Exception:
                error_msg = "Exception while attempting to book trade in legacy table"
                print(error_msg)
                print(traceback.format_exc())
                for trade in packaged_trades_to_send_new:
                    trade.deleted = True
                # to clear up new trades table assuming they were booked correctly
                # on there
                with sqlalchemy.orm.Session(shared_engine) as session:
                    session.add_all(packaged_trades_to_send_new)
                    session.commit()
                return False, True, [error_msg]

            # send trades to redis
            # try:
            #     with legacyEngine.connect() as pg_connection:
            #         trades = pd.read_sql("trades", pg_connection)
            #         positions = pd.read_sql("positions", pg_connection)

            #     trades.columns = trades.columns.str.lower()
            #     positions.columns = positions.columns.str.lower()
            # except Exception:
            #     error_msg = (
            #         "Exception encountered while trying to update redis trades/position"
            #     )
            #     print(error_msg)
            #     print(traceback.format_exc())
            #     return False, True, [error_msg]

            return True, False, ["Trade failed to save"]

    # moved recap button to its own dedicated callback away from Report - DONE
    @app.callback(
        Output("reponseOutput-c2", "children"),
        Input("clientRecap-c2", "n_clicks_timestamp"),
        [State("tradesTable-c2", "selected_rows"), State("tradesTable-c2", "data")],
    )
    def recap(clicks, indices, rows):
        # enact trade recap logic
        if clicks:
            response = "Recap: \r\n"

            if indices:
                for i in indices:
                    if rows[i]["Instrument"][-1] in ["C", "P"]:
                        # is option format: "XEXT-EBM-EUR O 23-04-17 A-254-C"
                        product, type, prompt, info = rows[i]["Instrument"].split(" ")

                        strike, CoP = info.split("-")[1:3]

                        prompt = datetime.strptime(prompt, "%y-%m-%d")
                        month = prompt.strftime("%b")[:3]

                        if CoP == "C":
                            CoP = "calls"
                        elif CoP == "P":
                            CoP = "puts"

                        price = round(abs(float(rows[i]["Theo"])), 2)
                        qty = float(rows[i]["Qty"])
                        vol = round(abs(float(rows[i]["IV"])), 2)
                        if qty > 0:
                            bs = "Sell"
                        elif qty < 0:
                            bs = "Buy"
                        else:
                            continue

                        response += "You {} {} {} {} {} {} at {} ({}%) \r\n".format(
                            bs,
                            abs(int(qty)),
                            month,
                            product.lower(),
                            strike,
                            CoP,
                            price,
                            round(vol, 2),
                        )
                    elif rows[i]["Instrument"].split(" ")[1] == "F":
                        # is futures in format: "XEXT-EBM-EUR F 23-05-10"
                        product, type, prompt = rows[i]["Instrument"].split(" ")

                        prompt = datetime.strptime(prompt, "%y-%m-%d")
                        month = prompt.strftime("%b")[:3]

                        price = rows[i]["Theo"]
                        qty = rows[i]["Qty"]
                        if qty > 0:
                            bs = "Sell"
                        elif qty < 0:
                            bs = "Buy"
                        response += "You {} {} {} {} at {} \r\n".format(
                            bs, abs(int(qty)), month, product.lower(), price
                        )

                return response
            else:
                return "No rows selected"

    @app.callback(  # DONE
        Output("calculatorPrice/Vola-c2", "value"),
        [
            Input("productCalc-selector-c2", "value"),
            Input("monthCalc-selector-c2", "value"),
        ],
    )
    def loadBasis(product, month):
        return ""

    @app.callback(
        [Output("days-per-year-c2", "children"), Output("t-to-expiry-c2", "children")],
        [Input("productHelperInfo-c2", "data")],
    )
    def update_option_info_rhs(product_helper_data):
        # TODO: fix these aren't populating on screen now for some reason
        if not product_helper_data:
            return "", ""
        return (
            product_helper_data["days_forward_year"],
            round(product_helper_data["expiry_time"], 6),
        )

    @app.callback(
        [Output("open-live-time-correction-c2", "data")],
        [Input("nowOpen-c2", "value"), Input("productHelperInfo-c2", "data")],
    )
    def update_open_live_time_correction(live_or_open, product_helper_data):
        if live_or_open == "open":
            product_locale = ZoneInfo(product_helper_data["locale"])
            now_datetime = datetime.now(product_locale)
            now_date = now_datetime.date()
            busday_start = datetime.combine(
                now_date,
                dt_time.fromisoformat(product_helper_data["busday_start"]),
                product_locale,
            )
            busday_end = datetime.combine(
                now_date,
                dt_time.fromisoformat(product_helper_data["busday_end"]),
                product_locale,
            )
            frac_through_busday = (now_datetime - busday_start).total_seconds() / (
                (busday_end - busday_start).total_seconds()
                * product_helper_data["days_forward_year"]
            )
            return [frac_through_busday]
        return [0.0]

    # update product info on product change # MIGHT NEED CHANGING!!
    @app.callback(
        [
            Output("productInfo-c2", "data"),
            Output("productHelperInfo-c2", "data"),
            Output("underlying-closing-price-c2", "data"),
            Output("strike-settlement-vols-c2", "data"),
        ],
        [
            Input("productCalc-selector-c2", "value"),
            Input("monthCalc-selector-c2", "value"),
            Input("productDataRefreshInterval-c2", "n_intervals"),
            # Input("monthCalc-selector-c2", "options"),
        ],
    )
    def updateProduct(product, month, refresh_interval):
        if product and month:
            pipeline = conn.pipeline()
            pipeline.get(month + dev_key_redis_append)
            pipeline.get(month + ":frontend_helper_data" + dev_key_redis_append)
            pipeline.get("v2:gli:" + month + ":osp" + dev_key_redis_append)
            pipeline.get("v2:gli:" + month + ":fcp" + dev_key_redis_append)
            params, helper_data, op_settle, fut_settle = pipeline.execute()
            if params is None:
                print(f"Params key not populated {month+dev_key_redis_append}")
                return None, None, None, None
            params = orjson.loads(params)
            if op_settle is not None and fut_settle is not None:
                op_settle = orjson.loads(op_settle)
                fut_settle = orjson.loads(fut_settle)
            else:
                op_settle = None
                fut_settle = 0.0
            if helper_data is None:
                print(
                    f"Helper data not populated {month+':frontend_helper_data' + dev_key_redis_append}"
                )
                return (params, None, None, fut_settle)
            helper_data = orjson.loads(helper_data)
            helper_data["discount_time"] = params["und_t_to_expiry"][0]
            helper_data["expiry_time"] = params["t_to_expiry"][0]
            helper_data["multiplier"] = params["multiplier"][0]

            return params, helper_data, fut_settle, op_settle

    legOptions = ["one", "two", "three", "four"]

    def buildVoltheta():  # should be fine and stay the same
        def loadtheo(vega, theta):
            if vega != None and theta != None:
                vega = float(vega)
                if vega > 0:
                    return "%.2f" % (float(theta) / vega)
                else:
                    return 0
            else:
                return 0

        return loadtheo

    @app.callback(  # should be fine, variables the same
        Output("calculatorForward-c2", "placeholder"),
        [
            Input("calculatorBasis-c2", "value"),
            Input("calculatorBasis-c2", "placeholder"),
            Input("calculatorSpread-c2", "value"),
            Input("calculatorSpread-c2", "placeholder"),
        ],
    )
    def forward_update(basis, basisp, spread, spreadp):
        if not basis:
            basis = basisp
        if not spread:
            spread = spreadp

        return float(basis) + float(spread)

    @app.callback(
        Output("calculatorForward-c2", "value"),
        [
            Input("productCalc-selector-c2", "value"),
        ],
    )
    def forward_update(productInfo):
        return ""

    def vol_output_func():
        def warning_vol_vs_settle_split(internal_vol, settle_vol):
            return

        return warning_vol_vs_settle_split

    def warning_vol_vs_settle_split(internal_vol, settle_vol, iv_style, settle_style):
        if None in (internal_vol, settle_vol):
            return {}, {}
        if iv_style is None:
            iv_style = {}
        if settle_style is None:
            settle_style = {}
        if abs(internal_vol - settle_vol) > 1.0:
            iv_style["background-color"] = "#FFDC00"
            settle_style["background-color"] = "#FFDC00"
        else:
            try:
                del iv_style["background-color"]
            except KeyError:
                pass
            try:
                del settle_style["background-color"]
            except KeyError:
                pass
        return iv_style, settle_style

    # create placeholder function for each {leg}Strike
    for leg in legOptions:
        # clientside black scholes
        app.clientside_callback(
            ClientsideFunction(namespace="clientside", function_name="blackScholes2"),
            [
                Output("{}{}-c2".format(leg, i), "children")
                for i in ["Theo", "Delta", "Gamma", "Vega", "Theta", "IV"]
            ],
            [
                Input("calculatorVol_price-c2", "value"),
                Input("productHelperInfo-c2", "data"),
                Input("open-live-time-correction-c2", "data"),
            ]
            + [
                Input("{}{}-c2".format(leg, i), "value")  # all there
                for i in ["CoP", "Strike", "Vol_price"]
            ]
            + [
                Input("{}{}-c2".format(leg, i), "placeholder")
                for i in ["Strike", "Vol_price"]
            ]
            + [
                Input("{}-c2".format(i), "value")  # all there
                for i in ["calculatorForward", "interestRate"]
            ]
            + [
                Input("{}-c2".format(i), "placeholder")
                for i in ["calculatorForward", "interestRate"]
            ],
        )

        # calculate the vol thata from vega and theta
        app.callback(
            Output("{}volTheta-c2".format(leg), "children"),
            [
                Input("{}Vega-c2".format(leg), "children"),
                Input("{}Theta-c2".format(leg), "children"),
            ],
        )(buildVoltheta())

        app.callback(
            [Output(f"{leg}IV-c2", "style"), Output(f"{leg}SettleVol-c2", "style")],
            [
                Input(f"{leg}IV-c2", "children"),
                Input(f"{leg}SettleVol-c2", "children"),
                State(f"{leg}IV-c2", "style"),
                State(f"{leg}SettleVol-c2", "style"),
            ],
        )(warning_vol_vs_settle_split)

    def buildStratGreeks(param):
        def stratGreeks(strat, one, two, three, four, qty, mult):
            if any([one, two, three, four]) and strat:
                strat = stratConverstion[strat]
                if all([one, two, three, four]):
                    greek = (
                        (strat[0] * float(one))
                        + (strat[1] * float(two))
                        + (strat[2] * float(three))
                        + (strat[3] * float(four))
                    )
                else:
                    greek = 0.0

                # show average vol for some strats
                avg_list = ["IV", "SettleVol"]
                if strat == [1, 1, 0, 0] and (param in avg_list):
                    greek = greek / 2

                # list for greeks to mult by qty
                qty_list = ["Delta", "Gamma", "Vega", "Theta"]

                mult_list = ["Vega", "Theta"]
                # mult by qty
                if param in qty_list:
                    if qty:
                        if param in mult_list:
                            if mult:
                                greek = greek * qty * float(mult)
                        else:
                            greek = greek * qty

                if param == "Gamma":
                    greek = round(greek, 5)
                else:
                    greek = round(greek, 2)

                return str(greek)

            else:
                return 0

        return stratGreeks

    # add different greeks to leg and calc
    for param in [
        "Theo",
        # "FullDelta",
        "Delta",
        "Gamma",
        "Vega",
        "Theta",
        "IV",
        "SettleVol",
        "volTheta",
    ]:
        app.callback(
            Output("strat{}-c2".format(param), "children"),  # DONE!
            [
                Input("strategy-c2", "value"),
                Input("one{}-c2".format(param), "children"),
                Input("two{}-c2".format(param), "children"),
                Input("three{}-c2".format(param), "children"),
                Input("four{}-c2".format(param), "children"),
                Input("qty-c2", "value"),
                Input("multiplier-c2", "children"),
            ],
        )(buildStratGreeks(param))

    inputs = ["calculatorBasis-c2", "calculatorSpread-c2"]

    @app.callback(
        [Output("{}".format(i), "placeholder") for i in inputs]
        + [Output("{}".format(i), "value") for i in inputs]
        + [Output("{}Strike-c2".format(i), "placeholder") for i in legOptions],
        [
            Input("productCalc-selector-c2", "value"),
            Input("productInfo-c2", "data"),
        ],
    )
    def updateInputs(product, params):
        if product is None:
            atmList = [no_update] * len(legOptions)
            valuesList = [no_update] * len(inputs)
            return (
                [no_update for _ in len(inputs)]
                + valuesList
                + [no_update, no_update]
                + atmList
            )

        data = params

        # spread is 0 for xext
        spread = data["spread"][0]

        # basis
        basis = data["underlying_prices"][0]

        # strike using binary search
        strike_index = bisect.bisect(data["strikes"], basis)

        if abs(data["strikes"][strike_index] - basis) > abs(
            data["strikes"][strike_index - 1] - basis
        ):
            strike_index -= 1
        basis -= spread
        strike = data["strikes"][strike_index]

        return (
            [
                basis,
                spread,
            ]
            + [""] * len(inputs)
            + [strike for _ in legOptions]
        )

    # update settlement vols store on product change
    # this now replaces the buildUpdateVola function
    for leg in legOptions:

        @app.callback(
            # Output("{}SettleVol-c2".format(leg), "placeholder"),
            Output("{}SettleVol-c2".format(leg), "children"),
            Output("{}Vol_price-c2".format(leg), "placeholder"),
            [
                Input("{}Strike-c2".format(leg), "value"),
                Input("{}Strike-c2".format(leg), "placeholder"),
                Input("productInfo-c2", "data"),
                Input("strike-settlement-vols-shifted-c2", "data"),
                Input("calc-settle-internal-c2", "value"),
            ],
        )
        def updateOptionInfo(
            strike, strikePH, product_info, shifting_settlements, calc_settle_internal
        ):  # DONE
            # placeholder check

            if not strike:
                strike = strikePH
            # round strike to nearest integer
            strike = float(strike)

            product_info = pd.DataFrame(product_info)
            product_info["settlement_vol"] = shifting_settlements
            product_strike_calc_vol = product_info.loc[
                (
                    (product_info["option_types"] == 1)
                    & (product_info["strikes"].round(5) == round(strike, 5))
                ),
                "volatilities",
            ]

            settlement_vol = product_info.loc[
                (product_info["option_types"] == 1)
                & (product_info["strikes"].round(5) == round(strike, 5)),
                "settlement_vol",
            ]
            if len(settlement_vol) == 0:
                settlement_vol = 0.0
            else:
                settlement_vol = round(settlement_vol.values[0], 2)

            if calc_settle_internal == "internal":
                if len(product_strike_calc_vol) == 0:
                    product_strike_calc_vol = 0.0
                else:
                    product_strike_calc_vol = round(
                        product_strike_calc_vol.values[0] * 100, 2
                    )
            else:
                product_strike_calc_vol = settlement_vol

            return settlement_vol, product_strike_calc_vol
