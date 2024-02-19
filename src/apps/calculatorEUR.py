import bisect
import datetime as dt
import json
import os
import pickle
import time
import traceback
from datetime import date, datetime, timedelta

import dash_bootstrap_components as dbc
import email_utils as email_utils
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
from flask import request
from parts import (
    buildTradesTableData,
    get_valid_counterpart_dropdown_options,
    loadRedisData,
    topMenu,
)
from upedata import dynamic_data as upe_dynamic
from upedata import static_data as upe_static

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


def loadProducts():
    with shared_session() as session:
        products = (
            session.query(upe_static.Product)
            .where(upe_static.Product.exchange_symbol == "xext")
            .all()
        )
        return products


productList = [
    {"label": product.long_name.title(), "value": product.symbol}
    for product in loadProducts()
]


def loadOptions(optionSymbol):
    with shared_session() as session:
        product = (
            session.query(upe_static.Product)
            .where(upe_static.Product.symbol == optionSymbol)
            .first()
        )
        optionsList = (option for option in product.options)
        return optionsList


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
        # this line will only work for the next 77 years
        und_expiry = "20" + und_name.split(" ")[-1]
        mult = int(option.multiplier)
        return (expiry, und_name, und_expiry, mult)


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
                    [dcc.Input(id="calculatorBasis-EU", type="text", debounce=True)],
                    width=4,
                ),
                dbc.Col([dcc.Input(id="calculatorForward-EU", type="text")], width=4),
                dbc.Col([dcc.Input(id="interestRate-EU", type="text")], width=4),
            ]
        ),
        # second row labels
        dbc.Row(
            [
                dbc.Col([html.Div("Spread")], width=4),
                dbc.Col([html.Div("Strategy")], width=4),
                dbc.Col([html.Div("Days Convention")], width=4),
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
                                    type="text", id="calculatorSpread-EU", debounce=True
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
                                    id="strategy-EU",
                                    value="outright",
                                    options=stratOptions,
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
                                    id="dayConvention-EU",
                                    value="bis",
                                    options=[
                                        {
                                            "label": "Bis/Bis",
                                            "value": "bis",
                                        },
                                        {"label": "Calendar/365", "value": "cal"},
                                    ],
                                )
                            ]
                        )
                    ],
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
                                    id="calculatorVol_price-EU",
                                    options=[
                                        {"label": "Vol", "value": "vol"},
                                        {"label": "Price", "value": "price"},
                                    ],
                                    value="vol",
                                )
                            ]
                        )
                    ],
                    width=3,
                ),
                dbc.Col(
                    [
                        html.Div(
                            [
                                dcc.RadioItems(
                                    id="nowOpen-EU",
                                    options=[
                                        {"label": "Now", "value": "now"},
                                        {"label": "Open", "value": "open"},
                                    ],
                                    value="now",
                                )
                            ]
                        )
                    ],
                    width=3,
                ),
                dbc.Col([html.Div("Counterparty:")], width=3),
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="counterparty-EU",
                            value="",
                            options=get_valid_counterpart_dropdown_options("xext"),
                        )
                    ],
                    width=3,
                ),
            ]
        ),
        # leg inputs and outputs
        # leg inputs
        # labels
        dbc.Row(
            [
                dbc.Col(["Strike: "], width=2),
                dbc.Col([dcc.Input(id="oneStrike-EU")], width=2),
                dbc.Col([dcc.Input(id="twoStrike-EU")], width=2),
                dbc.Col([dcc.Input(id="threeStrike-EU")], width=2),
                dbc.Col([dcc.Input(id="fourStrike-EU")], width=2),
                dbc.Col(
                    [dcc.Input(id="qty-EU", type="number", value=10, min=0)], width=2
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Price/Vol: "], width=2),
                dbc.Col([dcc.Input(id="oneVol_price-EU")], width=2),
                dbc.Col([dcc.Input(id="twoVol_price-EU")], width=2),
                dbc.Col([dcc.Input(id="threeVol_price-EU")], width=2),
                dbc.Col([dcc.Input(id="fourVol_price-EU")], width=2),
                dbc.Col(
                    [
                        dbc.Button(
                            "Buy", id="buy-EU", n_clicks_timestamp="0", active=True
                        )
                    ],
                    width=1,
                ),
                dbc.Col(
                    [
                        dbc.Button(
                            "Sell", id="sell-EU", n_clicks_timestamp="0", active=True
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
                            id="oneCoP-EU",
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
                            id="twoCoP-EU",
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
                            id="threeCoP-EU",
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
                            id="fourCoP-EU",
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
                dbc.Col([html.Div(id="oneTheo-EU")], width=2),
                dbc.Col([html.Div(id="twoTheo-EU")], width=2),
                dbc.Col([html.Div(id="threeTheo-EU")], width=2),
                dbc.Col([html.Div(id="fourTheo-EU")], width=2),
                dbc.Col(
                    [html.Div(id="stratTheo-EU", style={"background": stratColColor})],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["IV: "], width=2),
                dbc.Col([html.Div(id="oneIV-EU")], width=2),
                dbc.Col([html.Div(id="twoIV-EU")], width=2),
                dbc.Col([html.Div(id="threeIV-EU")], width=2),
                dbc.Col([html.Div(id="fourIV-EU")], width=2),
                dbc.Col(
                    [html.Div(id="stratIV-EU", style={"background": stratColColor})],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Settle IV:"], width=2),
                dbc.Col([html.Div(id="oneSettleVol-EU")], width=2),
                dbc.Col([html.Div(id="twoSettleVol-EU")], width=2),
                dbc.Col([html.Div(id="threeSettleVol-EU")], width=2),
                dbc.Col([html.Div(id="fourSettleVol-EU")], width=2),
                dbc.Col(
                    [
                        html.Div(
                            id="stratSettleVol-EU", style={"background": stratColColor}
                        )
                    ],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Delta: "], width=2),
                dbc.Col([html.Div(id="oneDelta-EU")], width=2),
                dbc.Col([html.Div(id="twoDelta-EU")], width=2),
                dbc.Col([html.Div(id="threeDelta-EU")], width=2),
                dbc.Col([html.Div(id="fourDelta-EU")], width=2),
                dbc.Col(
                    [html.Div(id="stratDelta-EU", style={"background": stratColColor})],
                    width=2,
                ),
            ]
        ),
        # dbc.Row(
        #     [
        #         dbc.Col(html.Div("Full Delta: ", id="fullDeltaLabel-EU"), width=2),
        #         dbc.Col([html.Div(id="oneFullDelta-EU")], width=2),
        #         dbc.Col([html.Div(id="twoFullDelta-EU")], width=2),
        #         dbc.Col([html.Div(id="threeFullDelta-EU")], width=2),
        #         dbc.Col([html.Div(id="fourFullDelta-EU")], width=2),
        #         dbc.Col(
        #             [
        #                 html.Div(
        #                     id="stratFullDelta-EU", style={"background": stratColColor}
        #                 )
        #             ],
        #             width=2,
        #         ),
        #     ]
        # ),
        dbc.Row(
            [
                dbc.Col(["Gamma: "], width=2),
                dbc.Col([html.Div(id="oneGamma-EU")], width=2),
                dbc.Col([html.Div(id="twoGamma-EU")], width=2),
                dbc.Col([html.Div(id="threeGamma-EU")], width=2),
                dbc.Col([html.Div(id="fourGamma-EU")], width=2),
                dbc.Col(
                    [html.Div(id="stratGamma-EU", style={"background": stratColColor})],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Vega: "], width=2),
                dbc.Col([html.Div(id="oneVega-EU")], width=2),
                dbc.Col([html.Div(id="twoVega-EU")], width=2),
                dbc.Col([html.Div(id="threeVega-EU")], width=2),
                dbc.Col([html.Div(id="fourVega-EU")], width=2),
                dbc.Col(
                    [html.Div(id="stratVega-EU", style={"background": stratColColor})],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Theta: "], width=2),
                dbc.Col([html.Div(id="oneTheta-EU")], width=2),
                dbc.Col([html.Div(id="twoTheta-EU")], width=2),
                dbc.Col([html.Div(id="threeTheta-EU")], width=2),
                dbc.Col([html.Div(id="fourTheta-EU")], width=2),
                dbc.Col(
                    [html.Div(id="stratTheta-EU", style={"background": stratColColor})],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(html.Div("Vol Theta: ", id="volThetaLabel"), width=2),
                dbc.Col([html.Div(id="onevolTheta-EU")], width=2),
                dbc.Col([html.Div(id="twovolTheta-EU")], width=2),
                dbc.Col([html.Div(id="threevolTheta-EU")], width=2),
                dbc.Col([html.Div(id="fourvolTheta-EU")], width=2),
                dbc.Col(
                    [
                        html.Div(
                            id="stratvolTheta-EU", style={"background": stratColColor}
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
    dcc.Store(id="tradesStore-EU"),
    dcc.Store(id="paramsStore-EU"),
    dcc.Store(id="productInfo-EU"),
    dcc.Store(id="settleVolsStore-EU"),
    html.Div(id="trades_div-EU", style={"display": "none"}),
    html.Div(id="trade_div-EU", style={"display": "none"}),
    html.Div(id="trade_div2-EU", style={"display": "none"}),
    html.Div(id="productData-EU", style={"display": "none"}),
    html.Div(id="holsToExpiry-EU", style={"display": "none"}),
    html.Div(id="und_name-EU", style={"display": "none"}),
)

actions = dbc.Row(
    [
        dbc.Col([html.Button("Delete", id="delete-EU", n_clicks_timestamp=0)], width=3),
        dbc.Col([html.Button("Trade", id="trade-EU", n_clicks_timestamp=0)], width=3),
        dbc.Col(
            [html.Button("Client Recap", id="clientRecap-EU", n_clicks_timestamp=0)],
            width=3,
        ),
        dbc.Col(
            [
                dcc.ConfirmDialogProvider(
                    html.Button("Report", id="report-EU", n_clicks_timestamp=0),
                    id="report-confirm-EU",
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
    # {
    #     "id": "Carry Link",
    #     "name": "Carry Link",
    #     "editable": True,
    # },
    {"id": "Counterparty", "name": "Counterparty", "presentation": "dropdown"},
]

tables = dbc.Col(
    dtable.DataTable(
        id="tradesTable-EU",
        data=[{}],
        columns=columns,
        row_selectable="multi",
        editable=True,
        dropdown={
            "Counterparty": {
                "clearable": False,
                "options": get_valid_counterpart_dropdown_options("xext"),
            },
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
                        id="productCalc-selector-EU",
                        options=productList,
                        value=productList[0]["value"],
                    )
                ],
                width=12,
            )
        ),
        dbc.Row(dbc.Col([dcc.Dropdown(id="monthCalc-selector-EU")], width=12)),
        dbc.Row(dbc.Col(["Product:"], width=12)),
        dbc.Row(dbc.Col(["Option Expiry:"], width=12)),
        dbc.Row(dbc.Col([html.Div("expiry", id="calculatorExpiry-EU")], width=12)),
        dbc.Row(dbc.Col(["Underlying Expiry:"], width=12)),
        dbc.Row(dbc.Col([html.Div("und_expiry", id="3wed-EU")])),
        dbc.Row(dbc.Col(["Multiplier:"], width=12)),
        dbc.Row(dbc.Col([html.Div("mult", id="multiplier-EU")])),
    ],
    width=3,
)

output = dcc.Markdown(id="reponseOutput-EU")

alert = html.Div(
    [
        dbc.Alert(
            "Trade sent",
            id="tradeSent-EU",
            dismissable=True,
            is_open=False,
            duration=5000,
            color="success",
        ),
        dbc.Alert(
            "Trade sent",
            id="tradeSentFail-EU",
            dismissable=True,
            is_open=False,
            duration=5000,
            color="success",
        ),
        dbc.Alert(
            "Trade Routed",
            id="tradeRouted-EU",
            dismissable=True,
            is_open=False,
            duration=5000,
            color="success",
        ),
        dbc.Alert(
            "Trade Routing Failed",
            id="tradeRouteFail-EU",
            dismissable=True,
            is_open=False,
            duration=5000,
            color="danger",
        ),
        dbc.Alert(
            "Trade Routing Partially Failed",
            id="tradeRoutePartialFail-EU",
            dismissable=True,
            is_open=False,
            duration=5000,
            color="danger",
        ),
    ]
)

layout = html.Div(
    [
        topMenu("CalculatEUR"),
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
    # update months options on product change
    @app.callback(
        Output("monthCalc-selector-EU", "options"),
        [Input("productCalc-selector-EU", "value")],
    )
    def updateOptions(product):  # DONE!
        if product:
            optionsList = []
            for option in loadOptions(product):
                expiry = option.expiry.strftime("%Y-%m-%d")
                expiry = datetime.strptime(expiry, "%Y-%m-%d")
                # only show non-expired options +1 day
                if expiry >= datetime.now() - timedelta(days=1):
                    # option is named after the expiry of the underlying
                    date = option.underlying_future_symbol.split(" ")[2]
                    label = months[date[3:5]].upper() + date[1]
                    optionsList.append({"label": label, "value": option.symbol})
            return optionsList

    # update months value on product change - DONE
    @app.callback(
        Output("monthCalc-selector-EU", "value"),
        [Input("monthCalc-selector-EU", "options")],
    )
    def updatevalue(options):
        if options:
            return options[0]["value"]

    # update static data on product/month change   DONE!
    @app.callback(
        Output("multiplier-EU", "children"),
        Output("und_name-EU", "children"),
        Output("3wed-EU", "children"),
        Output("calculatorExpiry-EU", "children"),
        Output("interestRate-EU", "placeholder"),
        [Input("monthCalc-selector-EU", "value")],
    )
    def updateOptionInfo(optionSymbol):
        if optionSymbol:
            (expiry, und_name, und_expiry, mult) = getOptionInfo(optionSymbol)

            # inr
            # new inr standard - xext to use option expiry date
            inr_curve = orjson.loads(
                conn.get("prep:cont_interest_rate:usd" + dev_key_redis_append).decode(
                    "utf-8"
                )
            )
            inr = inr_curve.get(expiry.strftime("%Y%m%d")) * 100

            return mult, und_name, und_expiry, expiry, round(inr, 3)

    # update settlement vols store on product change - DONE!
    @app.callback(
        Output("settleVolsStore-EU", "data"),
        [Input("monthCalc-selector-EU", "value")],
    )
    def updateOptionInfo(optionSymbol):
        if optionSymbol:
            settle_vols = pullSettleVolsEU(optionSymbol)
            if settle_vols:
                return settle_vols
            else:
                return None

    # update business days to expiry (used for daysConvention) - DONE!
    @app.callback(
        Output("holsToExpiry-EU", "children"),
        [Input("calculatorExpiry-EU", "children")],
        [State("monthCalc-selector-EU", "value")],
        [State("productCalc-selector-EU", "value")],
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
            Output("oneCoP-EU", "options"),
            Output("twoCoP-EU", "options"),
            Output("threeCoP-EU", "options"),
            Output("fourCoP-EU", "options"),
            Output("oneCoP-EU", "value"),
            Output("twoCoP-EU", "value"),
            Output("threeCoP-EU", "value"),
            Output("fourCoP-EU", "value"),
        ],
        [Input("monthCalc-selector-EU", "value")],
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
    @app.callback(Output("tradesTable-EU", "data"), [Input("tradesStore-EU", "data")])
    def loadTradeTable(data):
        if data != None:
            trades = buildTradesTableData(data)
            return trades.to_dict("records")

        else:
            return [{}]

    # change talbe data on buy/sell delete - SHOULD BE DONE!
    @app.callback(
        [Output("tradesStore-EU", "data"), Output("tradesTable-EU", "selected_rows")],
        [
            Input("buy-EU", "n_clicks_timestamp"),
            Input("sell-EU", "n_clicks_timestamp"),
            Input("delete-EU", "n_clicks_timestamp"),
        ],
        # standard trade inputs
        [
            State("tradesTable-EU", "selected_rows"),
            State("tradesTable-EU", "data"),
            State("calculatorVol_price-EU", "value"),
            State("tradesStore-EU", "data"),
            State("counterparty-EU", "value"),  # NOT USED FOR NOW
            State("und_name-EU", "children"),
            State("3wed-EU", "children"),
            # State('trades_div' , 'children'),
            State("productCalc-selector-EU", "value"),
            State("monthCalc-selector-EU", "value"),
            State("qty-EU", "value"),
            State("strategy-EU", "value"),
            # trade value inputs
            # one vlaues
            State("oneStrike-EU", "value"),
            State("oneStrike-EU", "placeholder"),
            State("oneCoP-EU", "value"),
            State("oneTheo-EU", "children"),
            State("oneIV-EU", "children"),
            State("oneDelta-EU", "children"),
            State("oneGamma-EU", "children"),
            State("oneVega-EU", "children"),
            State("oneTheta-EU", "children"),
            # two values
            State("twoStrike-EU", "value"),
            State("twoStrike-EU", "placeholder"),
            State("twoCoP-EU", "value"),
            State("twoTheo-EU", "children"),
            State("twoIV-EU", "children"),
            State("twoDelta-EU", "children"),
            State("twoGamma-EU", "children"),
            State("twoVega-EU", "children"),
            State("twoTheta-EU", "children"),
            # three values
            State("threeStrike-EU", "value"),
            State("threeStrike-EU", "placeholder"),
            State("threeCoP-EU", "value"),
            State("threeTheo-EU", "children"),
            State("threeIV-EU", "children"),
            State("threeDelta-EU", "children"),
            State("threeGamma-EU", "children"),
            State("threeVega-EU", "children"),
            State("threeTheta-EU", "children"),
            # four values
            State("fourStrike-EU", "value"),
            State("fourStrike-EU", "placeholder"),
            State("fourCoP-EU", "value"),
            State("fourTheo-EU", "children"),
            State("fourIV-EU", "children"),
            State("fourDelta-EU", "children"),
            State("fourGamma-EU", "children"),
            State("fourVega-EU", "children"),
            State("fourTheta-EU", "children"),
            State("calculatorForward-EU", "value"),
            State("calculatorForward-EU", "placeholder"),
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

    # delete all input values on product changes DONE
    @app.callback(
        [
            Output("oneStrike-EU", "value"),
            Output("oneVol_price-EU", "value"),
            Output("twoStrike-EU", "value"),
            Output("twoVol_price-EU", "value"),
            Output("threeStrike-EU", "value"),
            Output("threeVol_price-EU", "value"),
            Output("fourStrike-EU", "value"),
            Output("fourVol_price-EU", "value"),
        ],
        [
            Input("productCalc-selector-EU", "value"),
            Input("monthCalc-selector-EU", "value"),
        ],
    )
    def clearSelectedRows(product, month):
        return "", "", "", "", "", "", "", ""

    # send trade to system  DONE - double booking - (possibly need book w new name?)
    @app.callback(
        Output("tradeSent-EU", "is_open"),
        Output("tradeSentFail-EU", "is_open"),
        [Input("trade-EU", "n_clicks")],
        [State("tradesTable-EU", "selected_rows"), State("tradesTable-EU", "data")],
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

            for i in indices:
                # create st to record which products to update in redis
                redisUpdate = set([])
                # check that this is not the total line.
                if rows[i]["Instrument"] != "Total":
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

                        redisUpdate.add(product)

                        prompt = rows[i]["Prompt"]
                        price = rows[i]["Theo"]
                        qty = rows[i]["Qty"]
                        counterparty = rows[i]["Counterparty"]

                        # variables saved, now build class to send to DB twice
                        # trade_row = trade_table_data[trade_row_index]
                        processed_user = user.replace(" ", "").split("@")[0]
                        georgia_trade_id = (
                            f"calcxext.{processed_user}.{trade_time_ns}:{i}"
                        )

                        packaged_trades_to_send_legacy.append(
                            sql_utils.LegacyTradesTable(
                                dateTime=booking_dt,
                                instrument=instrument,
                                price=price,
                                quanitity=qty,
                                theo=0.0,
                                user=user,
                                counterPart=counterparty,
                                Comment="XEXT CALC",
                                prompt=prompt,
                                venue="Georgia",
                                deleted=0,
                                venue_trade_id=georgia_trade_id,
                            )
                        )
                        packaged_trades_to_send_new.append(
                            sql_utils.TradesTable(
                                trade_datetime_utc=booking_dt,
                                instrument_symbol=instrument,
                                quantity=qty,
                                price=price,
                                portfolio_id=3,  # euronext portfolio id = 3
                                trader_id=trader_id,
                                notes="XEXT CALC",
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
                        redisUpdate.add(product)
                        prompt = rows[i]["Prompt"]
                        price = rows[i]["Theo"]
                        qty = rows[i]["Qty"]
                        counterparty = rows[i]["Counterparty"]

                        processed_user = user.replace(" ", "").split("@")[0]
                        georgia_trade_id = (
                            f"calcxext.{processed_user}.{trade_time_ns}:{i}"
                        )

                        packaged_trades_to_send_legacy.append(
                            sql_utils.LegacyTradesTable(
                                dateTime=booking_dt,
                                instrument=product,
                                price=price,
                                quanitity=qty,
                                theo=0.0,
                                user=user,
                                counterPart=counterparty,
                                Comment="XEXT CALC",
                                prompt=prompt,
                                venue="Georgia",
                                deleted=0,
                                venue_trade_id=georgia_trade_id,
                            )
                        )
                        packaged_trades_to_send_new.append(
                            sql_utils.TradesTable(
                                trade_datetime_utc=booking_dt,
                                instrument_symbol=product,
                                quantity=qty,
                                price=price,
                                portfolio_id=3,  # euronext portfolio id = 3
                                trader_id=trader_id,
                                notes="XEXT CALC",
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
                print("Exception while attempting to book trade in new standard table")
                print(traceback.format_exc())
                return False, True
            try:
                with sqlalchemy.orm.Session(legacyEngine) as session:
                    session.add_all(packaged_trades_to_send_legacy)
                    pos_upsert_statement = sqlalchemy.text(
                        "SELECT upsert_position(:qty, :instrument, :tstamp)"
                    )
                    _ = session.execute(pos_upsert_statement, params=upsert_pos_params)
                    session.commit()
            except Exception:
                print("Exception while attempting to book trade in legacy table")
                print(traceback.format_exc())
                for trade in packaged_trades_to_send_new:
                    trade.deleted = True
                # to clear up new trades table assuming they were booked correctly
                # on there
                with sqlalchemy.orm.Session(shared_engine) as session:
                    session.add_all(packaged_trades_to_send_new)
                    session.commit()
                return False, True

            # send trades to redis
            try:
                with legacyEngine.connect() as pg_connection:
                    trades = pd.read_sql("trades", pg_connection)
                    positions = pd.read_sql("positions", pg_connection)

                trades.columns = trades.columns.str.lower()
                positions.columns = positions.columns.str.lower()

                pipeline = conn.pipeline()
                pipeline.set("trades" + dev_key_redis_append, pickle.dumps(trades))
                pipeline.set(
                    "positions" + dev_key_redis_append, pickle.dumps(positions)
                )
                pipeline.execute()
            except Exception:
                print("Exception encountered while trying to update redis trades/posi")
                print(traceback.format_exc())
                return False, True

            return True, False

    # moved recap button to its own dedicated callback away from Report - DONE
    @app.callback(
        Output("reponseOutput-EU", "children"),
        Input("clientRecap-EU", "n_clicks_timestamp"),
        [State("tradesTable-EU", "selected_rows"), State("tradesTable-EU", "data")],
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
        Output("calculatorPrice/Vola-EU", "value"),
        [
            Input("productCalc-selector-EU", "value"),
            Input("monthCalc-selector-EU", "value"),
        ],
    )
    def loadBasis(product, month):
        return ""

    # update product info on product change # MIGHT NEED CHANGING!!
    @app.callback(
        Output("productInfo-EU", "data"),
        [
            Input("productCalc-selector-EU", "value"),
            Input("monthCalc-selector-EU", "value"),
            # Input("monthCalc-selector-EU", "options"),
        ],
    )
    def updateProduct(product, month):
        if product and month:
            # this will be outputting redis data from option engine, currently no euronext keys in redis
            # for euronext wheat, feb/march is 'xext-ebm-eur o 23-02-15 a'
            # OVERWRITING USER INPUT FOR TESTING
            # month = "lcuom3"
            if USE_DEV_KEYS:
                month = month  # + ":dev"
            params = loadRedisData(month)
            params = orjson.loads(params)
            return params

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
        Output("calculatorForward-EU", "placeholder"),
        [
            Input("calculatorBasis-EU", "value"),
            Input("calculatorBasis-EU", "placeholder"),
            Input("calculatorSpread-EU", "value"),
            Input("calculatorSpread-EU", "placeholder"),
        ],
    )
    def forward_update(basis, basisp, spread, spreadp):
        if not basis:
            basis = basisp
        if not spread:
            spread = spreadp

        return float(basis) + float(spread)

    @app.callback(
        Output("calculatorForward-EU", "value"),
        [
            Input("productInfo-EU", "data"),
        ],
    )
    def forward_update(productInfo):
        return ""

    # create placeholder function for each {leg}Strike
    for leg in legOptions:
        # clientside black scholes
        app.clientside_callback(
            ClientsideFunction(namespace="clientside", function_name="blackScholesEU"),
            [
                Output("{}{}-EU".format(leg, i), "children")
                for i in ["Theo", "Delta", "Gamma", "Vega", "Theta", "IV"]
            ],
            [Input("calculatorVol_price-EU", "value")],  # radio button
            [Input("nowOpen-EU", "value")],  # now or open trade
            [Input("dayConvention-EU", "value")],  # days convention
            [Input("holsToExpiry-EU", "children")]  # holidays to discount
            + [
                Input("{}{}-EU".format(leg, i), "value")  # all there
                for i in ["CoP", "Strike", "Vol_price"]
            ]
            + [
                Input("{}{}-EU".format(leg, i), "placeholder")
                for i in ["Strike", "Vol_price"]
            ]
            + [
                Input("{}-EU".format(i), "value")  # all there
                for i in ["calculatorForward", "interestRate"]
            ]
            + [
                Input("{}-EU".format(i), "placeholder")
                for i in ["calculatorForward", "interestRate"]
            ]
            + [Input("calculatorExpiry-EU", "children")],  # all there
        )

        # calculate the vol thata from vega and theta
        app.callback(
            Output("{}volTheta-EU".format(leg), "children"),
            [
                Input("{}Vega-EU".format(leg), "children"),
                Input("{}Theta-EU".format(leg), "children"),
            ],
        )(buildVoltheta())

    def buildStratGreeks(param):
        def stratGreeks(strat, one, two, three, four, qty, mult):
            if any([one, two, three, four]) and strat:
                strat = stratConverstion[strat]
                greek = (
                    (strat[0] * float(one))
                    + (strat[1] * float(two))
                    + (strat[2] * float(three))
                    + (strat[3] * float(four))
                )

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
            Output("strat{}-EU".format(param), "children"),  # DONE!
            [
                Input("strategy-EU", "value"),
                Input("one{}-EU".format(param), "children"),
                Input("two{}-EU".format(param), "children"),
                Input("three{}-EU".format(param), "children"),
                Input("four{}-EU".format(param), "children"),
                Input("qty-EU", "value"),
                Input("multiplier-EU", "children"),
            ],
        )(buildStratGreeks(param))

    inputs = ["calculatorBasis-EU", "calculatorSpread-EU"]

    @app.callback(
        [Output("{}".format(i), "placeholder") for i in inputs]
        + [Output("{}".format(i), "value") for i in inputs]
        + [Output("{}Strike-EU".format(i), "placeholder") for i in legOptions],
        [
            Input("productCalc-selector-EU", "value"),
            Input("monthCalc-selector-EU", "value"),
            Input("productInfo-EU", "data"),
        ],
    )
    def updateInputs(product, month, params):
        #
        if product and month:
            # format: xlme-lad-usd o yy-mm-dd a:dev
            month += dev_key_redis_append

            data = orjson.loads(conn.get(month).decode("utf-8"))

            # basis
            basis = data["underlying_prices"][0]

            # spread is 0 for xext
            spread = 0

            # strike using binary search
            strike_index = bisect.bisect(data["strikes"], basis)

            if abs(data["strikes"][strike_index] - basis) > abs(
                data["strikes"][strike_index - 1] - basis
            ):
                strike_index -= 1

            strike = data["strikes"][strike_index]

            return (
                [
                    basis,
                    spread,
                ]
                + [""] * len(inputs)
                + [strike for _ in legOptions]
            )

        else:
            atmList = [no_update] * len(legOptions)
            valuesList = [no_update] * len(inputs)
            return (
                [no_update for _ in len(inputs)]
                + valuesList
                + [no_update, no_update]
                + atmList
            )

    # update settlement vols store on product change
    # this now replaces the buildUpdateVola function
    for leg in legOptions:

        @app.callback(
            # Output("{}SettleVol-EU".format(leg), "placeholder"),
            Output("{}SettleVol-EU".format(leg), "children"),
            Output("{}Vol_price-EU".format(leg), "placeholder"),
            [Input("{}Strike-EU".format(leg), "value")],
            [Input("{}Strike-EU".format(leg), "placeholder")],
            Input("settleVolsStore-EU", "data"),
        )
        def updateOptionInfo(strike, strikePH, settleVols):  # DONE
            # placeholder check
            if not settleVols:
                return 0, 0

            if not strike:
                strike = strikePH
            # round strike to nearest integer
            strike = int(strike)

            # array of dicts to df
            df = pd.DataFrame(settleVols)

            # set strike behaviour on the wings
            min = df["strike"].min()
            max = df["strike"].max()

            if strike > max:
                strike = max
            elif strike < min:
                strike = min

            # get the row of the df with the strike
            vol = df.loc[df["strike"] == strike]["vol"].values[0]
            vol = round(vol, 2)

            return vol, vol
