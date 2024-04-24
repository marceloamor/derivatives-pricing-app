import bisect
import datetime as dt
import os
import pickle
import tempfile
import time
import traceback
import uuid
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
from dash.exceptions import PreventUpdate
from data_connections import (
    PostGresEngine,
    conn,
    shared_engine,
    shared_session,
)
from flask import request
from parts import (
    buildTradesTableData,
    calc_lme_vol,
    codeToMonth,
    codeToName,
    get_valid_counterpart_dropdown_options,
    loadRedisData,
    loadStaticData,
    topMenu,
)
from upedata import static_data as upe_static

USE_DEV_KEYS = os.getenv("USE_DEV_KEYS", "false").lower() in [
    "true",
    "t",
    "1",
    "y",
    "yes",
]
if USE_DEV_KEYS:
    from icecream import ic

dev_key_redis_append = "" if not USE_DEV_KEYS else ":dev"

legacyEngine = PostGresEngine()

clearing_email = os.getenv(
    "CLEARING_EMAIL", default="frederick.fillingham@upetrading.com"
)
clearing_cc_email = os.getenv("CLEARING_CC_EMAIL", default="lmeclearing@upetrading.com")

stratColColor = "#9CABAA"

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


class BadCarryInput(Exception):
    pass


# re organise to have both calculator pages share essential functions
def loadProducts():
    with shared_session() as session:
        products = (
            session.query(upe_static.Product)
            .where(upe_static.Product.exchange_symbol == "xlme")
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
        # this line will only work for the next 76 years
        und_expiry = "20" + und_name.split(" ")[-1]
        mult = int(option.multiplier)
        return (expiry, und_name, und_expiry, mult)


def timeStamp():
    now = dt.datetime.now()
    now.strftime("%Y-%m-%d %H:%M:%S")
    return now


def build_trade_for_report(rows, destination="Eclipse"):
    # pull staticdata for contract name conversation
    static = loadStaticData()

    # trade date/time
    now = datetime.utcnow()
    trade_day = now.strftime(r"%d-%b-%y")
    trade_time = now.strftime(r"%H:%M:%S")

    # function to convert instrument to seals details
    def georgia_seals_name_convert(product, static):
        product = product.split()
        if len(product) > 2:
            product_type = product[2]
            strike_price = product[1]
            expiry = static.loc[static["product"] == product[0], "expiry"].values[0]
            datetime_object = datetime.strptime(expiry, "%d/%m/%Y")
            expiry = datetime_object.strftime(r"%d-%b-%y")

        else:
            product_type = "F"
            strike_price = ""
            expiry = product[1]
            datetime_object = datetime.strptime(expiry, "%Y-%m-%d")
            expiry = datetime_object.strftime(r"%d-%b-%y")

        underlying = product[0][:3]
        product_code = static.loc[static["f2_name"] == underlying, "seals_code"].values[
            0
        ]

        return product_type, strike_price, product_code, expiry

    # function to convert instrument to eclipse
    def georgia_eclipse_name_convert(product, static):
        # split product into parts
        product = product.split()

        # if len > 2 then must be option, this is not sketchy logic!
        if len(product) > 2:
            contract_type = product[2]
            strike_price = product[1]

            expiry = static.loc[static["product"] == product[0], "expiry"].values[0]
            datetime_object = datetime.strptime(expiry, "%d/%m/%Y") + timedelta(
                weeks=+2
            )
            expiry = datetime_object.strftime(r"%d-%b-%y")
            delivery = product[0][-2]
            external_id = static.loc[
                static["product"] == product[0], "lme_short_name"
            ].values[0]

        else:
            contract_type = "F"
            strike_price = ""
            expiry = product[1]
            delivery = product[1]
            datetime_object = datetime.strptime(expiry, "%Y-%m-%d")
            expiry = datetime_object.strftime(r"%d-%b-%y")
            # print(static)
            # print(product)
            external_id = static.loc[
                product[0] == static["f2_name"], "lme_short_name"
            ].values[0]

        underlying = product[0][:3]
        product_code = static.loc[static["f2_name"] == underlying, "seals_code"].values[
            0
        ]

        return contract_type, strike_price, product_code, expiry, external_id

    def georgia_marex_name_convert(product, static):
        product = product.split()
        if len(product) > 2:
            product_type = "CALL" if product[2] == "C" else "PUT"
            strike_price = product[1]
            expiry = datetime.strptime(
                static.loc[static["product"] == product[0], "expiry"].values[0],
                r"%d/%m/%Y",
            ).strftime(r"%Y%m%d")
        else:
            product_type = "FUTURE"
            strike_price = "0"
            expiry = product[1]
            datetime_object = datetime.strptime(expiry, r"%Y-%m-%d")
            expiry = datetime_object.strftime(r"%Y%m%d")

        underlying = product[0][:3]
        product_code = static.loc[
            static["f2_name"] == underlying, "eclipse_code"
        ].values[0]

        return product_type, strike_price, product_code, expiry

    if destination == "Seals":
        # standard columns required in the seals file
        seals_columns = [
            "Unique Identifier",
            "SEALSClient",
            "RegistrationType",
            "Counterparty",
            "ProductCode",
            "ProductType",
            "Expiry",
            "BuySell",
            "Price/Premium",
            "Volume",
            "StrikePrice",
            "Volatility",
            "UnderlyingPrice",
            "TradeDate",
            "TradeTime",
            "PublicReference",
            "PrivateReference",
            "Carry_Expiry",
            "Carry_BuySell",
            "Carry_Price/Premium",
            "Carry_Volume",
        ]

        # build base DF to add to
        to_send_df = pd.DataFrame(columns=seals_columns, index=list(range(len(rows))))

        # load static requirements in
        to_send_df["SEALSClient"] = "ZUPE"
        to_send_df["PrivateReference"] = "BH001"
        to_send_df["TradeDate"] = trade_day
        to_send_df["TradeTime"] = trade_time

        # loop over the indices
        for i in range(len(rows)):
            # if total rowthen skip
            if rows[i]["Instrument"] == "Total":
                continue
            # add dynamic columns
            clearer = sftp_utils.get_clearer_from_counterparty(
                rows[i]["Counterparty"].upper()
            )
            if clearer is not None:
                to_send_df.loc[i, "Counterparty"] = clearer
            else:
                return (None, destination)

            (
                to_send_df.loc[i, "ProductType"],
                to_send_df.loc[i, "StrikePrice"],
                to_send_df.loc[i, "ProductCode"],
                to_send_df.loc[i, "Expiry"],
            ) = georgia_seals_name_convert(rows[i]["Instrument"], static)
            if to_send_df.loc[i, "ProductType"] != "F":
                to_send_df.loc[i, "UnderlyingPrice"] = rows[i]["Forward"]

            # take B/S from Qty
            if int(rows[i]["Qty"]) > 0:
                to_send_df.loc[i, "BuySell"] = "B"
                to_send_df.loc[i, "Volume"] = rows[i]["Qty"]
            elif int(rows[i]["Qty"]) < 0:
                to_send_df.loc[i, "BuySell"] = "S"
                to_send_df.loc[i, "Volume"] = int(rows[i]["Qty"]) * -1

            to_send_df.loc[i, "Price/Premium"] = rows[i]["Theo"]
            to_send_df.loc[i, "Unique Identifier"] = f"upe-{str(uuid.uuid4())}"

            if float(rows[i]["IV"]) == 0:
                to_send_df.loc[i, "Volatility"] = ""
            else:
                to_send_df.loc[i, "Volatility"] = rows[i]["IV"]

            to_send_df.loc[i, "RegistrationType"] = "DD"

    elif destination == "Eclipse":
        # standard columns required in the eclipse file
        eclipse_columns = [
            "TradeType",
            "TradeReference",
            "TradeStatus",
            "Client",
            "SubAccount",
            "Broker",
            "Contract",
            "ContractType",
            "Exchange",
            "ExternalInstrumentID",
            "Del",
            "Strike",
            "StrikeSeq",
            "Lotsize",
            "BuySell",
            "Lots",
            "Price",
            "TrDate",
            "ExeBkr",
            "RecBkr",
            "ClientComm",
            "BrokerComm",
            "TradeTime",
            "TradeSource",
            "CommTradeType",
            "OpenClose",
            "UTI",
            "price2str",
            "Tvtic",
            "TradingCapacity",
            "StrategyPrice",
            "ComplexTradeId",
            "Waivers",
            "OtcType",
            "CommReduceRiskYN",
            "DeaIndYN",
            "BSShortCode",
            "BSDecision",
            "BSTransmitter",
            "InvmtDecCode",
            "InvmtDecType",
            "ExecIdCode",
            "ExecIdType",
            "EmirRtn",
            "ApsIndicator",
            "CleanPrice",
            "OrderType",
            "TradeExecutionNanoSeconds",
            "ApsLinkId",
            "SelectTradeNumber",
            "LinkTradeId",
        ]

        # build base DF to add to
        to_send_df = pd.DataFrame(columns=eclipse_columns, index=list(range(len(rows))))

        # load generic trade requirements
        to_send_df["TradeType"], to_send_df["TradeStatus"] = "N", "N"
        to_send_df["Client"] = "ZUPE"
        to_send_df["Exchange"] = "LME"
        to_send_df["ExeBkr"] = "BGM"
        to_send_df["TradeTime"] = trade_time
        to_send_df["TradeExecutionNanoSeconds"] = now.strftime(r"%f")
        to_send_df["TradeSource"] = "TEL"
        to_send_df["CommTradeType"] = "I"
        to_send_df["OpenClose"] = "O"
        to_send_df["TrDate"] = trade_day

        # load trade ralted fields
        for i in range(len(rows)):
            # if total row then skip
            if rows[i]["Instrument"] == "Total":
                continue

            # add row specific data
            (
                to_send_df.loc[i, "ContractType"],
                to_send_df.loc[i, "Strike"],
                to_send_df.loc[i, "Contract"],
                to_send_df.loc[i, "Del"],
                to_send_df.loc[i, "ExternalInstrumentID"],
            ) = georgia_eclipse_name_convert(rows[i]["Instrument"], static)

            to_send_df.loc[i, "Price"] = rows[i]["Theo"]

            to_send_df.loc[i, "TradeReference"] = f"upe-{uuid.uuid4()}"

            # fill in buy/sell based on QTY
            if int(rows[i]["Qty"]) > 0:
                to_send_df.loc[i, "BuySell"] = "B"
                to_send_df.loc[i, "Lots"] = rows[i]["Qty"]
            elif int(rows[i]["Qty"]) < 0:
                to_send_df.loc[i, "BuySell"] = "S"
                to_send_df.loc[i, "Lots"] = int(rows[i]["Qty"]) * -1

    elif destination == "Marex":
        marex_columns = [
            "TradeDate",
            "TradeTime",
            "Metal",
            "Client",
            "BackOff",
            "Trade Type",
            "Sub Type",
            "Price Type",
            "Venue",
            "BS",
            "Lots",
            "Price",
            "Prompt",
            "Strike",
            "Premium",
            "Underlying Price",
            "Volatility",
            "Pub Ref",
            "Comm",
            "MU",
            "Session Code",
            "Trader",
        ]
        to_send_df = pd.DataFrame(columns=marex_columns, index=list(range(len(rows))))

        to_send_df["TradeDate"] = now.strftime(r"%Y%m%d")
        to_send_df["TradeTime"] = now.strftime(r"%H:%M")
        to_send_df["Client"] = "BH001"
        to_send_df["Price Type"] = "CURRENT"
        to_send_df["Venue"] = "INTER OFFICE"
        to_send_df["Comm"] = "Y"
        to_send_df["MU"] = "N"
        to_send_df["Session Code"] = ""
        to_send_df["Trader"] = ""

        for i, row in enumerate(rows):
            (
                to_send_df.loc[i, "Trade Type"],
                to_send_df.loc[i, "Strike"],
                to_send_df.loc[i, "Metal"],
                to_send_df.loc[i, "Prompt"],
            ) = georgia_marex_name_convert(row["Instrument"], static)

            if row["Counterparty"] == "BGM":
                to_send_df.loc[i, "BackOff"] = "MFL"
                to_send_df.loc[i, "Sub Type"] = "EXCEPTION NON-REPORTABLE"
                to_send_df.loc[i, "Pub Ref"] = ""
            else:
                to_send_df.loc[i, "Sub Type"] = "GIVE-UP CLEARER"
                to_send_df.loc[i, "Pub Ref"] = "BGM"
                if row["Counterparty"] is None:
                    raise sftp_utils.CounterpartyClearerNotFound(
                        "No counterparty given"
                    )
                clearer = sftp_utils.get_clearer_from_counterparty(
                    row["Counterparty"].upper()
                )
                if clearer is not None:
                    to_send_df.loc[i, "BackOff"] = clearer
                else:
                    raise sftp_utils.CounterpartyClearerNotFound(
                        f"Unable to find clearer for `{row['Counterparty']}`",
                    )

            if to_send_df.loc[i, "Trade Type"] in ["CALL", "PUT"]:
                to_send_df.loc[i, "Volatility"] = row["IV"]
                to_send_df.loc[i, "Underlying Price"] = row["Forward"]
                to_send_df.loc[i, "Premium"] = row["Theo"]
            else:
                to_send_df.loc[i, "Volatility"] = ""
                to_send_df.loc[i, "Underlying Price"] = ""
                to_send_df.loc[i, "Premium"] = ""

            to_send_df.loc[i, "Price"] = row["Theo"]
            to_send_df.loc[i, "Lots"] = abs(int(row["Qty"]))
            to_send_df.loc[i, "BS"] = "B" if int(row["Qty"]) > 0 else "S"

    elif destination == "RJOBrien":
        RJO_COLUMNS = [
            "Type",
            "Client",
            "Buy/Sell",
            "Lots",
            "Commodity",
            "Prompt",
            "Strike",
            "C/P",
            "Price",
            "Broker",
            "Clearer",
            "clearer/executor/normal",
            "Volatility",
            "Hit Account",
            "Price2",
        ]
        LME_METAL_MAP = {
            "LZH": "ZSD",
            "LAD": "AHD",
            "LCU": "CAD",
            "PBD": "PBD",
            "LND": "NID",
        }

        to_send_df = pd.DataFrame(columns=RJO_COLUMNS, index=list(range(len(rows))))

        to_send_df["Client"] = "LJ4UPLME"
        to_send_df["Broker"] = "RJO"
        to_send_df["clearer/executor/normal"] = "clearer"

        carry_link_tracker = {}
        for i, row in enumerate(rows):
            try:
                carry_link = int(row["Carry Link"])
            except TypeError:
                if row["Carry Link"] is not None:
                    raise BadCarryInput(
                        f"Bad carry link input: `{row['Carry Link']}` couldn't be parsed to an integer"
                    )
                else:
                    carry_link = 0
                    row["Carry Link"] = 0
            print(carry_link)
            instrument_split = row["Instrument"].split(" ")

            clearer = sftp_utils.get_clearer_from_counterparty(
                row["Counterparty"].upper().strip()
            )
            if clearer is not None:
                to_send_df.loc[i, "Clearer"] = clearer
            else:
                raise sftp_utils.CounterpartyClearerNotFound(
                    f"Unable to find clearer for `{row['Counterparty'].upper().strip()}`",
                )

            if clearer == "RJO":
                to_send_df.loc[i, "clearer/executor/normal"] = "normal"
                to_send_df.loc[i, "Client"] = (
                    row["Counterparty"].upper().strip()
                    + "_"
                    + to_send_df.loc[i, "Client"]
                )

            try:
                to_send_df.loc[i, "Commodity"] = LME_METAL_MAP[
                    row["Instrument"][:3].upper()
                ]
            except KeyError:
                raise KeyError(
                    f"Symbol entered incorrectly for LME mapping: `{row['Instrument'].upper()}`"
                    f" parser uses the first three characters of this to find LME symbol."
                )
            to_send_df.loc[i, "Price"] = row["Theo"]
            to_send_df.loc[i, "Buy/Sell"] = "B" if int(row["Qty"]) > 0 else "S"
            to_send_df.loc[i, "Lots"] = abs(int(row["Qty"]))

            if len(instrument_split) > 2:
                # implies option in old symbol spec
                to_send_df.loc[i, "Type"] = "OPTION"
                to_send_df.loc[i, "Strike"] = int(instrument_split[1])
                to_send_df.loc[i, "C/P"] = instrument_split[2].upper()
                to_send_df.loc[i, "Price2"] = row["Forward"]
                to_send_df.loc[i, "Volatility"] = str(round(float(row["IV"]) * 100))
                to_send_df.loc[i, "Prompt"] = datetime.strptime(
                    static.loc[
                        static["product"] == instrument_split[0], "expiry"
                    ].values[0],
                    r"%d/%m/%Y",
                ).strftime(r"%Y%m00")
                to_send_df.loc[i, "Hit Account"] = ""
            else:
                if (
                    carry_link is not None
                    and isinstance(carry_link, int)
                    and carry_link > 0
                ):
                    try:
                        carry_link_tracker[carry_link].append(int(row["Qty"]))
                    except KeyError:
                        carry_link_tracker[carry_link] = [int(row["Qty"])]
                    if len(carry_link_tracker[carry_link]) > 2:
                        raise BadCarryInput(
                            f"Carry link input incorrectly, found more than two legs for link number {carry_link}"
                        )
                    to_send_df.loc[i, "Type"] = "CARRY"
                    to_send_df.loc[i, "Hit Account"] = str(carry_link)
                else:
                    to_send_df.loc[i, "Type"] = "OUTRIGHT"
                    to_send_df.loc[i, "Hit Account"] = ""
                to_send_df.loc[i, "Prompt"] = datetime.strptime(
                    instrument_split[1], r"%Y-%m-%d"
                ).strftime(r"%Y%m%d")
                to_send_df.loc[i, "Strike"] = ""
                to_send_df.loc[i, "C/P"] = ""
                to_send_df.loc[i, "Price2"] = ""
                to_send_df.loc[i, "Volatility"] = ""

        for key, value in carry_link_tracker.items():
            if len(value) != 2 or sum(value) != 0:
                raise BadCarryInput(
                    f"Carry link input incorrectly, found `{value}` legs for `{key}`"
                )
    return to_send_df, destination, now


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
                    [dcc.Input(id="calculatorBasis", type="text", debounce=True)],
                    width=4,
                ),
                dbc.Col([dcc.Input(id="calculatorForward", type="text")], width=4),
                dbc.Col([dcc.Input(id="interestRate", type="text")], width=4),
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
                                    type="text", id="calculatorSpread", debounce=True
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
                                    id="strategy",
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
                                    id="dayConvention",
                                    value="",
                                    options=[
                                        {"label": "Bis/Bis", "value": "b/b"},
                                        {"label": "Calendar/365", "value": ""},
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
                                    id="calculatorVol_price",
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
                                    id="nowOpen",
                                    options=[
                                        {"label": "Now", "value": "now"},
                                        {"label": "Open", "value": "open"},
                                    ],
                                    value="open",
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
                            id="counterparty",
                            value="",
                            options=get_valid_counterpart_dropdown_options("xlme"),
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
                dbc.Col([dcc.Input(id="oneStrike")], width=2),
                dbc.Col([dcc.Input(id="twoStrike")], width=2),
                dbc.Col([dcc.Input(id="threeStrike")], width=2),
                dbc.Col([dcc.Input(id="fourStrike")], width=2),
                dbc.Col([dcc.Input(id="qty", type="number", value=10, min=0)], width=2),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Price/Vol: "], width=2),
                dbc.Col([dcc.Input(id="oneVol_price")], width=2),
                dbc.Col([dcc.Input(id="twoVol_price")], width=2),
                dbc.Col([dcc.Input(id="threeVol_price")], width=2),
                dbc.Col([dcc.Input(id="fourVol_price")], width=2),
                dbc.Col(
                    [dbc.Button("Buy", id="buy", n_clicks_timestamp="0", active=True)],
                    width=1,
                ),
                dbc.Col(
                    [
                        dbc.Button(
                            "Sell", id="sell", n_clicks_timestamp="0", active=True
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
                            id="oneCoP",
                            value="c",
                            options=[
                                {"label": "C", "value": "c"},
                                {"label": "P", "value": "p"},
                                {"label": "F", "value": "f"},
                            ],
                            style={"height": "50%", "verticalAlign": "middle"},
                        )
                    ],
                    width=2,
                ),
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="twoCoP",
                            value="c",
                            options=[
                                {"label": "C", "value": "c"},
                                {"label": "P", "value": "p"},
                                {"label": "F", "value": "f"},
                            ],
                            style={"height": "50%", "verticalAlign": "middle"},
                        )
                    ],
                    width=2,
                ),
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="threeCoP",
                            value="c",
                            options=[
                                {"label": "C", "value": "c"},
                                {"label": "P", "value": "p"},
                                {"label": "F", "value": "f"},
                            ],
                            style={"height": "50%", "verticalAlign": "middle"},
                        )
                    ],
                    width=2,
                ),
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="fourCoP",
                            value="c",
                            options=[
                                {"label": "C", "value": "c"},
                                {"label": "P", "value": "p"},
                                {"label": "F", "value": "f"},
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
                dbc.Col([html.Div(id="oneTheo")], width=2),
                dbc.Col([html.Div(id="twoTheo")], width=2),
                dbc.Col([html.Div(id="threeTheo")], width=2),
                dbc.Col([html.Div(id="fourTheo")], width=2),
                dbc.Col(
                    [html.Div(id="stratTheo", style={"background": stratColColor})],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["IV: "], width=2),
                dbc.Col([html.Div(id="oneIV")], width=2),
                dbc.Col([html.Div(id="twoIV")], width=2),
                dbc.Col([html.Div(id="threeIV")], width=2),
                dbc.Col([html.Div(id="fourIV")], width=2),
                dbc.Col(
                    [html.Div(id="stratIV", style={"background": stratColColor})],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Settle IV:"], width=2),
                dbc.Col([html.Div(id="oneSettleVol")], width=2),
                dbc.Col([html.Div(id="twoSettleVol")], width=2),
                dbc.Col([html.Div(id="threeSettleVol")], width=2),
                dbc.Col([html.Div(id="fourSettleVol")], width=2),
                dbc.Col(
                    [
                        html.Div(
                            id="stratSettleVol", style={"background": stratColColor}
                        )
                    ],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Delta: "], width=2),
                dbc.Col([html.Div(id="oneDelta")], width=2),
                dbc.Col([html.Div(id="twoDelta")], width=2),
                dbc.Col([html.Div(id="threeDelta")], width=2),
                dbc.Col([html.Div(id="fourDelta")], width=2),
                dbc.Col(
                    [html.Div(id="stratDelta", style={"background": stratColColor})],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(html.Div("Full Delta: ", id="fullDeltaLabel"), width=2),
                dbc.Col([html.Div(id="oneFullDelta")], width=2),
                dbc.Col([html.Div(id="twoFullDelta")], width=2),
                dbc.Col([html.Div(id="threeFullDelta")], width=2),
                dbc.Col([html.Div(id="fourFullDelta")], width=2),
                dbc.Col(
                    [
                        html.Div(
                            id="stratFullDelta", style={"background": stratColColor}
                        )
                    ],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Gamma: "], width=2),
                dbc.Col([html.Div(id="oneGamma")], width=2),
                dbc.Col([html.Div(id="twoGamma")], width=2),
                dbc.Col([html.Div(id="threeGamma")], width=2),
                dbc.Col([html.Div(id="fourGamma")], width=2),
                dbc.Col(
                    [html.Div(id="stratGamma", style={"background": stratColColor})],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Vega: "], width=2),
                dbc.Col([html.Div(id="oneVega")], width=2),
                dbc.Col([html.Div(id="twoVega")], width=2),
                dbc.Col([html.Div(id="threeVega")], width=2),
                dbc.Col([html.Div(id="fourVega")], width=2),
                dbc.Col(
                    [html.Div(id="stratVega", style={"background": stratColColor})],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Theta: "], width=2),
                dbc.Col([html.Div(id="oneTheta")], width=2),
                dbc.Col([html.Div(id="twoTheta")], width=2),
                dbc.Col([html.Div(id="threeTheta")], width=2),
                dbc.Col([html.Div(id="fourTheta")], width=2),
                dbc.Col(
                    [html.Div(id="stratTheta", style={"background": stratColColor})],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(html.Div("Vol Theta: ", id="volThetaLabel"), width=2),
                dbc.Col([html.Div(id="onevolTheta")], width=2),
                dbc.Col([html.Div(id="twovolTheta")], width=2),
                dbc.Col([html.Div(id="threevolTheta")], width=2),
                dbc.Col([html.Div(id="fourvolTheta")], width=2),
                dbc.Col(
                    [html.Div(id="stratvolTheta", style={"background": stratColColor})],
                    width=2,
                ),
            ]
        ),
    ],
    width=9,
)

hidden = (
    dcc.Store(id="tradesStore"),
    dcc.Store(id="paramsStore"),
    dcc.Store(id="productInfo"),
    html.Div(id="trades_div", style={"display": "none"}),
    html.Div(id="trade_div", style={"display": "none"}),
    html.Div(id="trade_div2", style={"display": "none"}),
    html.Div(id="productData", style={"display": "none"}),
    html.Div(id="und_name", style={"display": "none"}),
)

actions = dbc.Row(
    [
        dbc.Col([html.Button("Delete", id="delete", n_clicks_timestamp=0)], width=3),
        dbc.Col([html.Button("Trade", id="trade", n_clicks_timestamp=0)], width=3),
        dbc.Col(
            [html.Button("Client Recap", id="clientRecap", n_clicks_timestamp=0)],
            width=3,
        ),
        dbc.Col(
            [
                dcc.ConfirmDialogProvider(
                    html.Button("Report", id="report", n_clicks_timestamp=0),
                    id="report-confirm",
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
    {
        "id": "Carry Link",
        "name": "Carry Link",
        "editable": True,
    },
    {"id": "Counterparty", "name": "Counterparty", "presentation": "dropdown"},
]

tables = dbc.Col(
    dtable.DataTable(
        id="tradesTable",
        data=[{}],
        columns=columns,
        row_selectable="multi",
        editable=True,
        dropdown={
            "Counterparty": {
                "clearable": False,
                "options": get_valid_counterpart_dropdown_options("xlme"),
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
                        id="productCalc-selector",
                        # value=onLoadProductProducts()[1],
                        # options=onLoadProductProducts()[0],
                        value=productList[0]["value"],
                        options=productList,
                    )
                ],
                width=12,
            )
        ),
        dbc.Row(dbc.Col([dcc.Dropdown(id="monthCalc-selector")], width=12)),
        dbc.Row(dbc.Col(["Product:"], width=12)),
        dbc.Row(dbc.Col(["Option Expiry:"], width=12)),
        dbc.Row(dbc.Col([html.Div("expiry", id="calculatorExpiry")], width=12)),
        dbc.Row(dbc.Col(["Third Wednesday:"], width=12)),
        dbc.Row(dbc.Col([html.Div("3wed", id="3wed")])),
        dbc.Row(dbc.Col(["Multiplier:"], width=12)),
        dbc.Row(dbc.Col([html.Div("mult", id="multiplier")])),
    ],
    width=3,
)

output = dcc.Markdown(id="reponseOutput")

alert = html.Div(
    [
        dbc.Alert(
            "Trade sent",
            id="tradeSent",
            dismissable=True,
            is_open=False,
            duration=5000,
            color="success",
        ),
        dbc.Alert(
            "Trade Submission Failure",
            id="tradeSentFail",
            dismissable=True,
            is_open=False,
            duration=5000,
            color="danger",
        ),
        dbc.Alert(
            "Trade Routed",
            id="tradeRouted",
            dismissable=True,
            is_open=False,
            duration=5000,
            color="success",
        ),
        dbc.Alert(
            "Trade Routing Failed",
            id="tradeRouteFail",
            dismissable=True,
            is_open=False,
            duration=5000,
            color="danger",
        ),
        dbc.Alert(
            "Trade Routing Partially Failed",
            id="tradeRoutePartialFail",
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
    # update months options on product change - SWITCH to db call!
    @app.callback(
        Output("monthCalc-selector", "options"),
        [Input("productCalc-selector", "value")],
    )
    def updateOptions(product):
        if product:
            # return onLoadProductMonths(product)[0]
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

    # update months value on product change  - PROBS DONE!!!!
    @app.callback(
        Output("monthCalc-selector", "value"), [Input("monthCalc-selector", "options")]
    )
    def updatevalue(options):
        if options:
            return options[0]["value"]

    # update static data on product/month change   DONE!
    @app.callback(
        Output("multiplier", "children"),
        Output("und_name", "children"),
        Output("3wed", "children"),
        Output("calculatorExpiry", "children"),
        Output("interestRate", "placeholder"),
        [Input("monthCalc-selector", "value")],
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

    # populate table on trade deltas change - DONE!
    @app.callback(Output("tradesTable", "data"), [Input("tradesStore", "data")])
    def loadTradeTable(data):
        if data != None:
            trades = buildTradesTableData(data)
            return trades.to_dict("records")

        else:
            return [{}]

    # change talbe data on buy/sell delete - DONE!
    @app.callback(
        [Output("tradesStore", "data"), Output("tradesTable", "selected_rows")],
        [
            Input("buy", "n_clicks_timestamp"),
            Input("sell", "n_clicks_timestamp"),
            Input("delete", "n_clicks_timestamp"),
        ],
        # standard trade inputs
        [
            State("tradesTable", "selected_rows"),
            State("tradesTable", "data"),
            State("calculatorVol_price", "value"),
            State("tradesStore", "data"),
            State("counterparty", "value"),
            State("3wed", "children"),
            # State('trades_div' , 'children'),
            State("productCalc-selector", "value"),
            State("monthCalc-selector", "value"),
            State("qty", "value"),
            State("strategy", "value"),
            # trade value inputs
            # one vlaues
            State("oneStrike", "value"),
            State("oneStrike", "placeholder"),
            State("oneCoP", "value"),
            State("oneTheo", "children"),
            State("oneIV", "children"),
            State("oneDelta", "children"),
            State("oneGamma", "children"),
            State("oneVega", "children"),
            State("oneTheta", "children"),
            # two values
            State("twoStrike", "value"),
            State("twoStrike", "placeholder"),
            State("twoCoP", "value"),
            State("twoTheo", "children"),
            State("twoIV", "children"),
            State("twoDelta", "children"),
            State("twoGamma", "children"),
            State("twoVega", "children"),
            State("twoTheta", "children"),
            # three values
            State("threeStrike", "value"),
            State("threeStrike", "placeholder"),
            State("threeCoP", "value"),
            State("threeTheo", "children"),
            State("threeIV", "children"),
            State("threeDelta", "children"),
            State("threeGamma", "children"),
            State("threeVega", "children"),
            State("threeTheta", "children"),
            # four values
            State("fourStrike", "value"),
            State("fourStrike", "placeholder"),
            State("fourCoP", "value"),
            State("fourTheo", "children"),
            State("fourIV", "children"),
            State("fourDelta", "children"),
            State("fourGamma", "children"),
            State("fourVega", "children"),
            State("fourTheta", "children"),
            State("calculatorExpiry", "children"),
            State("3wed", "children"),
            State("calculatorForward", "value"),
            State("calculatorForward", "placeholder"),
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
        expiry,
        wed,
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

        # build product from month and product dropdown
        if product and month:
            product = product + "O" + month

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
                futureName = str(product)[:3] + " " + str(prompt)

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
                                "carry link": None,
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
                                str(product)
                                + " "
                                + str(onestrike)
                                + " "
                                + str(onecop).upper()
                            )
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
                                "carry link": None,
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
                                "carry link": None,
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
                                str(product)
                                + " "
                                + str(twostrike)
                                + " "
                                + str(twocop).upper()
                            )
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
                                "carry link": None,
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
                                "carry link": None,
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
                                str(product)
                                + " "
                                + str(threestrike)
                                + " "
                                + str(threecop).upper()
                            )
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
                                "carry link": None,
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
                                "carry link": None,
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
                                str(product)
                                + " "
                                + str(fourstrike)
                                + " "
                                + str(fourcop).upper()
                            )
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
                                "carry link": None,
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
                            "carry link": None,
                            "counterparty": counterparty,
                        }
                        if futureName in trades:
                            trades[futureName]["qty"] = (
                                trades[futureName]["qty"] + hedge["qty"]
                            )
                        else:
                            trades[futureName] = hedge
            return trades, clickdata

    # delete all input values on product changes - DONE!
    @app.callback(
        [
            Output("oneStrike", "value"),
            Output("oneVol_price", "value"),
            Output("twoStrike", "value"),
            Output("twoVol_price", "value"),
            Output("threeStrike", "value"),
            Output("threeVol_price", "value"),
            Output("fourStrike", "value"),
            Output("fourVol_price", "value"),
        ],
        [Input("productCalc-selector", "value"), Input("monthCalc-selector", "value")],
    )
    def clearSelectedRows(product, month):
        return "", "", "", "", "", "", "", ""

    # send trade to system - DONE!
    @app.callback(
        Output("tradeSent", "is_open"),
        Output("tradeSentFail", "is_open"),
        [Input("trade", "n_clicks")],
        [State("tradesTable", "selected_rows"), State("tradesTable", "data")],
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
            packaged_trades_to_send_legacy = []
            packaged_trades_to_send_new = []
            trader_id = 0
            upsert_pos_params = []
            trade_time_ns = time.time_ns()
            booking_dt = datetime.utcnow()
            processed_user = user.replace(" ", "").split("@")[0]

            with shared_engine.connect() as pg_db2_connection:
                stmt = sqlalchemy.text(
                    "SELECT trader_id FROM traders WHERE email = :user_email"
                )
                result = pg_db2_connection.execute(
                    stmt, {"user_email": user.lower()}
                ).scalar_one_or_none()
                if result is None:
                    trader_id = -101
                else:
                    trader_id = result

            for i in indices:
                # build new instrument name for mew trades table
                # new_instrument_name = build_new_lme_symbol_from_old(
                #     rows[i]["Instrument"]
                # )
                # if new_instrument_name == "error":
                #     return False, True

                # create st to record which products to update in redis
                redisUpdate = set([])
                # check that this is not the total line.
                if rows[i]["Instrument"] != "Total":
                    if rows[i]["Instrument"][3] == "O":
                        # is option
                        product = rows[i]["Instrument"][:6]
                        instrument = rows[i]["Instrument"]
                        redisUpdate.add(product)
                        productName = (rows[i]["Instrument"]).split(" ")
                        strike = productName[1]
                        CoP = productName[2]

                        prompt = rows[i]["Prompt"]
                        theo = rows[i]["Theo"]
                        price = rows[i]["Theo"]
                        qty = rows[i]["Qty"]
                        counterparty = rows[i]["Counterparty"]

                        georgia_trade_id = (
                            f"calclme.{processed_user}.{trade_time_ns}:{i}"
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
                                Comment="LME CALC",
                                prompt=prompt,
                                venue="Georgia",
                                deleted=0,
                                venue_trade_id=georgia_trade_id,
                            )
                        )
                        packaged_trades_to_send_new.append(
                            sql_utils.TradesTable(
                                trade_datetime_utc=booking_dt,
                                instrument_symbol=instrument,  # new_instrument_name,
                                quantity=qty,
                                price=price,
                                portfolio_id=1,  # lme general = 1
                                trader_id=trader_id,
                                notes="LME CALC",
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

                    elif rows[i]["Instrument"][3] == " ":
                        # is futures
                        product = rows[i]["Instrument"][:3]
                        instrument = rows[i]["Instrument"]
                        redisUpdate.add(product)
                        prompt = rows[i]["Prompt"]
                        price = rows[i]["Theo"]
                        qty = rows[i]["Qty"]
                        counterparty = rows[i]["Counterparty"]

                        georgia_trade_id = (
                            f"calclme.{processed_user}.{trade_time_ns}:{i}"
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
                                Comment="LME CALC",
                                prompt=prompt,
                                venue="Georgia",
                                deleted=0,
                                venue_trade_id=georgia_trade_id,
                            )
                        )
                        packaged_trades_to_send_new.append(
                            sql_utils.TradesTable(
                                trade_datetime_utc=booking_dt,
                                instrument_symbol=instrument,  # new_instrument_name,
                                quantity=qty,
                                price=price,
                                portfolio_id=1,  # lme general id = 1
                                trader_id=trader_id,
                                notes="LME CALC",
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

            # options and futures built, sending trades
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
                with sqlalchemy.orm.Session(shared_engine) as session:
                    session.add_all(packaged_trades_to_send_new)
                    session.commit()
                return False, True

            # send trades to redis
            # try:
            #     with legacyEngine.connect() as pg_connection:
            #         trades = pd.read_sql("trades", pg_connection)
            #         positions = pd.read_sql("positions", pg_connection)

            #     trades.columns = trades.columns.str.lower()
            #     positions.columns = positions.columns.str.lower()

            #     # pipeline = conn.pipeline()
            #     # pipeline.set("trades" + dev_key_redis_append, pickle.dumps(trades))
            #     # pipeline.set(
            #     #     "positions" + dev_key_redis_append, pickle.dumps(positions)
            #     # )
            #     # pipeline.execute()
            # except Exception:
            #     print("Exception encountered while trying to update redis trades/posi")
            #     print(traceback.format_exc())
            #     return False, True

            return True, False

    # send trade to SFTP - DONE!
    @app.callback(
        [
            Output("reponseOutput", "children"),
            Output("tradeRouted", "is_open"),
            Output("tradeRouteFail", "is_open"),
            Output("tradeRoutePartialFail", "is_open"),
        ],
        [
            Input("report-confirm", "submit_n_clicks_timestamp"),
            Input("clientRecap", "n_clicks_timestamp"),
        ],
        [State("tradesTable", "selected_rows"), State("tradesTable", "data")],
    )
    def sendTrades(report, recap, indices, rows):
        # string to hold router respose
        tradeResponse = "## Response"
        if (int(report) + int(recap)) == 0:
            raise PreventUpdate

        # enact trade recap logic
        if int(recap) > int(report):
            response = "Recap: \r\n"
            if indices:
                for i in indices:
                    if rows[i]["Instrument"][3] == "O":
                        # is option
                        instrument = rows[i]["Instrument"].split()
                        product = codeToName(instrument[0])
                        strike = instrument[1]
                        CoP = instrument[2]
                        month = codeToMonth(instrument[0])

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
                            product,
                            strike,
                            CoP,
                            price,
                            round(vol, 2),
                        )
                    elif rows[i]["Instrument"][3] == " ":
                        # is futures
                        instrument = rows[i]["Instrument"].split()
                        date = datetime.strptime(instrument[1], "%Y-%m-%d")
                        month = date.strftime("%b")[:3]
                        product = rows[i]["Instrument"][:3]
                        prompt = rows[i]["Prompt"]
                        price = rows[i]["Theo"]
                        qty = rows[i]["Qty"]
                        if qty > 0:
                            bs = "Sell"
                        elif qty < 0:
                            bs = "Buy"

                        response += "You {} {} {} {} at {} \r\n".format(
                            bs, abs(int(qty)), month, product, price
                        )

                return response, False, False, False
            else:
                return "No rows selected", False, False, False

        # pull username from site header
        user = request.headers.get("X-MS-CLIENT-PRINCIPAL-NAME")
        if user is None or not user:
            user = "Test"
        if int(recap) < int(report):
            if indices:
                # print(rows)
                del_index = None
                rows_to_send = []
                for i in indices:
                    if rows[i]["Instrument"] != "Total":
                        rows_to_send.append(rows[i])
                # build csv in buffer from rows
                routing_trade = sftp_utils.add_routing_trade(
                    datetime.utcnow(),
                    user,
                    "PENDING",
                    "failed to build formatted trade",
                )
                try:
                    (
                        dataframe_rjob,
                        destination_rjob,
                        now_rjob,
                    ) = build_trade_for_report(rows_to_send, destination="RJOBrien")
                except sftp_utils.CounterpartyClearerNotFound as e:
                    routing_trade = sftp_utils.update_routing_trade(
                        routing_trade,
                        "FAILED",
                        error=f"Failed to find clearer for the given counterparty `{e.counterparty}`",
                    )
                    return (
                        "Failed to find clearer for the given counterparty",
                        False,
                        True,
                        False,
                    )
                except Exception:
                    formatted_traceback = traceback.format_exc()
                    routing_trade = sftp_utils.update_routing_trade(
                        routing_trade,
                        "FAILED",
                        error=formatted_traceback,
                    )
                    return formatted_traceback, False, True, False

                routing_trade = sftp_utils.update_routing_trade(
                    routing_trade,
                    "PENDING",
                    now_rjob,
                    rows_to_send[0]["Counterparty"],
                )
                # created file and message title based on current datetime
                now = now_rjob
                title = "LJ4UPLME_{}".format(now.strftime(r"%Y%m%d_%H%M%S%f"))
                att_name = "{}.csv".format(title)

                temp_file_sftp = tempfile.NamedTemporaryFile(
                    mode="w+b", dir="./", prefix=f"{title}_", suffix=".csv"
                )
                dataframe_rjob.to_csv(temp_file_sftp, mode="b", index=False)

                try:
                    sftp_utils.submit_to_stfp(
                        "/Allocations",
                        att_name,
                        temp_file_sftp.name,
                    )
                except Exception:
                    temp_file_sftp.close()
                    formatted_traceback = traceback.format_exc()
                    routing_trade = sftp_utils.update_routing_trade(
                        routing_trade,
                        "FAILED",
                        error=formatted_traceback,
                    )
                    return formatted_traceback, False, True, False

                tradeResponse = ""
                routing_trade = sftp_utils.update_routing_trade(
                    routing_trade, "ROUTED", error=None
                )

                temp_file_sftp.close()
                return tradeResponse, True, False, False

    @app.callback(
        Output("calculatorPrice/Vola", "value"),
        [Input("productCalc-selector", "value"), Input("monthCalc-selector", "value")],
    )
    def loadBasis(product, month):
        return ""

    # update product info on product change
    @app.callback(
        Output("productInfo", "data"),
        [
            Input("productCalc-selector", "value"),
            Input("monthCalc-selector", "value"),
            # Input("monthCalc-selector", "options"),
        ],
    )
    def updateProduct(product, month):
        ic(month, product)
        # if month and product:
        #     product = product + "O" + month
        #     params = loadRedisData(product.lower())
        #     # params = params.decode("utf-8")
        #     params = json.loads(params)

        # first test of new option engine output!! looks good !
        # op_eng_test = conn.get("xlme-lad-usd o 24-02-07 a:dev").decode("utf-8")  #
        # print(orjson.loads(op_eng_test))
        if month and product:
            params = loadRedisData(month.lower() + dev_key_redis_append)
            params = orjson.loads(params)

            return params

    legOptions = ["one", "two", "three", "four"]

    # create vola function
    def buildUpdateVola(leg):
        def updateVola(params, strike, pStrike, cop, priceVol, pforward, forward):
            # user input or placeholder

            if not forward:
                forward = pforward

            if cop == "f":
                return forward, None
            else:
                # get strike from strike vs pstrikesettle_model
                if not strike:
                    strike = pStrike
                strike = int(strike)
                if strike:
                    if params:
                        params = pd.DataFrame.from_dict(params, orient="index")

                        # if strike is real strike
                        if strike in params["strike"].values:
                            if priceVol == "vol":
                                vol = round(
                                    params.loc[
                                        (
                                            (params["strike"] == strike)
                                            & (params["cop"] == "c")
                                        )
                                    ]["vol"][0]
                                    * 100,
                                    2,
                                )
                                settle = calc_lme_vol(
                                    params, float(forward), float(strike)
                                )
                                ic(vol, round(settle * 100, 2))
                                return vol, round(settle * 100, 2)

                            elif priceVol == "price":
                                price = round(
                                    params.loc[
                                        (
                                            (params["strike"] == strike)
                                            & (params["cop"] == "c")
                                        )
                                    ]["calc_price"][0],
                                    2,
                                )
                                settle = calc_lme_vol(
                                    params, float(forward), float(strike)
                                )
                                return price, settle * 100

                else:
                    return 0, 0

        return updateVola

    def buildVoltheta():
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

    # calc forward from basis and spread - DONE
    @app.callback(
        Output("calculatorForward", "placeholder"),
        [
            Input("calculatorBasis", "value"),
            Input("calculatorBasis", "placeholder"),
            Input("calculatorSpread", "value"),
            Input("calculatorSpread", "placeholder"),
        ],
    )
    def forward_update(basis, basisp, spread, spreadp):
        if not basis:
            basis = basisp
        if not spread:
            spread = spreadp

        return float(basis) + float(spread)

    @app.callback(
        Output("calculatorForward", "value"),
        [
            Input("productInfo", "data"),
        ],
    )
    def forward_update(productInfo):
        return ""

    # create placeholder function for each {leg}Strike
    for leg in legOptions:
        # clientside black scholes
        app.clientside_callback(
            ClientsideFunction(namespace="clientside", function_name="blackScholes"),
            [
                Output("{}{}".format(leg, i), "children")
                for i in ["Theo", "Delta", "Gamma", "Vega", "Theta", "IV"]
            ],
            [Input("calculatorVol_price", "value")]
            + [
                Input("{}{}".format(leg, i), "value")
                for i in ["CoP", "Strike", "Vol_price"]
            ]
            + [
                Input("{}{}".format(leg, i), "placeholder")
                for i in ["Strike", "Vol_price"]
            ]
            + [
                Input("{}".format(i), "value")
                for i in ["calculatorForward", "interestRate"]
            ]
            + [
                Input("{}".format(i), "placeholder")
                for i in ["calculatorForward", "interestRate"]
            ]
            + [Input("calculatorExpiry", "children")],
        )

        # update vol_price placeholder
        app.callback(
            [
                Output("{}Vol_price".format(leg), "placeholder"),
                Output("{}SettleVol".format(leg), "children"),
            ],
            [
                Input("productInfo", "data"),
                Input("{}Strike".format(leg), "value"),
                Input("{}Strike".format(leg), "placeholder"),
                Input("{}CoP".format(leg), "value"),
                Input("calculatorVol_price", "value"),  # radio button
                Input("calculatorForward", "placeholder"),
                Input("calculatorForward", "value"),
            ],
        )(buildUpdateVola(leg))

        # calculate the vol thata from vega and theta - DONE
        app.callback(
            Output("{}volTheta".format(leg), "children"),
            [
                Input("{}Vega".format(leg), "children"),
                Input("{}Theta".format(leg), "children"),
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
        "FullDelta",
        "Delta",
        "Gamma",
        "Vega",
        "Theta",
        "IV",
        "SettleVol",
        "volTheta",
    ]:
        app.callback(
            Output("strat{}".format(param), "children"),
            [
                Input("strategy", "value"),
                Input("one{}".format(param), "children"),
                Input("two{}".format(param), "children"),
                Input("three{}".format(param), "children"),
                Input("four{}".format(param), "children"),
                Input("qty", "value"),
                Input("multiplier", "children"),
            ],
        )(buildStratGreeks(param))

    inputs = ["calculatorBasis", "calculatorSpread"]

    @app.callback(
        [Output("{}".format(i), "placeholder") for i in inputs]
        + [Output("{}".format(i), "value") for i in inputs]
        + [Output("{}Strike".format(i), "placeholder") for i in legOptions],
        [
            Input("productCalc-selector", "value"),
            Input("monthCalc-selector", "value"),
            Input("productInfo", "data"),
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
        # if params:
        #     params = pd.DataFrame.from_dict(params, orient="index")

        #     atm = float(params.iloc[0]["und_calc_price"])

        #     params = params.iloc[(params["strike"] - atm).abs().argsort()[:2]]

        #     valuesList = [""] * len(inputs)
        #     atmList = [params.iloc[0]["strike"]] * len(legOptions)
        #     expiry = date.fromtimestamp(params.iloc[0]["expiry"] / 1e9)
        #     third_wed = date.fromtimestamp(params.iloc[0]["third_wed"] / 1e9)
        #     mult = params.iloc[0]["multiplier"]
        #     inr = round((params.iloc[0]["interest_rate"] * 100), 5)
        #     spread = round(params.iloc[0]["spread"], 5)
        #     basis = atm - params.iloc[0]["spread"]
        #     return (
        #         [
        #             inr,
        #             basis,
        #             spread,
        #         ]
        #         + valuesList
        #         + [expiry, third_wed, mult]
        #         + atmList
        #     )

        # else:
        #     atmList = [no_update] * len(legOptions)
        #     valuesList = [no_update] * len(inputs)
        #     return (
        #         [no_update for _ in len(inputs)]
        #         + valuesList
        #         + [no_update, no_update]
        #         + atmList
        #     )
