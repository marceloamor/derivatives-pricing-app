from dash.dependencies import Input, Output, State, ClientsideFunction
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash import no_update
from datetime import datetime, date, timedelta
from dash import dash_table as dtable
import pandas as pd
import datetime as dt
import time, os, json, io
import uuid
import pytz
from dash.exceptions import PreventUpdate
from flask import request
import traceback
import tempfile

from TradeClass import TradeClass, Option
from sql import sendTrade, pullCodeNames, updatePos
from parts import (
    loadStaticData,
    send_email,
    topMenu,
    calc_lme_vol,
    onLoadProductProducts,
    sendPosQueueUpdate,
    sendPosQueueUpdateEU,
    loadRedisData,
    pullCurrent3m,
    buildTradesTableData,
    retriveParams,
    updateRedisDeltaEU,
    updateRedisPos,
    updateRedisTrade,
    loadVolaData,
    buildSurfaceParams,
    codeToName,
    codeToMonth,
    onLoadProductMonths,
)
import sftp_utils as sftp_utils
import email_utils as email_utils
from data_connections import Session
from sqlalchemy import select
import upestatic

USE_DEV_KEYS = os.getenv("USE_DEV_KEYS", "false").lower() in [
    "true",
    "t",
    "1",
    "y",
    "yes",
]

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
    with Session() as session:
        products = (
            session.query(upestatic.Product)
            .where(upestatic.Product.exchange_symbol == "xext")
            .all()
        )
        return products


productList = [
    {"label": product.long_name.title(), "value": product.symbol}
    for product in loadProducts()
]


def loadOptions(optionSymbol):
    with Session() as session:
        product = (
            session.query(upestatic.Product)
            .where(upestatic.Product.symbol == optionSymbol)
            .first()
        )
        optionsList = (option for option in product.options)
        return optionsList


def getOptionInfo(optionSymbol):
    with Session() as session:
        option = (
            session.query(upestatic.Option)
            .where(upestatic.Option.symbol == optionSymbol)
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
    with Session() as session:
        try:
            most_recent_date = (
                session.query(upestatic.SettlementVol)
                .where(upestatic.SettlementVol.option_symbol == optionSymbol)
                .order_by(upestatic.SettlementVol.settlement_date.desc())
                .first()
                .settlement_date
            )
            settle_vols = (
                session.query(upestatic.SettlementVol)
                .where(upestatic.SettlementVol.option_symbol == optionSymbol)
                .where(upestatic.SettlementVol.settlement_date == most_recent_date)
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


class OptionDataNotFoundError(Exception):
    pass


class BadCarryInput(Exception):
    pass


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


def convertTimestampToSQLDateTime(value):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(value))


def convertToSQLDate(date):
    value = date.strftime(f)
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(value))


def buildProductName(product, strike, Cop):
    if strike == None and Cop == None:
        return product
    else:
        return product + " " + str(strike) + " " + Cop


def buildCounterparties():
    # load couterparties from DB
    try:
        df = pullCodeNames()
        nestedOptions = df["codename"].values
        options = [{"label": opt, "value": opt} for opt in nestedOptions]
        options.append({"label": "ERROR", "value": "ERROR"})
    except Exception as e:
        print("failed to load codenames")
        print(e)
        options = [{"label": "ERROR", "value": "ERROR"}]

    return options


def excelNameConversion(name):
    if name == "cu":
        return "LCUO"
    elif name == "zn":
        return "LZHO"
    elif name == "ni":
        return "LNDO"
    elif name == "pb":
        return "PBDO"
    elif name == "al":
        return "LADO"


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
                        "No counterparty given", counterparty="NONE GIVEN"
                    )
                clearer = sftp_utils.get_clearer_from_counterparty(
                    row["Counterparty"].upper()
                )
                if clearer is not None:
                    to_send_df.loc[i, "BackOff"] = clearer
                else:
                    raise sftp_utils.CounterpartyClearerNotFound(
                        f"Unable to find clearer for `{row['Counterparty']}`",
                        counterparty=row["Counterparty"],
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
        LME_METAL_MAP = {"LZH": "ZSD", "LAD": "AHD", "LCU": "CAD", "PBD": "PBD"}

        to_send_df = pd.DataFrame(columns=RJO_COLUMNS, index=list(range(len(rows))))

        to_send_df["Client"] = "LJ4UPETD"
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
                    counterparty=row["Counterparty"].upper().strip(),
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
]

stratConverstion = {
    "outright": [1, 0, 0, 0],
    "spread": [1, -1, 0, 0],
    "straddle": [1, 1, 0, 0],
    "fly": [1, -2, 1, 0],
    "condor": [1, -1, -1, 1],
    "ladder": [1, -1, -1, 0],
    "ratio": [1, -2, 0, 0],
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
                            options=buildCounterparties(),
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
        dbc.Row(
            [
                dbc.Col(html.Div("Full Delta: ", id="fullDeltaLabel-EU"), width=2),
                dbc.Col([html.Div(id="oneFullDelta-EU")], width=2),
                dbc.Col([html.Div(id="twoFullDelta-EU")], width=2),
                dbc.Col([html.Div(id="threeFullDelta-EU")], width=2),
                dbc.Col([html.Div(id="fourFullDelta-EU")], width=2),
                dbc.Col(
                    [
                        html.Div(
                            id="stratFullDelta-EU", style={"background": stratColColor}
                        )
                    ],
                    width=2,
                ),
            ]
        ),
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
    {"id": "Instrument", "name": "Instrument"},
    {
        "id": "Qty",
        "name": "Qty",
    },
    {
        "id": "Theo",
        "name": "Theo",
    },
    {"id": "Prompt", "name": "Prompt"},
    {"id": "Forward", "name": "Forward"},
    {"id": "IV", "name": "IV"},
    {"id": "Delta", "name": "Delta"},
    {"id": "Gamma", "name": "Gamma"},
    {"id": "Vega", "name": "Vega"},
    {"id": "Theta", "name": "Theta"},
    {"id": "Carry Link", "name": "Carry Link"},
    {"id": "Counterparty", "name": "Counterparty"},
]

tables = dbc.Col(
    dtable.DataTable(
        id="tradesTable-EU",
        data=[{}],
        columns=columns,
        row_selectable="multi",
        editable=True,
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
                        # value=onLoadProductProducts()[1],
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
    # load product on product/month change
    # @app.callback(
    #     Output("productData-EU", "children"), [Input("productCalc-selector-EU", "value")]
    # )
    # def updateSpread1(product):

    #     params = retriveParams(product.lower())
    #     if params:
    #         spread = params["spread"]
    #         return spread

    # load vola params for us fulldelta calc later
    # @app.callback(
    #     Output("paramsStore-EU", "data"),
    #     [
    #         Input("productCalc-selector-EU", "value"),
    #         Input("monthCalc-selector-EU", "value"),
    #         Input("calculatorForward-EU", "value"),
    #         Input("calculatorForward-EU", "placeholder"),
    #         Input("calculatorExpiry-EU", "children"),
    #     ],
    # )
    # def updateSpread1(product, month, spot, spotP, expiry):
    #     # build product from month and product
    #     if product and month:
    #         if month != "3M":
    #             product = product + "O" + month
    #             params = loadVolaData(product.lower())
    #             if params:
    #                 return params

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

    # update months value on product change   DONE!
    @app.callback(
        Output("monthCalc-selector-EU", "value"),
        [Input("monthCalc-selector-EU", "options")],
    )
    def updatevalue(options):
        if options:
            return options[0]["value"]

    # update months value on product change   DONE!
    @app.callback(
        Output("multiplier-EU", "children"),
        Output("und_name-EU", "children"),
        Output("3wed-EU", "children"),
        Output("calculatorExpiry-EU", "children"),
        [Input("monthCalc-selector-EU", "value")],
    )
    def updateOptionInfo(optionSymbol):
        if optionSymbol:
            (expiry, und_name, und_expiry, mult) = getOptionInfo(optionSymbol)
            return mult, und_name, und_expiry, expiry

    # update settlement vols store on product change
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

    # update business days to expiry (used for daysConvention)
    @app.callback(
        Output("holsToExpiry-EU", "children"),
        [Input("calculatorExpiry-EU", "children")],
        [State("monthCalc-selector-EU", "value")],
        [State("productCalc-selector-EU", "value")],
    )
    def updateBis(expiry, month, product):
        if month and product:
            with Session() as session:
                product = (
                    session.query(upestatic.Product)
                    .where(upestatic.Product.symbol == product)
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

    # change the CoP dropdown options depning on if £m or not
    @app.callback(  # NO CHANGE NEEDED!?
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
        if month == "3M":
            options = [{"label": "F", "value": "f"}]
            return options, options, options, options, "f", "f", "f", "f"
        else:
            options = [
                {"label": "C", "value": "c"},
                {"label": "P", "value": "p"},
                {"label": "F", "value": "f"},
            ]
            return options, options, options, options, "c", "c", "c", "c"

    # populate table on trade deltas change   DONE

    @app.callback(Output("tradesTable-EU", "data"), [Input("tradesStore-EU", "data")])
    def loadTradeTable(data):
        if data != None:
            trades = buildTradesTableData(data)
            return trades.to_dict("records")

        else:
            return [{}]

    # change talbe data on buy/sell delete NEED CHANGING -LATER-
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
        counterparty = "none"
        carry_link = "none"
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

    # send trade to system  DONE PROBS
    @app.callback(
        Output("tradeSent-EU", "is_open"),
        [Input("trade-EU", "n_clicks")],
        [State("tradesTable-EU", "selected_rows"), State("tradesTable-EU", "data")],
    )
    def sendTrades(clicks, indices, rows):
        timestamp = timeStamp()
        # pull username from site header
        user = request.headers.get("X-MS-CLIENT-PRINCIPAL-NAME")
        if not user:
            user = "Test"

        if indices:
            for i in indices:
                # create st to record which products to update in redis
                redisUpdate = set([])
                # check that this is not the total line.
                if rows[i]["Instrument"] != "Total":
                    if rows[i]["Instrument"][-1] in ["C", "P"]:  # done
                        # is option in format: "XEXT-EBM-EUR O 23-04-17 A-254-C"
                        product = " ".join(rows[i]["Instrument"].split(" ")[:3])
                        product = (
                            product + " " + rows[i]["Instrument"].split(" ")[-1][0]
                        )

                        info = rows[i]["Instrument"].split(" ")[3]
                        strike, CoP = info.split("-")[1:3]

                        redisUpdate.add(product)

                        prompt = rows[i]["Prompt"]
                        price = rows[i]["Theo"]
                        qty = rows[i]["Qty"]
                        counterparty = rows[i]["Counterparty"]

                        trade = TradeClass(
                            0,
                            timestamp,
                            product,
                            strike,
                            CoP,
                            prompt,
                            price,
                            qty,
                            counterparty,
                            "",
                            user,
                            "Georgia",
                            "EURONEXT",
                        )
                        # send trade to DB and record ID returened
                        trade.id = sendTrade(trade)
                        updatePos(trade)

                    elif rows[i]["Instrument"].split(" ")[1] == "F":  # done
                        # is futures in format: "XEXT-EBM-EUR F 23-05-10"
                        product = rows[i]["Instrument"]
                        redisUpdate.add(product)
                        prompt = rows[i]["Prompt"]
                        price = rows[i]["Theo"]
                        qty = rows[i]["Qty"]
                        counterparty = rows[i]["Counterparty"]

                        trade = TradeClass(
                            0,
                            timestamp,
                            product,
                            None,
                            None,
                            prompt,
                            price,
                            qty,
                            counterparty,
                            "",
                            user,
                            "Georgia",
                            "EURONEXT",
                        )
                        # send trade to DB and record ID returened
                        trade.id = sendTrade(trade)  # stay the same
                        updatePos(trade)  # stay the same

                    # # update redis for each product requirng it
                    for update in redisUpdate:
                        updateRedisDeltaEU(update)  # done
                        updateRedisPos(update)  # same
                        updateRedisTrade(update)  # no change needed
                        sendPosQueueUpdateEU(update)  # done
            return True

    # # send trade to SFTP TO DO LATER
    # @app.callback(
    #     [
    #         #Output("reponseOutput-EU", "children"),
    #         Output("tradeRouted-EU", "is_open"),
    #         Output("tradeRouteFail-EU", "is_open"),
    #         Output("tradeRoutePartialFail-EU", "is_open"),
    #     ],
    #     [
    #         Input("report-confirm-EU", "submit_n_clicks_timestamp"),
    #         #Input("clientRecap-EU", "n_clicks_timestamp"),
    #     ],
    #     [State("tradesTable-EU", "selected_rows"), State("tradesTable-EU", "data")],
    # )
    # def sendTrades(report, indices, rows):

    #     if int(report) == 0:
    #         raise PreventUpdate

    #     # pull username from site header
    #     user = request.headers.get("X-MS-CLIENT-PRINCIPAL-NAME")
    #     if user is None or not user:
    #         user = "Test"
    #     #destination_folder = "Seals"
    #     if report:
    #         if indices:
    #             print(rows)
    #             del_index = None
    #             rows_to_send = []
    #             for i in indices:
    #                 if rows[i]["Instrument"] != "Total":
    #                     rows_to_send.append(rows[i])
    #             # build csv in buffer from rows
    #             print(rows_to_send)
    #             routing_trade = sftp_utils.add_routing_trade(
    #                 datetime.utcnow(),
    #                 user,
    #                 "PENDING",
    #                 "failed to build formatted trade",
    #             )
    #             try:
    #                 (
    #                     dataframe_rjob,
    #                     destination_rjob,
    #                     now_rjob,
    #                 ) = build_trade_for_report(rows_to_send, destination="RJOBrien")
    #                 #     dataframe_eclipse,
    #                 #     destination_eclipse,
    #                 #     now_eclipse,
    #                 # ) = build_trade_for_report(rows_to_send)
    #                 # # (
    #                 # #     dataframe_seals,
    #                 # #     destination_seals,
    #                 # #     now_eclipse,
    #                 # # ) = build_trade_for_report(rows_to_send, destination="Seals")
    #                 # (
    #                 #     dataframe_marex,
    #                 #     destination_marex,
    #                 #     now_marex,
    #                 # ) = build_trade_for_report(rows_to_send, destination="Marex")
    #             except sftp_utils.CounterpartyClearerNotFound as e:
    #                 routing_trade = sftp_utils.update_routing_trade(
    #                     routing_trade,
    #                     "FAILED",
    #                     error=f"Failed to find clearer for the given counterparty `{e.counterparty}`",
    #                 )
    #                 return (
    #                     #"Failed to find clearer for the given counterparty",
    #                     False,
    #                     True,
    #                     False,
    #                 )
    #             except Exception as e:
    #                 formatted_traceback = traceback.format_exc()
    #                 routing_trade = sftp_utils.update_routing_trade(
    #                     routing_trade,
    #                     "FAILED",
    #                     error=formatted_traceback,
    #                 )
    #                 return False, True, False

    #             routing_trade = sftp_utils.update_routing_trade(
    #                 routing_trade,
    #                 "PENDING",
    #                 now_rjob,
    #                 rows_to_send[0]["Counterparty"],
    #             )

    #             # created file and message title based on current datetime
    #             now = now_rjob
    #             title = "LJ4UPLME_{}".format(now.strftime(r"%Y%m%d_%H%M%S%f"))
    #             att_name = "{}.csv".format(title)
    #             temp_file_sftp = tempfile.NamedTemporaryFile(
    #             mode="w+b", dir="./", prefix=f"{title}_", suffix=".csv"
    #             )
    #             # lmeinput.gm@britannia.com; lmeclearing@upetrading.com
    #             # send email with file attached
    #             dataframe_rjob.to_csv(temp_file_sftp, mode="b", index=False)

    #             try:
    #                 sftp_utils.submit_to_stfp(
    #                     "/Allocations",
    #                     att_name,
    #                     temp_file_sftp.name,
    #                 )
    #             except Exception as e:
    #                 temp_file_sftp.close()
    #                 formatted_traceback = traceback.format_exc()
    #                 routing_trade = sftp_utils.update_routing_trade(
    #                     routing_trade,
    #                     "FAILED",
    #                     error=formatted_traceback,
    #                 )
    #                 return False, True, False

    #             tradeResponse = ""
    #             routing_trade = sftp_utils.update_routing_trade(
    #                 routing_trade, "ROUTED", error=None
    #             )

    #             temp_file_sftp.close()
    #             return True, False, False

    # move recap button to its own dedicated callback away from Report
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

    def responseParser(response):
        return "Status: {} Error: {}".format(
            response["Status"], response["ErrorMessage"]
        )

    # not entirely sure but DONE!! anyway
    @app.callback(
        Output("calculatorPrice/Vola-EU", "value"),
        [
            Input("productCalc-selector-EU", "value"),
            Input("monthCalc-selector-EU", "value"),
        ],
    )
    def loadBasis(product, month):
        return ""

    # update product info on product change   DONE for :dev keys
    @app.callback(
        Output("productInfo-EU", "data"),
        [
            Input("productCalc-selector-EU", "value"),
            Input("monthCalc-selector-EU", "value"),
            # Input("monthCalc-selector-EU", "options"),
        ],
    )
    def updateProduct(product, month):  # deleted options to make page slightly faster
        if product and month:
            # this will be outputting redis data from option engine, currently no euronext keys in redis
            # for euronext wheat, feb/march is 'xext-ebm-eur o 23-02-15 a'
            # OVERWRITING USER INPUT FOR TESTING
            # month = "lcuom3"
            if USE_DEV_KEYS:
                month = month + ":dev"
            params = loadRedisData(month)
            params = json.loads(params)
            return params

    def placholderCheck(value, placeholder):  # should be fine
        if type(value) is float:
            return value, value
        elif type(placeholder) is float:
            return placeholder, placeholder
        elif value and value != None and value != " ":
            value = value.split("/")
            if len(value) > 1:
                if value[1] != "":
                    return float(value[0]), float(value[1])
                else:
                    return float(value[0]), float(value[0])
            else:
                return float(value[0]), float(value[0])

        elif placeholder and placeholder != " ":
            placeholder = placeholder.split("/")
            if len(placeholder) > 1 and placeholder[1] != " ":
                return float(placeholder[0]), float(placeholder[1])
            else:
                return float(placeholder[0]), float(placeholder[0])
        else:
            return 0, 0

    def strikePlaceholderCheck(value, placeholder):  # should be fine (unused)
        if value:
            return value
        elif placeholder:
            value = placeholder.split(".")
            return value[0]
        else:
            return 0

    legOptions = ["one", "two", "three", "four"]

    # create fecth strikes function
    def buildFetchStrikes():  # UNUSED
        def updateDropdown(product, month, cop):
            if product and month:
                if cop == "f" or month == "3M":
                    return ""
                else:
                    product = product + "O" + month
                    strikes = fetechstrikes(product)
                    length = int(len(strikes) / 2)
                    value = strikes[length]["value"]
                    return value
            return updateDropdown

    # create vola function    DONE
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
                                # settle = calc_lme_vol(
                                #     params, float(forward), float(strike)
                                # )
                                return vol  # , 0  # round(settle * 100, 2)
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
                                # settle = calc_lme_vol(
                                #     params, float(forward), float(strike)
                                # )
                                return price  # , 0  # settle * 100
                else:
                    return 0  # , 0

        return updateVola

    def buildvolaCalc(leg):  # should be fine, UNUSED
        def volaCalc(
            expiry,
            nowOpen,
            rate,
            prate,
            forward,
            pforward,
            strike,
            pStrike,
            cop,
            priceVola,
            ppriceVola,
            volprice,
            days,
            params,
        ):
            # get inputs placeholders vs values
            if not strike:
                strike = pStrike
            Brate, Arate = placholderCheck(rate, prate)
            Bforward, Aforward = placholderCheck(forward, pforward)
            BpriceVola, ApriceVola = placholderCheck(priceVola, ppriceVola)

            # if no params then return nothing
            if not params or cop == "f":
                Bgreeks = [
                    0,
                    1,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    1,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                ]
                return {"bid": Bgreeks, "Bvol": 0}

            # set eval date
            eval_date = dt.datetime.now()
            # build params object
            params = buildSurfaceParams(params, Bforward, expiry, eval_date)

            if None not in (
                expiry,
                Bforward,
                Aforward,
                BpriceVola,
                ApriceVola,
                strike,
                cop,
            ):
                if nowOpen == "now":
                    now = True
                else:
                    now = False
                today = dt.datetime.today()
                if volprice == "vol":
                    option = Option(
                        cop,
                        Bforward,
                        strike,
                        today,
                        expiry,
                        Brate / 100,
                        BpriceVola / 100,
                        days=days,
                        now=now,
                        params=params,
                    )
                    Bgreeks = option.get_all()

                    return {"bid": Bgreeks, "Bvol": BpriceVola}

                elif volprice == "price":
                    option = Option(
                        cop,
                        Bforward,
                        strike,
                        today,
                        expiry,
                        Brate / 100,
                        0,
                        price=BpriceVola,
                        days=days,
                        now=now,
                        params=params,
                    )
                    option.get_impl_vol()
                    Bvol = option.vol
                    Bgreeks = list(option.get_all())
                    Bgreeks[0] = BpriceVola

                    return {"bid": Bgreeks, "Bvol": Bvol * 100}

        return volaCalc

    def createLoadParam(param):  # should be fine, UNUSED
        def loadtheo(params):
            # pull greeks from stored hidden
            if params != None:
                return str("%.4f" % params["bid"][param[1]])
            else:
                return str("%.4f" % 0)

        return loadtheo

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

    def buildTheoIV():  # can stay the same, new params has a vol attribute as well
        def loadIV(params):
            if params != None:
                # params = json.loads(params)
                return str("%.4f" % params["vol"])
            else:
                return 0

        return loadIV

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

        # update vol_price placeholder # CHANGE THE called function
        # app.callback(
        #     # [
        #     Output("{}Vol_price-EU".format(leg), "placeholder"),
        #     # Output("{}SettleVol-EU".format(leg), "children"),
        #     # ],
        #     [
        #         Input("productInfo-EU", "data"),
        #         Input("{}Strike-EU".format(leg), "value"),
        #         Input("{}Strike-EU".format(leg), "placeholder"),
        #         Input("{}CoP-EU".format(leg), "value"),
        #         Input("calculatorVol_price-EU", "value"),
        #         Input("calculatorForward-EU", "placeholder"),
        #         Input("calculatorForward-EU", "value"),
        #     ],
        # )(buildUpdateVola(leg))

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
            Output("strat{}-EU".format(param), "children"),
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

    inputs = ["interestRate-EU", "calculatorBasis-EU", "calculatorSpread-EU"]

    @app.callback(
        [Output("{}".format(i), "placeholder") for i in inputs]
        + [Output("{}".format(i), "value") for i in inputs]
        + [Output("{}Strike-EU".format(i), "placeholder") for i in legOptions],
        [Input("productInfo-EU", "data")],
    )
    def updateInputs(params):
        if params:
            params = pd.DataFrame.from_dict(params, orient="index")
            # get price of underlying from whichever option
            atm = float(params.iloc[0]["und_calc_price"])
            # get the two closest strikes to the atm (c&p)
            params = params.iloc[(params["strike"] - atm).abs().argsort()[:2]]
            # set placeholders
            valuesList = [""] * len(inputs)
            # create list of atm strikes to populate strike placeholders
            atmList = [params.iloc[0]["strike"]] * len(legOptions)
            spread = 0
            return (
                [
                    round(params.iloc[0]["interest_rate"] * 100, 4),  # correct for euronext
                    atm,  # correct for euronext
                    spread,  # correct for euronext
                ]
                + valuesList
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
        def updateOptionInfo(strike, strikePH, settleVols):
            # placeholder check
            if not settleVols:
                return 0

            if not strike:
                strike = strikePH
            # round strike to nearest integer
            strike = int(strike)

            # array of dicts to df
            df = pd.DataFrame(settleVols)

            # set strike behaviour on the wings
            if strike > df["strike"].max():
                strike = max
            elif strike < df["strike"].min():
                strike = min

            # get the row of the df with the strike
            vol = df.loc[df["strike"] == strike]["vol"].values[0]
            vol = round(vol, 2)

            return vol, vol
