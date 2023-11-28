from TradeClass import VolSurface
from company_styling import main_color, logo
from calculators import linearinterpol
from data_connections import (
    conn,
    select_from,
    PostGresEngine,
    HistoricalVolParams,
    Session,
    engine,
)
import sftp_utils

import upestatic

import dash_bootstrap_components as dbc
import backports.zoneinfo as zoneinfo
from dateutil import relativedelta
from pytz import timezone
import sqlalchemy.orm
from dash import html
import ujson as json
import pandas as pd
import numpy as np

from datetime import datetime, timedelta
from typing import List, Optional, Tuple
from email.message import EmailMessage
import pickle, math, os, time
from datetime import date
from time import sleep
import mimetypes
import smtplib


sdLocation = os.getenv("SD_LOCAITON", default="staticdata")
positionLocation = os.getenv("POS_LOCAITON", default="greekpositions")

DAYS_TO_TRADE_REC = 5

GEORGIA_LME_SYMBOL_VERSION_OLD_NEW_MAP = {
    "lad": "xlme-lad-usd",
    "aluminium": "xlme-lad-usd",
    "lcu": "xlme-lcu-usd",
    "copper": "xlme-lcu-usd",
    "lzh": "xlme-lzh-usd",
    "zinc": "xlme-lzh-usd",
    "pbd": "xlme-pbd-usd",
    "lead": "xlme-pbd-usd",
    "lnd": "xlme-lnd-usd",
    "nickel": "xlme-lnd-usd",
}


# this isn't good
multipliers = {
    "aluminium": 25,
    "copper": 25,
    "lead": 25,
    "nickel": 6,
    "zinc": 25,
    "xext-ebm-eur": 50,
}


def loadStaticData():
    # pull staticdata from redis
    i = 0
    while i < 5:
        try:
            staticData = conn.get(sdLocation)
            staticData = pd.read_json(staticData)
            break
        except Exception as e:
            time.sleep(1)
            i = i + 1

    # filter for non expired months
    today = datetime.now()  # + timedelta(days=1)
    today = today.strftime("%Y-%m-%d")
    staticData = staticData[
        pd.to_datetime(staticData["expiry"], format="%d/%m/%Y").dt.strftime("%Y-%m-%d")
        >= today
    ]
    return staticData


def loadStaticDataExpiry():
    # pull staticdata from redis, but includes products with expiry today
    i = 0
    while i < 5:
        try:
            staticData = conn.get(sdLocation)
            staticData = pd.read_json(staticData)
            break
        except Exception as e:
            time.sleep(1)
            i = i + 1

    # filter for non-expired months
    today = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    staticData = staticData[
        pd.to_datetime(staticData["expiry"], format="%d/%m/%Y").dt.strftime("%Y-%m-%d")
        >= today
    ]
    return staticData


def getPromptFromLME(product: str) -> str:
    # pull staticdata from redis, but includes products with expiry today
    i = 0
    while i < 3:
        try:
            staticData = conn.get(sdLocation)
            staticData = pd.read_json(staticData)
            break
        except Exception as e:
            time.sleep(1)
            i = i + 1

    # filter for non-expired months
    today = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    staticData = staticData[
        pd.to_datetime(staticData["expiry"], format="%d/%m/%Y").dt.strftime("%Y-%m-%d")
        >= today
    ]
    # get prompt value for first product that matches the product name
    prompt = staticData[staticData["product"] == product]["expiry"].values[0]

    return prompt


# needs password removed
# gareth
def send_email(to, subject, body, att=None, att_name=None):
    try:
        # Create message container - the correct MIME type is multipart/alternative.
        msg = EmailMessage()

        # build message requirements
        msg["From"] = "georgia@upetrading.com"
        msg["To"] = to
        msg["Subject"] = subject

        # see the code below to use template as body
        msg.set_content(body)

        # if attachment add to mail
        if att:
            filename = att_name
            maintype, _, subtype = (
                mimetypes.guess_type(filename)[0] or "application/octet-stream"
            ).partition("/")
            # Add as attachment
            csv = att.read()
            msg.add_attachment(
                csv, maintype=maintype, subtype=subtype, filename=filename
            )

        # build mail class
        mail = smtplib.SMTP("smtp.office365.com", 587, timeout=30)

        # use TLS
        mail.starttls()

        # build mail and add message
        recepient = [to]
        mail.login("georgia@upetrading.com", "Bop54730")
        mail.sendmail("georgia@upetrading.com", recepient, msg.as_string())
        mail.quit()

    except Exception as e:
        raise e


def loadRedisData(product):
    new_data = conn.get(product)
    return new_data


def retriveParams(product, dev_keys=False):
    if dev_keys:
        paramData = conn.get(product + "Vola" + ":dev")
    else:
        paramData = conn.get(product + "Vola")
    paramData = buildParamsList(paramData)
    return paramData


def loadVolaData(product):
    product = product + "Vola"
    new_data = conn.get(product)
    new_data = json.loads(new_data)
    return new_data


def calc_vol(params, und, strike):
    # convert expiry to datetime
    expiry = int(params.iloc[0]["expiry"])
    expiry = date.fromtimestamp(expiry / 1e3)

    today = datetime.now()
    # today = today.strftime('%Y-%m-%d')

    params = params.iloc[0]

    model = VolSurface(
        s=und,
        vol=params["vola"],
        sk=params["skew"],
        c=params["calls"],
        p=params["puts"],
        cmax=params["cmax"],
        pmax=params["pmax"],
        exp_date=expiry,
        eval_date=today,
        ref=0,
    )

    return model.get_vola(strike)


def calc_lme_vol(params, und, strike):
    # select first row
    params = params.iloc[0]
    # pull model inputs
    modelparams = params["settle_model_inputs"]

    # build vol model
    model = linearinterpol(
        und,
        params["t"],
        params["interest_rate"],
        atm_vol=modelparams["vol"],
        var1=modelparams["var1"],
        var2=modelparams["var2"],
        var3=modelparams["var3"],
        var4=modelparams["var4"],
    ).model()

    vol = model(strike)
    vol = np.round(vol, 4)
    return vol


# take inputs and include spot and time to buildvolsurface inputs
def buildSurfaceParams(params, spot, exp_date, eval_date):
    volParams = VolSurface(
        spot,
        params["vola"],
        params["skew"],
        params["calls"],
        params["puts"],
        params["cmax"],
        params["pmax"],
        exp_date,
        eval_date,
        ref=params["ref"],
        k=None,
    )
    return volParams


def buildParamsList(Params):
    paramslist = {
        "spread": 0,
        "vol": 0,
        "10 delta": 0,
        "25 delta": 0,
        "75 delta": 0,
        "90 delta": 0,
        "skew": 0,
        "call": 0,
        "put": 0,
        "cmax": 0,
        "pmax": 0,
        "ref": 0,
    }
    if Params:
        Params = json.loads(Params)
        paramslist["spread"] = Params["spread"]
        paramslist["vol"] = Params["vola"]
        paramslist["ref"] = Params["ref"]
        try:
            paramslist["skew"] = Params["skew"]
            paramslist["call"] = Params["calls"]
            paramslist["put"] = Params["puts"]
            paramslist["cmax"] = Params["cmax"]
            paramslist["pmax"] = Params["pmax"]
        except KeyError:
            pass
        try:
            paramslist["10 delta"] = Params["10 delta"]
            paramslist["25 delta"] = Params["25 delta"]
            paramslist["75 delta"] = Params["75 delta"]
            paramslist["90 delta"] = Params["90 delta"]
        except KeyError:
            pass
    return paramslist


def buildParamMatrix(portfolio, dev_keys=False):
    # pull sttaicdata and extract prodcuts and sol3_names
    staticData = loadStaticData()

    products = staticData[staticData["name"] == portfolio]["product"]
    sol3_names = staticData[staticData["name"] == portfolio]["sol_vol"]

    # dict to build params
    params = {}

    # dict to gather vols
    curve_dicts = {}

    for product in products:
        # load each month into the params list
        param = retriveParams(product.lower(), dev_keys=dev_keys)

        # find prompt
        prompt = staticData[staticData["product"] == product]["expiry"].values

        # add prompt to df for sorting later
        param["prompt"] = prompt[0]

        params[product] = param

        # find the sol_name for the current prodcut
        sol_name = staticData[staticData["product"] == product]["sol_vol"].values[0]

        if sol_name:
            vol_data = conn.get(sol_name)
            if vol_data:
                vol_data = json.loads(vol_data)
                curve_dicts[product] = vol_data

    return pd.DataFrame.from_dict(params, orient="index"), curve_dicts


# retive data for a certain product returning dataframe
def retriveTickData(product):
    tickData = conn.get(product.lower() + "MD")
    if tickData:
        tickData = json.loads(tickData)
        cols = ("TimeStamp", "Price")
        tickData = pd.DataFrame(columns=cols, data=tickData)
        return tickData


def buildTableData(data):
    cols = (
        "Cpos",
        "Ctheo",
        "Cdelta",
        "strikes",
        "Ppos",
        "Ptheo",
        "Vega",
        "Gamma",
        "Theta",
        "Volas",
        "SettleVol",
        "Skew",
        "Call",
        "Put",
        "Vp",
        "FD",
    )
    greeks = []
    # for each strike load data from json file
    for strike in data["strikes"]:
        # oull product mult to convert greeks later
        mult = float(data["mult"])
        Ctheo = float(data["strikes"][strike]["C"]["theo"])
        Ptheo = float(data["strikes"][strike]["P"]["theo"])
        Cdelta = float(data["strikes"][strike]["C"]["delta"])
        vega = float(data["strikes"][strike]["C"]["vega"]) * mult
        Cposition = float(data["strikes"][strike]["C"]["position"])
        Pposition = float(data["strikes"][strike]["P"]["position"])
        vola = float(data["strikes"][strike]["P"]["vola"])
        # add call or put settlement vol is exists
        if data["strikes"][strike]["P"]["settlevol"]:
            settleVola = data["strikes"][strike]["P"]["settlevol"]
        elif data["strikes"][strike]["C"]["settlevol"]:
            settleVola = data["strikes"][strike]["C"]["settlevol"]
        else:
            settleVola = 0

        if type(settleVola) == float:
            settleVola = round(settleVola, 4) * 100
        else:
            settleVola = ""
        gamma = float(data["strikes"][strike]["P"]["gamma"])
        skew = (float(data["strikes"][strike]["P"]["skewSense"])) * vega
        call = (float(data["strikes"][strike]["P"]["callSense"])) * vega
        put = (float(data["strikes"][strike]["P"]["putSense"])) * vega
        theta = float(data["strikes"][strike]["P"]["theta"])
        vp = float(data["strikes"][strike]["P"]["vp"]) * 100
        fd = float(data["strikes"][strike]["C"]["fullDelta"])

        greeks.append(
            (
                Cposition,
                "%.2f" % Ctheo,
                "%.2f" % Cdelta,
                float(strike),
                Pposition,
                "%.2f" % Ptheo,
                "%.2f" % vega,
                "%.4f" % gamma,
                "%.2f" % theta,
                "%.2f" % (vola * 100),
                settleVola,
                "%.2f" % skew,
                "%.2f" % call,
                "%.2f" % put,
                "%.4f" % vp,
                "%.6f" % fd,
            )
        )
        strikeGreeks = pd.DataFrame(columns=cols, data=greeks)
        strikeGreeks = strikeGreeks.sort_values(["strikes"], ascending=True)
    return strikeGreeks


def buildTradesTableData(data):
    cols = (
        "Instrument",
        "Qty",
        "Theo",
        "Prompt",
        "Forward",
        "IV",
        "Delta",
        "Gamma",
        "Vega",
        "Theta",
        "Carry Link",
        "Counterparty",
    )
    greeks = []
    Ttheo = Tdelta = Tgamma = Tvega = Ttheta = 0

    # for each strike load data from json file
    for instrument in data:
        qtyCalc = math.fabs((data[instrument]["qty"]))
        qty = data[instrument]["qty"]
        theo = float(data[instrument]["theo"])
        prompt = data[instrument]["prompt"]
        forward = data[instrument]["forward"]
        IV = float(data[instrument]["iv"])
        delta = round(float(data[instrument]["delta"]), 2)
        gamma = round(float(data[instrument]["gamma"]), 3)
        vega = round(float(data[instrument]["vega"]), 2)
        theta = round(float(data[instrument]["theta"]), 2)
        carry_link = data[instrument]["carry link"]
        counterparty = data[instrument]["counterparty"]

        Ttheo = Ttheo + theo
        Tdelta = Tdelta + delta
        Tgamma = Tgamma + gamma
        Tvega = Tvega + vega
        Ttheta = Ttheta + theta

        greeks.append(
            (
                instrument.upper(),
                qty,
                theo,
                prompt,
                forward,
                IV,
                delta,
                gamma,
                vega,
                theta,
                carry_link,
                counterparty,
            )
        )

    greeks.append(
        ("Total", " ", Ttheo, "", "", "", Tdelta, Tgamma, Tvega, Ttheta, "", "")
    )
    trades = pd.DataFrame(columns=cols, data=greeks)

    return trades


def buildOptionsBoard(data, volPrem):
    if volPrem == "vol":
        cols = ("Call Bid", "Call Offer", "Strike", "Put Bid", "Put Offer")
        greeks = []
        # for each strike load data from json file
        for strike in data:
            cBidVol = "%.2f" % (data[strike]["C"]["bidVol"] * 100)
            cAskVol = "%.2f" % (data[strike]["C"]["askVol"] * 100)
            pBidVol = "%.2f" % (data[strike]["P"]["bidVol"] * 100)
            pAskVol = "%.2f" % (data[strike]["P"]["askVol"] * 100)

            greeks.append((cBidVol, cAskVol, strike, pBidVol, pAskVol))
            strikeGreeks = pd.DataFrame(columns=cols, data=greeks)
            strikeGreeks = strikeGreeks.sort_values(["Strike"], ascending=True)
        return strikeGreeks
    elif volPrem == "prem":
        cols = ("Call Bid", "Call Offer", "Strike", "Put Bid", "Put Offer")
        greeks = []
        # for each strike load data from json file
        for strike in data:
            cBidPrem = "%.2f" % (data[strike]["C"]["bidPrice"])
            cAskPrem = "%.2f" % (data[strike]["C"]["askPrice"])
            pBidPrem = "%.2f" % (data[strike]["P"]["bidPrice"])
            pAskPrem = "%.2f" % (data[strike]["P"]["askPrice"])

            greeks.append((cBidPrem, cAskPrem, strike, pBidPrem, pAskPrem))
            strikeGreeks = pd.DataFrame(columns=cols, data=greeks)
            strikeGreeks = strikeGreeks.sort_values(["Strike"], ascending=True)
        return strikeGreeks


# go to redis and get theo for instrument
def get_theo(instrument):
    instrument = instrument.split(" ")
    data = conn.get(instrument[0].lower())
    data = json.loads(data)
    if data != None:
        theo = data["strikes"][instrument[1]][instrument[2]]["theo"]
        return float(theo)


def productOptions():
    try:
        df = loadStaticData()
    except:
        options = [{"label": "error", "value": "error"}]
        return options

    values = df.name.unique()
    options = [{"label": i, "value": i} for i in values]
    return options


def pullRates(currency):
    curve = json.loads(conn.get(currency.upper() + "Rate"))
    curve = pd.DataFrame.from_dict(curve, orient="index")
    return curve


def pullPrompts(product):
    rates = pickle.loads(conn.get(product.lower() + "Curve"))

    return rates


def redistrades(trade):
    product = trade[1]
    trade = json.loads(conn.get(product + "trades"))


def pullPortfolioGreeks():
    data = conn.get(positionLocation)
    if data != None:
        greeks = pd.read_json(data)
        return greeks


def lme_option_to_georgia(product, series):
    products = {"ah": "lad", "zs": "lzh", "pb": "pbd", "ca": "lcu", "ni": "lnd"}
    months = {
        "jan": "f",
        "feb": "g",
        "mar": "h",
        "apr": "j",
        "may": "k",
        "jun": "m",
        "jul": "n",
        "aug": "q",
        "sep": "u",
        "oct": "v",
        "nov": "x",
        "dec": "z",
    }

    return products[product.lower()] + "o" + months[series[:3].lower()] + series[-1:]


def settleVolsProcess():
    # pull vols from postgres
    vols = select_from("get_settlement_vols")

    # convert lme names
    vols["instrument"] = vols.apply(
        lambda row: lme_option_to_georgia(row["Product"], row["Series"]), axis=1
    )

    # convert to vols from diff
    vol_cols = ["-10 DIFF", "-25 DIFF", "+25 DIFF", "+10 DIFF"]
    for cols in vol_cols:
        vols[cols] = vols[cols] + vols["50 Delta"]

    # set instrument to index
    vols.set_index("instrument", inplace=True)
    vols = vols[~vols.index.duplicated(keep="first")]

    # send to redis
    pick_vols = pickle.dumps(vols)
    conn.set("lme_vols", pick_vols)

    # tell options engine that theres new vols
    products = loadStaticData()
    products = products["product"].values

    for product in products:
        pic_data = pickle.dumps([product, "staticdata"])
        conn.publish("compute", pic_data)


def monthSymbol(prompt):
    month = prompt.month
    year = prompt.year
    year = str(year)[-1:]

    if month == 1:
        return "F" + str(year)
    elif month == 2:
        return "G" + str(year)
    elif month == 3:
        return "H" + str(year)
    elif month == 4:
        return "J" + str(year)
    elif month == 5:
        return "K" + str(year)
    elif month == 6:
        return "M" + str(year)
    elif month == 7:
        return "N" + str(year)
    elif month == 8:
        return "Q" + str(year)
    elif month == 9:
        return "U" + str(year)
    elif month == 10:
        return "V" + str(year)
    elif month == 11:
        return "X" + str(year)
    elif month == 12:
        return "Z" + str(year)


def timeStamp():
    now = datetime.datetime.now()
    now.strftime("%Y-%m-%d %H:%M:%S")
    return now


def sumbiSettings(product, settings):
    settings = json.dumps(settings)
    conn.set(product + "Settings", settings)


def retriveSettings(product):
    settings = conn.get(product + "Settings")

    return settings


def callRedis(query):
    data = conn.get(query)
    return data


def loadDeltaPosition(product):
    data = conn.get(product.lower() + "Delta")
    data = pickle.loads(data)
    return data


def portfolioToProduct(portfolio):
    if portfolio.lower() == "copper":
        return "lcu"
    elif portfolio.lower() == "aluminium":
        return "lad"
    elif portfolio.lower() == "lead":
        return "pbd"
    elif portfolio.lower() == "nickel":
        return "lnd"
    elif portfolio.lower() == "zinc":
        return "lzh"
    else:
        return "unkown"


def productToPortfolio(product):
    if product.lower() == "lcu":
        return "copper"
    elif product.lower() == "lad":
        return "aluminium"
    elif product.lower() == "pbd":
        return "lead"
    elif product.lower() == "lnd":
        return "nickel"
    elif product.lower() == "lzh":
        return "zinc"
    else:
        return "unkown"


def pullPnl():
    data = conn.get("pnl")
    return data


def PortfolioPnlTable(data):
    pnl = []
    pTrade = pPos = Total = 0

    for portfolio in data:
        totalPnl = data[portfolio]["pPos"] + data[portfolio]["pTrade"]
        pnl.append(
            {
                "Portfolio": portfolio.capitalize(),
                "Trade Pnl": round(data[portfolio]["pTrade"], 2),
                "Position PNL": round(data[portfolio]["pPos"], 2),
                "Total PNL": round(totalPnl, 2),
            }
        )
        pTrade += data[portfolio]["pTrade"]
        pPos += data[portfolio]["pPos"]
        Total += data[portfolio]["pPos"] + data[portfolio]["pTrade"]
    pnl.append(
        {
            "Portfolio": "Total",
            "Trade Pnl": round(pTrade, 2),
            "Position PNL": round(pPos, 2),
            "Total PNL": round(Total, 2),
        }
    )
    return pnl


def productPnlTable(data, portfolio):
    pnl = []
    if data:
        for product in data[portfolio]["product"]:
            product = product.upper()
            totalPnl = float(
                data[portfolio]["product"][product]["tPos"]
                + data[portfolio]["product"][product]["tTrade"]
            )

            pnl.append(
                {
                    "Product": product,
                    "Trade Pnl": round(
                        float(data[portfolio]["product"][product]["tTrade"]), 2
                    ),
                    "Position PNL": round(
                        float(data[portfolio]["product"][product]["tPos"]), 2
                    ),
                    "Total PNL": round(float(totalPnl), 2),
                }
            )

        return pnl


def strikePnlTable(data, portfolio, product):
    pnl = []
    if data:
        # data = json.loads(data)
        for strike in data[portfolio]["product"][product]["strikes"]:
            for cop in ["C", "P"]:
                pnl.append(
                    {
                        "Strike": strike,
                        "CoP": cop,
                        "Trade Pnl": data[portfolio]["product"][product]["strikes"][
                            strike
                        ][cop]["tradePnl"],
                        "Position PNL": data[portfolio]["product"][product]["strikes"][
                            strike
                        ][cop]["posPnl"],
                        "Total PNL": (
                            data[portfolio]["product"][product]["strikes"][strike][cop][
                                "posPnl"
                            ]
                            + data[portfolio]["product"][product]["strikes"][strike][
                                cop
                            ]["tradePnl"]
                        ),
                    }
                )
        return pnl


def unpackPriceRisk(data, tm):
    greeks = []

    for risk in data[list(data.keys())[0]][" 0.0"]:
        greek = {}
        # add greek name to table
        greek["Greek"] = risk.capitalize()
        # iter over prices changes and add greeks value
        for i in data:
            # add three month price to convert from change to absolute
            price = float(i) + tm

            greek[price] = round(data[i][" 0.0"][risk], 2)

        greeks.append(greek)

    return greeks


def heatunpackRisk(data, greek):
    output = []
    underlying = []

    for i in data:
        greeks = []
        underlying.append(i)
        volaility = []
        for j in data[i]:
            greeks.append(round(float(data[i][j][greek]), 2))
            volaility.append(float(j) * 100)
        output.append(greeks)

    return output, underlying, volaility


heampMapColourScale = [
    [0, "rgb(255, 0, 0)"],
    [0.1, "rgb(255, 0, 0)"],
    [0.1, "rgb(255, 0, 0)"],
    [0.2, "rgb(226, 28, 0)"],
    [0.2, "rgb(226, 28, 0)"],
    [0.3, "rgb(198, 56, 0)"],
    [0.3, "rgb(198, 56, 0)"],
    [0.4, "rgb(170, 85, 0)"],
    [0.4, "rgb(170, 85, 0)"],
    [0.5, "rgb(141, 113, 0)"],
    [0.5, "rgb(141, 113, 0)"],
    [0.6, "rgb(113, 141, 0)"],
    [0.6, "rgb(113, 141, 0)"],
    [0.7, "rgb(85, 170, 0)"],
    [0.7, "rgb(85, 170, 0)"],
    [0.8, "rgb(56, 198, 0)"],
    [0.8, "rgb(56, 198, 0)"],
    [0.9, "rgb(28, 226, 0)"],
    [0.9, "rgb(28, 226, 0)"],
    [1.0, "rgb(0, 255, 0)"],
]


def productsFromPortfolio(portfolio):
    staticData = loadStaticData()
    staticData = staticData.loc[staticData["portfolio"] == portfolio]
    staticData = staticData.sort_values("expiry")
    product = staticData["product"]

    return product


def curren3mPortfolio(portfolio):
    products = productsFromPortfolio(portfolio)
    product = products.values[0].lower()
    data = loadRedisData(product)
    if data:
        jData = json.loads(data)
        und = list(jData.values())[0]["und_calc_price"]
        return und


def getDeltaPrompt(portfolio):
    data = conn.get(portfolio.lower())
    if data:
        data = json.loads(data)
        return data


def getOptionDelta(portfolio):
    data = getDeltaPrompt(portfolio)
    deltas = {}
    for product in data:
        if product == "Total" or product == portfolio:
            continue
        delta = data[product]["delta"]
        prompt = datetime.datetime.strptime(
            data[product]["und_prompt"], "%d/%m/%Y"
        ).strftime("%d-%m-%Y")
        deltas[prompt] = {"delta": delta}

    return deltas


def optionPrompt(product):
    staticData = loadStaticData()
    # staticData = pd.read_json(staticData)
    staticdata = staticData.loc[staticData["product"] == product.upper()]
    staticdata = staticdata["third_wed"].values[0]
    date = staticdata.split("/")
    prompt = date[2] + "-" + date[1] + "-" + date[0]
    return prompt


def convertInstrumentName(row):
    if row["optionTypeId"] in ["C", "P"]:
        # is option
        product = (
            row["productId"]
            + monthSymbol(row["prompt"])
            + " "
            + str(int(row["strike"]))
            + " "
            + row["optionTypeId"]
        )
    else:
        # is underlying
        product = row["productId"] + " " + str(row["prompt"].strftime("%Y-%m-%d"))
    return product


def tradeID():
    epoch_time = time.time() * (10**6)
    epoch_time = str(int(epoch_time))[-12:]
    tradeID = "MVD" + epoch_time
    sleep(0.000001)
    return tradeID


def productToPortfolio(product):
    if product.lower() == "lcu":
        return "copper"
    elif product.lower() == "lad":
        return "aluminium"
    elif product.lower() == "pbd":
        return "lead"
    elif product.lower() == "lnd":
        return "nickel"
    elif product.lower() == "lzh":
        return "zinc"
    else:
        return "unkown"


def is_between(time, time_range):
    if time_range[1] < time_range[0]:
        return time >= time_range[0] or time <= time_range[1]
    return time_range[0] <= time <= time_range[1]


def ringTime():
    now = datetime.now(tz=timezone("Europe/London")).time()
    if is_between(str(now), ("12:30", "12:35")):
        return "Copper Ring 2"
    elif is_between(str(now), ("12:35", "12:40")):
        return "Aluminium alloy Ring 2"
    elif is_between(str(now), ("12:40", "12:45")):
        return "Tin Ring 2"
    elif is_between(str(now), ("12:45", "12:50")):
        return "Lead Ring 2"
    elif is_between(str(now), ("12:50", "12:55")):
        return "Zinc Ring 2"
    elif is_between(str(now), ("12:55", "13:00")):
        return "Aluminium Ring 2"
    elif is_between(str(now), ("13:00", "13:05")):
        return "Nickel Ring 2"
    elif is_between(str(now), ("13:05", "13:10")):
        return "Aluminium Premiums Ring 2"
    elif is_between(str(now), ("13:10", "13:15")):
        return "Steel Billet Ring 2"
    elif is_between(str(now), ("13:15", "13:25")):
        return "Interval"
    elif is_between(str(now), ("13:25", "13:35")):
        return "Kerb Trading"
    elif is_between(str(now), ("13:35", "14:55")):
        return "Lunch Interval"
    elif is_between(str(now), ("14:55", "16:15")):
        return "Afternoon Rings"
    elif is_between(str(now), ("16:15", "16:25")):
        return "Zinc Kerb"
    elif is_between(str(now), ("16:25", "16:30")):
        return "Tin Kerb"
    elif is_between(str(now), ("16:30", "16:35")):
        return "Lead Kerb"
    elif is_between(str(now), ("16:35", "16:40")):
        return "Cobalt Kerb"
    elif is_between(str(now), ("16:40", "16:45")):
        return "Aluminium Kerb"
    elif is_between(str(now), ("16:45", "16:50")):
        return "Aluminium Derivatives Kerb"
    elif is_between(str(now), ("16:50", "16:55")):
        return "Copper Kerb"
    elif is_between(str(now), ("16:55", "17:02")):
        return "Nickel Kerb"


def topMenu(page):
    return html.Div(
        [
            dbc.Navbar(
                children=[
                    html.A(
                        dbc.Row(
                            [
                                dbc.Col(html.Img(src=logo, height="40px")),
                                dbc.Col(dbc.NavbarBrand(page, className="ml-1")),
                            ]
                        ),
                        href="/",
                    ),
                    dbc.DropdownMenu(
                        children=[
                            dbc.DropdownMenuItem("Calculator", href="/calculator"),
                            dbc.DropdownMenuItem("LME Carry", href="/lmecarry"),
                            dbc.DropdownMenuItem(
                                "Calculator EUR", href="/calculatorEUR"
                            ),
                            dbc.DropdownMenuItem("Vol Surface", href="/volsurface"),
                            dbc.DropdownMenuItem("Vol Matrix", href="/volMatrix"),
                            dbc.DropdownMenuItem("Pnl", href="/pnl"),
                        ],
                        # nav=True,
                        in_navbar=True,
                        label="Pricing",
                    ),
                    dbc.DropdownMenu(
                        children=[
                            dbc.DropdownMenuItem("Risk Matrix", href="/riskmatrix"),
                            dbc.DropdownMenuItem("Strike Risk", href="/strikeRisk"),
                            dbc.DropdownMenuItem(
                                "Strike Risk New", href="/strikeRiskNew"
                            ),
                            dbc.DropdownMenuItem("Delta Vola", href="/deltaVola"),
                            dbc.DropdownMenuItem("Portfolio", href="/portfolio"),
                            dbc.DropdownMenuItem("Prompt Curve", href="/prompt"),
                        ],
                        # nav=True,
                        in_navbar=True,
                        label="Risk",
                    ),
                    dbc.DropdownMenu(
                        children=[
                            dbc.DropdownMenuItem("Trades", href="/trades"),
                            dbc.DropdownMenuItem("Position", href="/position"),
                            # dbc.DropdownMenuItem("F2 Rec", href="/rec"),
                            dbc.DropdownMenuItem("Route Status", href="/routeStatus"),
                            dbc.DropdownMenuItem("Expiry", href="/expiry"),
                            # dbc.DropdownMenuItem("Rate Curve", href="/rates"),
                            dbc.DropdownMenuItem("Mark to Market", href="/m2m_rec"),
                            dbc.DropdownMenuItem("Cash Manager", href="/cashManager"),
                        ],
                        # nav=True,
                        in_navbar=True,
                        label="Reconciliation",
                    ),
                    dbc.DropdownMenu(
                        children=[
                            dbc.DropdownMenuItem("Static Data", href="/staticData"),
                            dbc.DropdownMenuItem("Brokers", href="/brokers"),
                            dbc.DropdownMenuItem("Data Load", href="/dataload"),
                            dbc.DropdownMenuItem("Data Download", href="/dataDownload"),
                            dbc.DropdownMenuItem("Logs", href="/logpage"),
                            dbc.DropdownMenuItem("Calendar", href="/calendarPage"),
                        ],
                        # nav=True,
                        in_navbar=True,
                        label="Settings",
                    ),
                    html.Div([ringTime()]),
                ],
                color=main_color,
                dark=True,
            )
        ]
    )


def onLoadProductProducts():
    try:
        staticData = loadStaticData()
    except:
        products = [{"label": "error", "value": "error"}]
        return products, products[0]["value"]

    products = []
    staticData["product"] = [x[:3] for x in staticData["product"]]
    productNames = staticData["product"].unique()
    staticData.sort_values("product")
    for product in productNames:
        products.append({"label": product, "value": product})
    return products, products[0]["value"]


def onLoadPortFolio():
    try:
        staticData = loadStaticData()
    except:
        portfolios = [{"label": "error", "value": "error"}]
        return portfolios

    portfolios = []
    for portfolio in staticData.portfolio.unique():
        portfolios.append({"label": portfolio.capitalize(), "value": portfolio})
    return portfolios


def onLoadPortFolioAll():
    try:
        staticData = loadStaticData()
    except:
        portfolios = [{"label": "error", "value": "error"}]
        return portfolios

    portfolios = [{"label": "All", "value": "all"}]
    for portfolio in staticData.portfolio.unique():
        portfolios.append({"label": portfolio.capitalize(), "value": portfolio})
    return portfolios


def strikeRisk(portfolio, riskType, relAbs):
    # pull list of porducts from static data
    static = conn.get("staticData")
    # static = json.loads(static)
    df = pd.read_json(static)
    products = df[df["portfolio"] == portfolio]["product"].values

    # setup greeks and products bucket to collect data
    greeks = []
    productset = []

    if relAbs == "strike":
        # for each product collect greek per strike
        for product in products:
            productset.append(product)
            data = conn.get(product.lower())
            if data:
                data = json.loads(data)
                strikegreeks = []
                # go over strikes and uppack greeks
                strikeset = []

                for strike in data["strikes"]:
                    # pull product mult to convert greeks later
                    strikeset.append(strike)
                    mult = float(data["mult"])

                    Cposition = float(data["strikes"][strike]["C"]["position"])
                    Pposition = float(data["strikes"][strike]["P"]["position"])

                    netPos = Cposition + Pposition

                    if netPos != 0:
                        # calc combinded greeks
                        if riskType == "delta":
                            risk = (
                                float(data["strikes"][strike]["C"][riskType]) * netPos
                            )
                        elif riskType in ["skew", "call", "put"]:
                            risk = (
                                float(data["strikes"][strike]["C"][riskType])
                                * netPos
                                * float(data["strikes"][strike]["C"]["vega"])
                            )
                        elif riskType == "position":
                            risk = netPos
                        else:
                            risk = (
                                float(data["strikes"][strike]["C"][riskType])
                                * mult
                                * netPos
                            )
                        risk = round(risk, 2)
                    else:
                        risk = 0
                    strikegreeks.append(risk)
                greeks.append(strikegreeks)

        return greeks, productset, strikeset
    elif relAbs == "bucket":
        for product in products:
            productset.append(product)
            data = conn.get(product.lower())
            if data:
                data = json.loads(data)
                strikegreeks = []
                # go over strikes and uppack greeks
                strikeset = []


def newstrikeRisk(portfolio, riskType, relAbs):
    # pull list of porducts from static data
    static = conn.get("staticData")
    df = pd.read_json(static)

    products = df[df["portfolio"] == portfolio]["product"].values

    # setup greeks and products bucket to collect data
    greeks = []
    productset = []
    strikegreeks = []
    if relAbs == "strike":
        strikeset = []
        # for each product collect greek per strike
        for product in products:
            data = conn.get(product.lower())

            if data:
                data = json.loads(data)
                strikegreeks = []
                # go over strikes and uppack greeks

                # turn strikes into DF
                # df= pd.Panel(data["strikes"]).to_frame().reset_index()
                df = (
                    pd.Panel.from_dict(data["strikes"], orient="minor")
                    .to_frame()
                    .reset_index()
                )
                # check if postion is empty for calls nad puts
                call = all(df[df["major"] == "position"]["C"].astype(float).values == 0)
                put = all(df[df["major"] == "position"]["P"].astype(float).values == 0)
                # if no position then skip the product
                if call and put:
                    continue
                else:
                    # add prodcut to list
                    productset.append(product)

                    # add net pos column
                    netPos = (
                        df[df["major"] == "position"]["C"]
                        .astype(float)
                        .add(df[df["major"] == "position"]["P"].astype(float))
                    ).values

                    # get mult
                    mult = float(data["mult"])

                    # convert just columns "C" and "P"
                    df[["C", "P"]] = df[["C", "P"]].apply(pd.to_numeric)

                    # calculate required risktype
                    if riskType == "delta":
                        risk = (
                            df[df["major"] == riskType]["C"].values
                            * df[df["major"] == "position"]["C"].values
                        ) + (
                            df[df["major"] == "delta"]["P"].values
                            * df[df["major"] == "position"]["P"].values
                        )
                        strikeset = df[df["major"] == "riskType"]["minor"].values

                    elif riskType == "fullDelta":
                        risk = (
                            df[df["major"] == riskType]["C"].values
                            * df[df["major"] == "fullDelta"]["C"].values
                        ) + (
                            df[df["major"] == "fullDelta"]["P"].values
                            * df[df["major"] == "fullDelta"]["P"].values
                        )
                        strikeset = df[df["major"] == riskType]["minor"].values

                    elif riskType == "position":
                        risk = netPos
                        strikeset = df[df["major"] == "position"]["minor"].values

                    elif riskType in ["skew", "call", "put"]:
                        risk = (
                            df[df["major"] == riskType]["C"].values
                            * netPos
                            * df[df["major"] == "vega"]["C"].values
                            * mult
                        )
                        strikeset = df[df["major"] == riskType]["minor"].values
                    elif riskType in ["gamma"]:
                        risk = df[df["major"] == riskType]["C"].values * netPos
                        strikeset = df[df["major"] == riskType]["minor"].values
                    else:
                        risk = df[df["major"] == riskType]["C"].values * netPos * mult
                        strikeset = df[df["major"] == riskType]["minor"].values
                    risk = list(np.around(np.array(risk), 2))

                # combine risk with other products
                greeks.append(risk)

        return greeks, productset, list(strikeset)


def timeStamp():
    now = datetime.now()
    now.strftime("%Y-%m-%d %H:%M:%S")
    return now


def sendMessage(text, user, messageTime):
    message = {"user": user, "message": text, "prority": "open"}

    # pull current messages if not return empty dict
    oldMessages = conn.get("messages")
    if oldMessages:
        oldMessages = json.loads(oldMessages)
    else:
        oldMessages = {}
    # combine old and new messages
    oldMessages[str(messageTime)] = message

    oldMessages = json.dumps(oldMessages)
    conn.set("messages", oldMessages)


def pullMessages():
    # get messages from redis
    messages = conn.get("messages")
    # if messages unpack and send back
    if messages:
        # messages = pd.read_json(messages)
        messages = json.loads(messages)

        return messages


# takes SD and gives vola
def volCalc(a, atm, skew, call, put, cMax, pMax):
    vol = atm + (a * skew)
    if a < 0:
        kurt = a * a * call
        vol = vol + kurt
        vol = min(vol, cMax)
    elif a > 0:
        kurt = a * a * put
        vol = vol + kurt
        vol = min(vol, pMax)

    return round(vol * 100, 2)


def sumbitVolas(product, data, user, dev_keys=False):
    # send new data to redis
    timestamp = datetime.utcnow()
    dict = json.dumps(data)
    if dev_keys:
        conn.set(product + "Vola" + ":dev", dict)
    else:
        conn.set(product + "Vola", dict)
    # inform options engine about update
    pic_data = pickle.dumps([product, "update"])
    conn.publish("compute", pic_data)

    engine = PostGresEngine()

    with sqlalchemy.orm.Session(engine) as session:
        HistoricalVolParams.metadata.create_all(engine)
        session.add(
            HistoricalVolParams(
                datetime=timestamp,
                product=product.upper(),
                vol_model="delta_spline_wing",
                spread=data["spread"],
                var1=data["vola"],
                var2=data["10 delta"],
                var3=data["25 delta"],
                var4=data["75 delta"],
                var5=data["90 delta"],
                ref=data["ref"],
                saved_by=user,
            )
        )
        session.commit()


# copy of function above with minor changes specific to volMatrix page
# will be replaced when all data moved to ORM either way
def sumbitVolasLME(product, data, user, index, dev_keys=False):
    # send new data to redis
    timestamp = datetime.utcnow()
    dict = json.dumps(data)
    if dev_keys:
        conn.set(product + "Vola" + ":dev", dict)
    else:
        conn.set(product + "Vola", dict)
    # inform options engine about update
    if index == 0:
        pic_data = json.dumps([product, "staticdata"])
        conn.publish("compute", pic_data)
    else:
        pic_data = json.dumps([product, "update"])
        conn.publish("compute", pic_data)

    engine = PostGresEngine()

    with sqlalchemy.orm.Session(engine) as session:
        HistoricalVolParams.metadata.create_all(engine)
        session.add(
            HistoricalVolParams(
                datetime=timestamp,
                product=product.upper(),
                vol_model="delta_spline_wing",
                spread=data["spread"],
                var1=data["vola"],
                var2=data["10 delta"],
                var3=data["25 delta"],
                var4=data["75 delta"],
                var5=data["90 delta"],
                ref=data["ref"],
                saved_by=user,
            )
        )
        session.commit()


def expiryProcess(product, ref):
    ##inputs to be entered from the page
    now = datetime.now().strftime("%Y-%m-%d")

    # load positions for product
    positions = conn.get("positions")
    positions = pickle.loads(positions)
    pos = pd.DataFrame.from_dict(positions)

    # filter for just the month we are looking at
    pos = pos[pos["instrument"].str[:6].isin([product.upper()])]
    pos = pos[pos["quanitity"] != 0]

    # new data frame with split value columns
    split = pos["instrument"].str.split(" ", n=2, expand=True)

    # making separate first name column from new data frame
    pos["strike"] = split[1]
    pos["optionTypeId"] = split[2]

    # drop futures
    pos = pos[pos["optionTypeId"].isin(["C", "P"])]

    # convert strike to float
    pos["strike"] = pos["strike"].astype(float)

    # remove partials
    posPartial = pos[pos["strike"] == ref]
    posPartial["action"] = "Partial"
    # reverse qty so it takes position out
    posPartial["quanitity"] = posPartial["quanitity"] * -1
    posPartial["price"] = 0

    # seperate into calls and puts
    posC = pos[pos["optionTypeId"] == "C"]
    posP = pos[pos["optionTypeId"] == "P"]

    # seperate into ITM and OTM
    posIC = posC[posC["strike"] < ref]
    posOC = posC[posC["strike"] > ref]
    posIP = posP[posP["strike"] > ref]
    posOP = posP[posP["strike"] < ref]

    # Create Df for out only
    out = pd.concat([posOC, posOP])
    out["action"] = "Abandon"

    # reverse qty so it takes position out
    out["quanitity"] = out["quanitity"] * -1

    # set price to Zero
    out["price"] = 0

    # go find 3month for underling
    staticData = loadStaticData()
    # staticData = pd.read_json(staticData)
    thirdWed = staticData[staticData["product"] == product]
    thirdWed = thirdWed["third_wed"].values[0]
    thirdWed = datetime.strptime(thirdWed, "%d/%m/%Y").strftime("%Y-%m-%d")

    # build future name
    futureName = product[:3] + " " + thirdWed

    # build expiry futures trade df
    futC = posIC.reset_index(drop=True)
    futC["instrument"] = futureName
    futC["prompt"] = thirdWed
    futC["action"] = "Exercise Future"
    futC["price"] = futC["strike"]
    futC["strike"] = None
    futC["optionTypeId"] = None

    futP = posIP.reset_index(drop=True)
    futP["instrument"] = futureName
    futP["prompt"] = thirdWed
    futP["quanitity"] = futP["quanitity"] * -1
    futP["action"] = "Exercise Future"
    futP["price"] = futP["strike"]
    futP["strike"] = None
    futP["optionTypeId"] = None

    # build conteracting options position df
    posIP["quanitity"] = posIP["quanitity"].values * -1
    posIC["quanitity"] = posIC["quanitity"].values * -1
    posIP["action"] = "Exercised"
    posIC["action"] = "Exercised"
    posIP["price"] = 0
    posIC["price"] = 0

    # pull it all together
    all = out.append([futC, futP, posIP, posIC, posPartial])

    # add trading venue
    all["tradingVenue"] = "Exercise Process"

    # add trading time
    all["tradeDate"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    # drop the columns we dont need
    all.drop(
        ["delta", "index", "settlePrice", "index", "ID", "dateTime"],
        axis=1,
        inplace=True,
        errors="ignore",
    )

    return all


def expiryProcessEUR(product, ref):
    ##inputs to be entered from the page
    now = datetime.now().strftime("%Y-%m-%d")

    # load positions for product
    positions = conn.get("positions")
    positions = pickle.loads(positions)
    pos = pd.DataFrame.from_dict(positions)

    # filter for euronext
    pos = pos[pos["instrument"].str[:1] == "X"]

    # set option and future names
    option_name = product[:25].lower()
    with Session() as session:
        future_name = (
            session.query(upestatic.Option.underlying_future_symbol)
            .filter(upestatic.Option.symbol == option_name)
            .first()
        )[0].upper()

    # filter for just the month we are looking at
    pos = pos[pos["instrument"].str[:25].isin([product.upper()])]
    pos = pos[pos["quanitity"] != 0]

    # new data frame with split value columns
    pos["info"] = pos["instrument"].str.split(" ", n=3, expand=True)[3]
    pos[["_", "strike", "optionTypeId"]] = pos["info"].str.split("-", expand=True)

    # drop futures - no need as filtering for month already filtered out futures
    # pos = pos[pos["optionTypeId"].isin(["C", "P"])]

    # convert strike to float
    pos["strike"] = pos["strike"].astype(float)

    # remove partials
    posPartial = pos[pos["strike"] == ref]
    posPartial["action"] = "Partial"

    # reverse qty so it takes position out
    posPartial["quanitity"] = posPartial["quanitity"] * -1
    posPartial["price"] = 0

    # seperate into calls and puts
    posC = pos[pos["optionTypeId"] == "C"]
    posP = pos[pos["optionTypeId"] == "P"]

    # seperate into ITM and OTM
    posIC = posC[posC["strike"] < ref]
    posOC = posC[posC["strike"] > ref]
    posIP = posP[posP["strike"] > ref]
    posOP = posP[posP["strike"] < ref]

    # Create Df for out only
    out = pd.concat([posOC, posOP])
    out["action"] = "Abandon"

    # reverse qty so it takes position out
    out["quanitity"] = out["quanitity"] * -1

    # set price to Zero
    out["price"] = 0

    # build expiry futures trade df
    futC = posIC.reset_index(drop=True)
    futC["instrument"] = future_name
    # futC["prompt"] = thirdWed
    futC["action"] = "Exercise Future"
    futC["price"] = futC["strike"]
    futC["strike"] = None
    futC["optionTypeId"] = None

    futP = posIP.reset_index(drop=True)
    futP["instrument"] = future_name
    # futP["prompt"] = thirdWed
    futP["quanitity"] = futP["quanitity"] * -1
    futP["action"] = "Exercise Future"
    futP["price"] = futP["strike"]
    futP["strike"] = None
    futP["optionTypeId"] = None

    # build conteracting options position df
    posIP["quanitity"] = posIP["quanitity"].values * -1
    posIC["quanitity"] = posIC["quanitity"].values * -1
    posIP["action"] = "Exercised"
    posIC["action"] = "Exercised"
    posIP["price"] = 0
    posIC["price"] = 0

    # pull it all together
    all = out.append([futC, futP, posIP, posIC, posPartial])

    # add trading venue
    all["tradingVenue"] = "Exercise Process"

    # add trading time
    all["tradeDate"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    # drop the columns we dont need
    all.drop(
        ["delta", "index", "settlePrice", "index", "ID", "dateTime"],
        axis=1,
        inplace=True,
        errors="ignore",
    )

    return all


def pullCurrent3m():
    date = conn.get("3m")
    date = pickle.loads(date)

    return date


def recRJO(exchange: str):
    # fetch georgia positions
    data = conn.get("positions")
    data = pickle.loads(data)
    georgia_pos = pd.DataFrame(data)

    # filter for desired exchange
    if exchange == "LME":
        georgia_pos = georgia_pos[georgia_pos["instrument"].str[:1] != "X"]
    elif exchange == "EURONEXT":
        georgia_pos = georgia_pos[georgia_pos["instrument"].str[0:4] == "XEXT"]
    georgia_pos.set_index("instrument", inplace=True)

    # fetch rjo lme positions
    (rjo_pos_df, latest_rjo_filename) = sftp_utils.fetch_latest_rjo_export(
        "UPETRADING_csvnpos_npos_%Y%m%d.csv"
    )
    # remove CME positions and duplicates
    rjo_pos_df = rjo_pos_df[rjo_pos_df["Record Code"] == "P"]
    rjo_pos_df = rjo_pos_df[rjo_pos_df["Bloomberg Exch Code"].isin(["LME", "EOP"])]

    if exchange == "LME":
        rjo_pos_df = rjo_pos_df[rjo_pos_df["Bloomberg Exch Code"] == "LME"]
    elif exchange == "EURONEXT":
        rjo_pos_df = rjo_pos_df[rjo_pos_df["Bloomberg Exch Code"] == "EOP"]

    rjo_pos_df.columns = rjo_pos_df.columns.str.replace(" ", "")
    rjo_pos_df.columns = rjo_pos_df.columns.str.lower()

    rjo_pos_df["quanitity"] = rjo_pos_df.apply(multiply_rjo_positions, axis=1)
    rjo_pos_df["instrument"] = rjo_pos_df.apply(build_georgia_symbol_from_rjo, axis=1)
    rjo_pos_df.set_index("instrument", inplace=True)
    rjo_pos_df = rjo_pos_df[["quanitity"]]
    rjo_pos_df = rjo_pos_df.groupby(["instrument"], as_index=True).agg(
        {"quanitity": "sum"}
    )

    # merge RJO and UPE position on index(instrument)
    combinded = rjo_pos_df[["quanitity"]].merge(
        georgia_pos[["quanitity"]],
        how="outer",
        left_index=True,
        right_index=True,
        suffixes=("_RJO", "_UPE"),
    )
    combinded.fillna(0, inplace=True)

    # calc diff
    combinded["diff"] = combinded["quanitity_RJO"] - combinded["quanitity_UPE"]

    # return only rows with a non 0 diff
    combinded = combinded[combinded["diff"] != 0]

    return combinded, latest_rjo_filename


def build_georgia_symbol_from_rjo(rjo_row: pd.Series) -> str:
    is_option = True if rjo_row["securitysubtypecode"] in ["C", "P"] else False
    # if euronext
    if rjo_row["bloombergexchcode"] == "EOP":
        # this euronext rec is a bit of a mess, but that is what happens when
        # we choose the most verbose instrument name possible.
        # update this when our internal naming conventions change

        # the try except puts all foreign symbols into an ERROR bucket and logs them

        exchange = "XEXT-EBM-EUR"
        if is_option:
            try:
                # from format: CALL SEP 23 MTF MILL WHT 26000
                # to format: XEXT-EBM-EUR O 23-08-15 A-275-C
                type, month, year, MTF, product = rjo_row["securitydescline1"].split(
                    " "
                )[0:5]
                strike = str(int(rjo_row["optionstrikeprice"]))
                type = "C" if type == "CALL" else "P"
                month = str(int(monthsNumber[month.lower()]) - 1)
                month = "0" + month if len(month) == 1 else month
                day = EUoptionsDict[str(rjo_row["contractmonth"])]
                option = (
                    exchange
                    + " O "
                    + year
                    + "-"
                    + month
                    + "-"
                    + day
                    + " A-"
                    + strike
                    + "-"
                    + type
                )
            except:
                print(
                    "unexpected error occured for instrument: "
                    + rjo_row["securitydescline1"]
                )
                return "ERROR"

            return option
        else:
            try:
                # from format: SEP 23 MTF MILL WHT
                # to format: XEXT-EBM-EUR F 23-12-11
                month, year, MTF, product = rjo_row["securitydescline1"].split(" ")[0:4]
                month = monthsNumber[month.lower()]
                day = EUfuturesDict[str(rjo_row["contractmonth"])]
                future = exchange + " F " + year + "-" + month + "-" + day
            except:
                print(
                    "unexpected error occured for instrument: "
                    + rjo_row["securitydescline1"]
                )
                return "ERROR"
            return future
    else:  # if LME
        if is_option:
            try:
                # format: CALL DEC 23 LME COPPER US 9500
                type, month, year, LME, product = rjo_row["securitydescline1"].split(
                    " "
                )[0:5]

                strike = int(rjo_row["optionstrikeprice"])
                type = "C" if type == "CALL" else "P"
                product = (
                    productCodes[product]
                    + "O"
                    + monthCode[month.lower()].upper()
                    + year[1]
                )

                option = product + " " + str(strike) + " " + type.upper()
            except:
                print(
                    "unexpected error occured for instrument: "
                    + rjo_row["securitydescline1"]
                )
                return "ERROR"
            return option
        else:
            # format: 17 MAY 23 LME LEAD US
            try:
                day, month, year, LME, product = rjo_row["securitydescline1"].split(
                    " "
                )[0:5]
                future = (
                    productCodes[product]
                    + " 20"
                    + year
                    + "-"
                    + monthsNumber[month.lower()]
                    + "-"
                    + day
                )
            except:
                print(
                    "unexpected error occured for instrument: "
                    + rjo_row["securitydescline1"]
                )
                return "ERROR"
            return future


# get expiry day from contract month for euronext. replace when naming convention changes
EUfuturesDict = {
    "202303": "10",
    "202305": "10",
    "202309": "11",
    "202312": "11",
    "202403": "11",
    "202405": "10",
    "202409": "10",
    "202412": "10",
    "202503": "10",
    "202505": "12",
    "202509": "10",
    "202512": "10",
}

EUoptionsDict = {
    "202303": "15",
    "202305": "17",
    "202309": "15",
    "202312": "15",
    "202403": "15",
    "202405": "15",
    "202409": "15",
    "202412": "15",
    "202503": "17",
    "202505": "15",
    "202509": "15",
    "202512": "17",
}


monthsNumber = {
    "jan": "01",
    "feb": "02",
    "mar": "03",
    "apr": "04",
    "may": "05",
    "jun": "06",
    "jul": "07",
    "aug": "08",
    "sep": "09",
    "oct": "10",
    "nov": "11",
    "dec": "12",
}

monthCode = {
    "jan": "f",
    "feb": "g",
    "mar": "h",
    "apr": "j",
    "may": "k",
    "jun": "m",
    "jul": "n",
    "aug": "q",
    "sep": "u",
    "oct": "v",
    "nov": "x",
    "dec": "z",
}

productCodes = {
    "ALUM": "LAD",
    "LEAD": "PBD",
    "ZINC": "LZH",
    "COPPER": "LCU",
    "NICKEL": "LND",
}


# this function is NOT ready for when LME is added to static data
def sendEURVolsToPostgres(df, date):
    with Session() as session:
        # check if date is already in table
        dates = (
            session.query(upestatic.SettlementVol.settlement_date)
            .where(upestatic.SettlementVol.settlement_date == date)
            # this is where youll need to add the exchange filter for xext / xlme
            .distinct()
            .all()
        )
        datePresent = True if dates else False

        if datePresent:
            # delete all rows where date == df["date"].iloc[0]]
            session.query(upestatic.SettlementVol).filter(
                upestatic.SettlementVol.settlement_date == date
            ).delete()
            session.commit()
        df.to_sql("settlement_vols", engine, if_exists="append", index=False)

    return


def filter_trade_rec_df(rec_df: pd.DataFrame, days_to_rec) -> pd.DataFrame:
    """Cleans and filters trade reconciliation dataframe to only include trades
    from within the last three days, in USD, and with the `Type == OT`, cleans
    column names by lowering their cases and removing all instances of the space
    character.

    :param rec_df: Trade reconciliation dataframe
    :type rec_df: pd.DataFrame
    :return: Filtered and cleaned trade reconciliation dataframe
    :rtype: pd.DataFrame
    """
    today_date = date.today()
    today_date = datetime(today_date.year, today_date.month, today_date.day)
    rec_df.columns = rec_df.columns.str.replace(" ", "")
    rec_df.columns = rec_df.columns.str.lower()
    rec_df["trdate"] = rec_df["trdate"].apply(lambda date_str: str(date_str).lower())
    rec_df["type"] = rec_df["type"].apply(lambda entry: entry.lower().replace(" ", ""))

    rec_df = rec_df[rec_df.type == "ot"]
    rec_df["trdate"] = rec_df.apply(
        lambda row: datetime.strptime(row["trdate"], r"%d-%b-%y"), axis=1
    )
    rec_df["ccy"] = rec_df["ccy"].apply(lambda entry: entry.lower().replace(" ", ""))
    rec_df = rec_df[
        (
            (timedelta(days=0.0) < today_date - rec_df["trdate"])
            & (today_date - rec_df["trdate"] <= timedelta(days=days_to_rec))
        )
        & (rec_df.ccy == "usd")
    ]
    rec_df["contracttype"] = rec_df["contracttype"].apply(
        lambda entry: str(entry).lower().replace(" ", "")
    )
    rec_df["exchangeid"] = rec_df["exchangeid"].apply(
        lambda entry: entry.lower().replace(" ", "")
    )
    # rec_df["lotssigned"] = rec_df["lotssigned"].apply(
    #     lambda entry: entry.lower().replace(" ", "")
    # )
    rec_df["strike"] = rec_df["strike"].apply(
        lambda entry: entry.lower().replace(" ", "")
    )
    rec_df["roll/deliverydate"] = rec_df["roll/deliverydate"].apply(
        lambda entry: str(entry).lower().replace(" ", "")
    )
    print(rec_df)
    return rec_df


def match_rec_trades_to_georgia_trades(rec_row, georgia_trades_df: pd.DataFrame):
    matched_georgia_trades = georgia_trades_df[
        (georgia_trades_df["quanitity"] == rec_row.lotssigned)
        & (georgia_trades_df["instrument"] == rec_row.georgia_name)
        & (georgia_trades_df["price"] == rec_row.price)
    ]
    return matched_georgia_trades


def rec_sol3_cme_pos_bgm_mir_14(
    sol3_pos_df: pd.DataFrame, bgm_mir_14: pd.DataFrame
) -> pd.DataFrame:
    bgm_mir_14.columns = bgm_mir_14.columns.str.replace(" ", "")
    bgm_mir_14.columns = bgm_mir_14.columns.str.lower()
    df_obj = bgm_mir_14.select_dtypes(["object"])
    bgm_mir_14[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
    bgm_mir_14 = bgm_mir_14[bgm_mir_14["postype"] != "EOF"]
    print(bgm_mir_14[["exchangeid"]])
    bgm_mir_14["exchangeid"] = bgm_mir_14["exchangeid"].apply(
        lambda entry: entry.lower().replace(" ", "")
    )
    bgm_mir_14["instrument"] = bgm_mir_14.apply(
        build_sol3_symbol_from_bgm_mir_14, axis=1
    )
    bgm_mir_14.rename(columns={"nett": "pos"}, inplace=True)
    sol3_pos_df.rename(
        columns={"Pos Net": "pos", "Ctr Unique Str": "instrument"}, inplace=True
    )
    bgm_mir_14.set_index("instrument", inplace=True)
    sol3_pos_df.set_index("instrument", inplace=True)

    combined_df = bgm_mir_14[["pos"]].merge(
        sol3_pos_df[["pos"]],
        how="outer",
        left_index=True,
        right_index=True,
        suffixes=("_bgm", "_sol3"),
    )
    combined_df.fillna(0, inplace=True)
    combined_df["diff"] = combined_df["pos_bgm"] - combined_df["pos_sol3"]
    combined_df = combined_df.reset_index()
    return combined_df[combined_df["diff"] != 0]


def rec_sol3_rjo_cme_pos(
    sol3_pos_df: pd.DataFrame, rjo_pos_df: pd.DataFrame
) -> pd.DataFrame:
    rjo_pos_df.columns = rjo_pos_df.columns.str.replace(" ", "")
    rjo_pos_df.columns = rjo_pos_df.columns.str.lower()
    df_obj = rjo_pos_df.select_dtypes(["object"])
    rjo_pos_df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
    rjo_pos_df = rjo_pos_df[rjo_pos_df["recordcode"] == "R"]

    # aggregate data to match sol3 instrument column
    rjo_pos_df["instrument"] = rjo_pos_df.apply(build_sol3_symbol_from_rjo, axis=1)
    rjo_pos_df["pos"] = rjo_pos_df.apply(multiply_rjo_positions, axis=1)

    rjo_pos_df = rjo_pos_df[["instrument", "pos"]]
    # collapse positions into one row per instrument to match sol3
    rjo_pos_df = rjo_pos_df.groupby(["instrument"], as_index=False).agg({"pos": "sum"})

    sol3_pos_df.rename(
        columns={"Pos Net": "pos", "Ctr Unique Str": "instrument"}, inplace=True
    )
    rjo_pos_df.set_index("instrument", inplace=True)
    sol3_pos_df.set_index("instrument", inplace=True)

    combined_df = rjo_pos_df[["pos"]].merge(
        sol3_pos_df[["pos"]],
        how="outer",
        left_index=True,
        right_index=True,
        suffixes=("_rjo", "_sol3"),
    )
    combined_df.fillna(0, inplace=True)
    combined_df["diff"] = combined_df["pos_rjo"] - combined_df["pos_sol3"]
    combined_df = combined_df.reset_index()

    return combined_df[combined_df["diff"] != 0]


def multiply_rjo_positions(rjo_row: pd.Series) -> int:
    pos = rjo_row["quantity"]
    if rjo_row["buysellcode"] == 2:
        pos = pos * -1
    return pos


rjo_to_sol3_hash = {
    # RJO futures and options share a code,
    # so this mapping is all options, futures will use extra logic
    # in build_sol3_symbol_from_rjo() to override the matching here
    "AL": "AX",  # Ali options
    "HG": "HXE",  # Copper options
    "37": "OG",  # Gold options
    "39": "SO",  # Silver options
    "BG": "H1M",  # weekly copper mon
    "BH": "H2M",
    "BI": "H3M",
    "BJ": "H4M",
    "BK": "H5M",
    "BL": "H1W",  # weekly copper weds
    "BM": "H2W",
    "BN": "H3W",
    "BO": "H4W",
    "BP": "H5W",
    "A>": "H1E",  # weekly copper fri
    "A?": "H2E",
    "A:": "H3E",
    "A#": "H4E",
    "A@": "H5E",
}


def build_sol3_symbol_from_rjo(rjo_row: pd.Series) -> str:
    sol3_symbol = "XCME "
    is_option = True if rjo_row["securitysubtypecode"] in ["C", "P"] else False
    sol3_symbol += "OPT " if is_option else "FUT "
    # use dictionary to map option symbols, then the remaining futures
    if is_option:
        sol3_symbol += rjo_to_sol3_hash[rjo_row["contractcode"]]
    else:
        # manual override for cme futures as sol3 has different opt/fut symbols
        if rjo_row["contractcode"] == "AL":
            sol3_symbol += "ALI"  # aluminum futures
        elif rjo_row["contractcode"] == "37":
            sol3_symbol += "GC"  # gold futures
        elif rjo_row["contractcode"] == "39":
            sol3_symbol += "SI"  # silver futures
        elif rjo_row["contractcode"] == "HG":
            sol3_symbol += "HG"  # copper futures
    # date rearrangings
    date = (
        " "
        + str(rjo_row["contractmonth"])[-2:]
        + " "
        + str(rjo_row["contractmonth"])[0:4]
    )
    sol3_symbol += date

    # futures code is built, options still need strike and type
    if is_option:
        if rjo_row["contractcode"] in ["AL", "37"]:
            sol3_symbol += (
                " "
                + str(float(rjo_row["optionstrikeprice"])).rstrip("0").rstrip(".")
                + " "
            )
        else:
            # Copper and Silver options need price multiplier correction
            sol3_symbol += (
                " "
                + str(float(rjo_row["optionstrikeprice"]) / 100).rstrip("0").rstrip(".")
                + " "
            )

        sol3_symbol += rjo_row["securitysubtypecode"]

    return sol3_symbol.upper()


def build_sol3_symbol_from_bgm_mir_14(bgm_mir_14_row: pd.Series) -> str:
    sol3_symbol = "XCME "
    is_option = bgm_mir_14_row["type"].upper() in ["CALL", "PUT"]
    sol3_symbol += "OPT " if is_option else "FUT "
    if (exchange_id := bgm_mir_14_row["exchangeid"].upper()) == "HX":
        sol3_symbol += "HXE"
    else:
        sol3_symbol += exchange_id
    contract_date = datetime.strptime(bgm_mir_14_row["delivery"].capitalize(), r"%b-%y")
    sol3_symbol += contract_date.strftime(r" %m %Y")
    if is_option:
        if bgm_mir_14_row["underlyingcode"].upper() == "HG":
            # BGM for some reason store prices and strikes for CME Copper
            # contracts in cents per pound instead of dollars, and arbitrarily
            # alter lot sizes to make total prices work, this conditional should
            # be removed the moment we move to a functional clearer.
            bgm_mir_14_row["strike"] = f"{float(bgm_mir_14_row['strike']) / 100.0:g}"
        sol3_symbol += (
            " "
            + bgm_mir_14_row["strike"]
            + " "
            + bgm_mir_14_row["contract"][-1].upper()
        )

    return sol3_symbol.upper()


def rec_britannia_mir13(britannia_mir_13_doc: pd.DataFrame):
    products = {"ah": "lad", "zs": "lzh", "pb": "pbd", "ca": "lcu", "ni": "lnd"}
    months = {
        "jan": "f",
        "feb": "g",
        "mar": "h",
        "apr": "j",
        "may": "k",
        "jun": "m",
        "jul": "n",
        "aug": "q",
        "sep": "u",
        "oct": "v",
        "nov": "x",
        "dec": "z",
    }

    def build_georgia_name(row) -> str:
        if row["contracttype"] == "fut":
            return "{0} {1}".format(
                lme_future_to_georgia(row["exchangeid"].upper()),
                datetime.strptime(row["roll/deliverydate"], r"%d-%b-%y").strftime(
                    r"%Y-%m-%d"
                ),
            )
        elif row["contracttype"] == "call" or row["contracttype"] == "put":
            return "{}o{}{} {} {}".format(
                products[row["exchangeid"]],
                months[row["roll/deliverydate"].split("-")[0].lower()],
                row["roll/deliverydate"].split("-")[1][1],
                row["strike"],
                row["contracttype"][0].lower(),
            )

    today_date = date.today()
    today_date = datetime(today_date.year, today_date.month, today_date.day)
    trade_table = pd.DataFrame(pickle.loads(conn.get("trades")))
    trade_table = trade_table.rename(columns=str.lower)
    days_to_rec = DAYS_TO_TRADE_REC
    # Has to be == False for pandas binary array operation to work, truly a modern
    # tragedy
    trade_table = trade_table[trade_table["deleted"] == False]
    britannia_mir_13 = filter_trade_rec_df(britannia_mir_13_doc, days_to_rec)
    britannia_mir_13["georgia_name"] = britannia_mir_13.apply(
        build_georgia_name, axis=1
    )
    trade_table = trade_table[
        (timedelta(days=0.0) < today_date - trade_table["datetime"])
        & (today_date - trade_table["datetime"] <= timedelta(days=days_to_rec))
    ]
    britannia_mir_13["non_unique_internal_matching_id"] = britannia_mir_13.apply(
        lambda row: "{0}:{1}:{2}:{3}".format(
            row["georgia_name"],
            row["b"],
            row.price,
            row.trdate.strftime(r"%Y-%m-%d"),
        ).lower(),
        axis=1,
    )
    trade_table["non_unique_internal_matching_id"] = trade_table.apply(
        lambda row: "{0}:{1}:{2}:{3}".format(
            row.instrument.lower(),
            "b" if row.quanitity > 0.0 else "s",
            row.price,
            row.datetime.strftime(r"%Y-%m-%d"),
        ).lower(),
        axis=1,
    )
    britannia_rec_series = britannia_mir_13.groupby(
        ["non_unique_internal_matching_id"]
    ).sum()["lotssigned"]
    georgia_rec_series = trade_table.groupby(["non_unique_internal_matching_id"]).sum()[
        "quanitity"
    ]

    trade_rec_diff_df = pd.merge(
        georgia_rec_series,
        britannia_rec_series,
        how="outer",
        left_index=True,
        right_index=True,
        suffixes=["_upe", "_bgm"],
    )
    trade_rec_diff_df.fillna(0, inplace=True)
    trade_rec_diff_df = trade_rec_diff_df.rename(
        columns={"quanitity": "UPE", "lotssigned": "BGM"}
    )
    trade_rec_diff_df["Break"] = trade_rec_diff_df["UPE"] - trade_rec_diff_df["BGM"]
    trade_rec_diff_df = trade_rec_diff_df.rename_axis("trade_identifier").reset_index()
    split_identifier_df = trade_rec_diff_df["trade_identifier"].str.split(
        ":", expand=True
    )
    trade_rec_diff_df = trade_rec_diff_df.merge(
        split_identifier_df, left_index=True, right_index=True
    )
    trade_rec_diff_df = trade_rec_diff_df[trade_rec_diff_df["Break"] != 0.0]
    trade_rec_diff_df = (
        trade_rec_diff_df.rename(
            columns={0: "Instrument", 1: "Buy/Sell", 2: "Price", 3: "Date"}
        )
        .sort_values(by=["Date", "Instrument", "Buy/Sell", "Price"])
        .set_index(["Date", "Instrument", "Buy/Sell"])
        .drop(columns=["trade_identifier"])
    )[["Price", "UPE", "BGM", "Break"]]
    trade_rec_diff_df = trade_rec_diff_df.reset_index()
    return trade_rec_diff_df


def lme_future_to_georgia(product):
    if product == "CA":
        return "LCU"
    elif product == "ZS":
        return "LZH"
    elif product == "AH":
        return "LAD"
    elif product == "PB":
        return "PBD"
    elif product == "NI":
        return "LND"


def codeToName(product):
    product = product[0:3]
    if product == "LCU":
        return "Copper"
    elif product == "LZH":
        return "Zinc"
    elif product == "LAD":
        return "Aluminium"
    elif product == "PBD":
        return "Lead"
    elif product == "LND":
        return "Nickel"


def codeToMonth(product):
    product = product[4]
    if product == "F":
        return "Jan"
    elif product == "G":
        return "Feb"
    elif product == "H":
        return "Mar"
    elif product == "J":
        return "Apr"
    elif product == "K":
        return "May"
    elif product == "M":
        return "Jun"
    elif product == "N":
        return "Jul"
    elif product == "Q":
        return "Aug"
    elif product == "U":
        return "Sep"
    elif product == "V":
        return "Oct"
    elif product == "X":
        return "Nov"
    elif product == "Z":
        return "Dec"


def onLoadPortfolio():
    try:
        staticData = loadStaticData()
    except:
        portfolios = [{"label": "error", "value": "error"}]
        return portfolios

    portfolios = []
    for portfolio in staticData.portfolio.unique():
        portfolios.append({"label": portfolio, "value": portfolio})
    return portfolios


def onLoadProduct():
    try:
        staticData = loadStaticData()
        products = []
        for product in set(staticData["product"]):
            products.append({"label": product, "value": product})
        return products
    except Exception as e:
        return {"label": "Error", "value": "Error"}


def onLoadProductMonths(product):
    # load staticdata
    try:
        staticData = loadStaticData()
    except:
        products = [{"label": "error", "value": "error"}]
        return products, products[0]["value"]

    # convert to shortname
    staticData = staticData.loc[staticData["f2_name"] == product]

    # sort data
    staticData["expiry"] = pd.to_datetime(staticData["expiry"], dayfirst=True)
    staticData = staticData.sort_values(by=["expiry"])

    # create month code from product code
    productNames = set()
    productNames = [x[4:] for x in staticData["product"]]

    products = []

    for product in productNames:
        products.append({"label": product, "value": product})
    products.append({"label": "3M", "value": "3M"})
    return products, products[0]["value"]


def georgiaLabel(label):
    return html.Label([label], style={"font-weight": "bold", "text-align": "left"})


def calculate_time_remaining(
    expiry_datetime: datetime,
    holiday_list: Optional[List[date]] = [],
    holiday_weight_list: Optional[List[float]] = [],
    weekmask=[1, 1, 1, 1, 1, 1, 1],  # calc_option: Option,
    _eval_date: Optional[datetime] = None,
    _apply_time_corrections=False,
) -> Tuple[float, float]:
    """Backend utility staticmethod that calculates the number of business days
    until expiry of a product as a fraction of its respective business year,
    applying conditional logic, such as weekend decay, and holidays.

    :param expiry_datetime: Expiry datetime of the option
    :type expiry_datetime: datetime
    :param holiday_list: List of holiday datetimes, defaults to []
    :type holiday_list: Optional[List[datetime]], optional
    :param holiday_weight_list: Corresponding list of weights of each holiday,
    defaults to []
    :type holiday_weight_list: Optional[List[float]], optional
    :param weekmask: [M,Tu,W,Th,F,Sa,Su], defaults to [1, 1, 1, 1, 1, 1, 1]
    :type weekmask: list, optional
    :param _eval_date: Optional date to evaluate from, required for risk matrices,
    defaults to None
    :type _eval_date: Optional[datetime], optional
    :param _apply_time_corrections: Optional flag to enable time corrections in
    calculation, requires time information to be carried in expiry data
    :type _apply_time_corrections: bool, defaults to False
    :return: Floating point value representing the fraction of the option's
    "decay-year" left until expiry
    :rtype: Tuple[float, float]
    """
    eval_date = (
        datetime.now(tz=zoneinfo.ZoneInfo("UTC")) if _eval_date is None else _eval_date
    )

    if expiry_datetime.tzinfo is None:
        expiry_datetime = expiry_datetime.replace(tzinfo=zoneinfo.ZoneInfo("UTC"))
    if eval_date.tzinfo is None:
        eval_date = eval_date.replace(tzinfo=zoneinfo.ZoneInfo("UTC"))

    curr_year_start_date = datetime(eval_date.year, 1, 1)
    curr_year_end_date = datetime(eval_date.year + 1, 1, 1)
    np_curr_year_start_date = np.datetime64(curr_year_start_date, "D")
    np_curr_year_end_date = np.datetime64(curr_year_end_date, "D")
    np_expiry_date = np.datetime64(expiry_datetime, "D")
    np_eval_date = np.datetime64(eval_date, "D")

    holiday_array = np.array(holiday_list, dtype="datetime64[D]")

    partial_holiday_correction_to_expiry = 0.0
    partial_holiday_correction_this_yr = 0.0

    for holiday_weight, holiday_date in zip(
        holiday_weight_list, holiday_array.astype("datetime64[D]")
    ):
        holiday_date: np.datetime64
        holiday_weight: float
        if eval_date.date() <= holiday_date <= expiry_datetime.date():
            # This will correct for any partial holidays that fall within
            # the option's lifetime
            partial_holiday_correction_to_expiry += 1.0 - holiday_weight

        if curr_year_start_date.date() <= holiday_date <= curr_year_end_date.date():
            # and this will do the same for those that fall within the current calendar
            # year
            partial_holiday_correction_this_yr += 1.0 - holiday_weight

    time_correction = 0.0
    if _apply_time_corrections:
        time_correction = (
            expiry_datetime
            - datetime.combine(expiry_datetime.date(), eval_date.timetz())
        ) / timedelta(days=1)

    days_to_expiry = (
        _get_busdays_to_expiry(
            np_expiry_date,
            np_eval_date,
            weekmask=weekmask,
            holidays=holiday_array,
        )
        + partial_holiday_correction_to_expiry
        + time_correction
    )

    # This has to be done to account for business years as well as calendar
    # years
    day_forward_year_days = (
        np.busday_count(
            np_curr_year_start_date,
            np_curr_year_end_date,
            weekmask=weekmask,
            holidays=holiday_array,
        )
        + partial_holiday_correction_this_yr
    )
    return days_to_expiry / day_forward_year_days, day_forward_year_days


def _get_busdays_to_expiry(
    expiry_datetimes: np.ndarray,
    evaluation_datetimes: np.ndarray,
    weekmask: List[int] = [1, 1, 1, 1, 1, 1, 1],
    holidays: np.ndarray = np.array([]),
) -> np.ndarray:
    if np.array(holidays).size != 0:
        holidays = holidays.astype("datetime64[D]")
        busday_diff = np.busday_count(
            evaluation_datetimes.astype("datetime64[D]"),
            expiry_datetimes.astype("datetime64[D]"),
            weekmask=weekmask,
            holidays=holidays,
        )
    else:
        busday_diff = np.busday_count(
            evaluation_datetimes.astype("datetime64[D]"),
            expiry_datetimes.astype("datetime64[D]"),
            weekmask=weekmask,
        )
    return busday_diff


MONTH_CODE_TO_MONTH_NUM = {
    "f": 1,
    "g": 2,
    "h": 3,
    "j": 4,
    "k": 5,
    "m": 6,
    "n": 7,
    "q": 8,
    "u": 9,
    "v": 10,
    "x": 11,
    "z": 12,
}


class SymbolStandardError(Exception):
    pass


def convert_georgia_option_symbol_to_expiry(georgia_option_symbol: str) -> datetime:
    split_option_symbol = georgia_option_symbol.split(" ")
    if len(split_option_symbol) > 1:
        # means we have a full option symbol, we only want the first bit
        georgia_option_symbol = split_option_symbol[0]
        if georgia_option_symbol in list(
            GEORGIA_LME_SYMBOL_VERSION_OLD_NEW_MAP.values()
        ):
            raise SymbolStandardError(
                "New standard symbol {} passed to old standard converter".format(
                    georgia_option_symbol
                )
            )

    option_symbol_date_data = georgia_option_symbol[-2:].lower()
    preliminary_date = datetime(
        int(f"202{option_symbol_date_data[1]}"),
        MONTH_CODE_TO_MONTH_NUM[option_symbol_date_data[0]],
        1,
        11,
        15,
        # tzinfo=zoneinfo.ZoneInfo("Europe/London"),
    )
    first_wednesday = preliminary_date + relativedelta.relativedelta(
        days=((2 - preliminary_date.weekday() + 7) % 7 + 14)
    )
    return first_wednesday


def get_product_holidays(product_symbol: str, _session=None) -> List[date]:
    """Fetches and returns all FULL holidays associated with a given
    product, ignoring partially weighted holidays

    :param product_symbol: Georgia new symbol for `Product`
    :type product_symbol: str
    :return: List of dates associated with full holidays for the given
    product
    :rtype: List[date]
    """
    product_symbol = product_symbol.lower()
    with Session() as session:
        product: upestatic.Product = session.get(upestatic.Product, product_symbol)
        if product is None and _session is None:
            # print(
            #     f"`get_product_holidays(...)` in parts.py was supplied with "
            #     f"an old format symbol: {product_symbol}\nbloody migrate "
            #     f"whatever's calling this!"
            # )
            return get_product_holidays(
                GEORGIA_LME_SYMBOL_VERSION_OLD_NEW_MAP[product_symbol.lower()],
                _session=session,
            )
        elif product is None and _session is not None:
            raise KeyError(
                f"Failed to find product: {product_symbol} in new static data"
            )

        valid_holiday_dates = []
        for holiday in product.holidays:
            if holiday.holiday_weight == 1.0:
                valid_holiday_dates.append(holiday.holiday_date)

    return valid_holiday_dates


def get_first_wednesday(year, month):
    d = date(year, month, 1)
    while d.weekday() != 2:
        d += timedelta(1)
    return d


# build new symbol from old symbol for static data migration
def build_new_lme_symbol_from_old(old_symbol: str) -> str:
    """
    format:
    opt: lcuoz3 8400 c -> xlme-lcu-usd o 23-12-06 a-8400-c
    fut: lcu 2023-11-15 -> xlme-lcu-usd f 23-12-06
    """
    LME_SYMBOL_MAP = {
        "lad": "xlme-lad-usd",
        "lcu": "xlme-lcu-usd",
        "lnd": "xlme-lnd-usd",
        "pbd": "xlme-pbd-usd",
        "lzh": "xlme-lzh-usd",
    }

    LETTER_TO_MONTH = {
        "f": 1,
        "g": 2,
        "h": 3,
        "j": 4,
        "k": 5,
        "m": 6,
        "n": 7,
        "q": 8,
        "u": 9,
        "v": 10,
        "x": 11,
        "z": 12,
    }

    # check if euronext, if so, return old symbol
    if old_symbol[:4].lower() == "xext":
        return old_symbol

    # check if appears to be in new format
    # validate, if not valid, return error
    # else, return new format
    if len(old_symbol.split(" ")[0].split("-")) > 1:
        # check if date correctly formatted
        try:
            # check date
            date = old_symbol.split(" ")[2]
            datetime.strptime(date, "%y-%m-%d")
        except:
            return "error"
        return old_symbol.lower()

    # convert to new format
    # if doesnt work, return error
    try:
        old_symbol = old_symbol.lower()
        isOption = True if old_symbol[-1] in ["c", "p"] else False

        if isOption:
            product, strike, cop = old_symbol.split(" ")
            year = "202" + str(product[-1])
            month = str(LETTER_TO_MONTH[product[-2]])
            product = product[:3]

            expiry = get_first_wednesday(int(year), int(month))
            # date object to YYYY-MM-DD
            expiry = expiry.strftime("%y-%m-%d")

            new_symbol = (
                LME_SYMBOL_MAP[product]
                + " o "
                + expiry
                + " a-"
                + str(strike)
                + "-"
                + cop
            )
            return new_symbol

        else:
            # split in two
            product, expiry = old_symbol.split(" ")

            new_symbol = LME_SYMBOL_MAP[product] + " f " + expiry[2:]
            return new_symbol
    except:
        print("unexpected error occured for instrument: " + old_symbol)
        return "error"


def get_valid_counterpart_dropdown_options(exchange):
    dropdown_options = []
    with engine.connect() as connection:
        result = connection.execute(
            f"SELECT counterparty FROM counterparty_clearer WHERE exchange_symbol = '{exchange}'"
        ).fetchall()

    # with legacyEngine.connect() as connection:
    #     result = connection.execute("SELECT * FROM counterparty_clearer")

    for row in result:
        counterparty = row[0]
        if counterparty != "TEST":
            dropdown_options.append({"label": counterparty, "value": counterparty})

    return dropdown_options


# desired format:
# {'id': 17, 'date': '2023-12-17', 'row-formatter': 'n', 'net-pos': 0.0, 'total': 61.0}
# current format:
# {'id': 'Sep-24', 'net': 0, 'cumulative': 0, 'date': '2024-09-18', 'net-pos': 20.0, 'total': 41.91000000000001}
# changes:
# id: diff
# date: same!
# total = same!
