import pandas as pd

# from pysimplesoap.client import SoapClient
import pickle, math, os, time
from time import sleep
import ujson as json
import numpy as np
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.message import EmailMessage
import mimetypes
import sqlalchemy.orm
from datetime import date
from dash import dcc, html
import dash_bootstrap_components as dbc
from company_styling import main_color, logo

from sql import (
    sendTrade,
    deleteTrades,
    updatePos,
    updateRedisDelta,
    updateRedisPos,
    updateRedisTrade,
    pulltrades,
    pullPosition,
)
from data_connections import (
    Cursor,
    conn,
    select_from,
    PostGresEngine,
    HistoricalVolParams,
)
from calculators import linearinterpol
from TradeClass import TradeClass, VolSurface

sdLocation = os.getenv("SD_LOCAITON", default="staticdata")
positionLocation = os.getenv("POS_LOCAITON", default="greekpositions")

DAYS_TO_TRADE_REC = 5


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
        carry_link = data[instrument]["carry_link"]
        counterparty = data[instrument]["counterparty"]

        Ttheo = Ttheo + theo
        Tdelta = Tdelta + delta
        Tgamma = Tgamma + gamma
        Tvega = Tvega + vega
        Ttheta = Ttheta + theta

        greeks.append(
            (
                instrument,
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

    greeks.append(("Total", " ", Ttheo, "", "", "", Tdelta, Tgamma, Tvega, Ttheta, "", ""))
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


def unpackRisk(data, greek):
    output = []

    for i in data:
        greeks = {}
        greeks["Underlying\Volatility"] = i
        for j in data[i]:
            greeks[j] = data[i][j][greek]

        output.append(greeks)
    return output


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


def saveF2Trade(df, user):
    # create st to record which products to update in redis
    redisUpdate = set([])
    for row in df.iterrows():
        if row[1]["optionTypeId"] in ["C", "P"]:
            # is option
            product = row[1]["productId"] + monthSymbol(row[1]["prompt"])
            redisUpdate.add(product[:6])
            prompt = optionPrompt(product)
            trade = TradeClass(
                0,
                row[1]["tradeDate"],
                product,
                int(row[1]["strike"]),
                row[1]["optionTypeId"],
                prompt,
                float(row[1]["price"]),
                row[1]["quanitity"],
                "Trade Transfer",
                "",
                user,
                row[1]["tradingVenue"],
            )
            # send trade to SQL and update all redis parts
            trade.id = sendTrade(trade)
            updatePos(trade)
        else:
            # is underlying
            product = row[1]["productId"] + " " + row[1]["prompt"].strftime("%Y-%m-%d")
            redisUpdate.add(product[:6])
            trade = TradeClass(
                0,
                row[1]["tradeDate"],
                product,
                None,
                None,
                row[1]["prompt"].strftime("%Y-%m-%d"),
                float(row[1]["price"]),
                row[1]["quanitity"],
                "Trade Transfer",
                "",
                user,
                row[1]["tradingVenue"],
            )
            # send trade to SQL and update all redis parts
            trade.id = sendTrade(trade)
            updatePos(trade)

    # update redis for delta, pos trades and the prompt curve
    for update in redisUpdate:
        updateRedisDelta(update)
        updateRedisPos(update)
        updateRedisTrade(update)


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


def recTrades():
    # compare F2 and Georgia trades and show differences

    # pull f2 trades
    # todays date in F2 format
    today = pd.datetime.now().strftime("%Y-%m-%d")
    gTrades = pulltrades(today)

    # convert quanitity to int
    gTrades["quanitity"] = gTrades["quanitity"].astype(int)

    # round price to 2 dp
    gTrades["price"] = gTrades["price"].round(2)

    # filter for columns we want
    gTrades = gTrades[["instrument", "price", "quanitity", "prompt", "venue"]]

    # filter out expiry trades
    # gTrades = gTrades[gTrades['venue'] !='Expiry Process']

    # pull F2 trades
    fTrades = allF2Trades()

    # convert prompt to date
    fTrades["prompt"] = pd.to_datetime(
        fTrades["prompt"], dayfirst=True, format="%d/%m/%Y"
    )

    # build instrument name from parts
    fTrades["instrument"] = fTrades.apply(convertInstrumentName, axis=1)
    # filter for columns we want
    fTrades = fTrades[["instrument", "price", "quanitity", "prompt", "tradingVenue"]]

    # rename tradingVenue as venue
    fTrades = fTrades.rename(columns={"tradingVenue": "venue"})

    # concat and group then take only inputs with groups of 1
    all = pd.concat([gTrades, fTrades])
    all = all.reset_index(drop=True)
    all_gpby = all.groupby(list(["instrument", "price", "quanitity"]))
    idx = [x[0] for x in all_gpby.groups.values() if len(x) % 2 != 0]
    all.reindex(idx)

    return all.reindex(idx)


def loadSelectTrades():
    # todays date in F2 format
    today = pd.datetime.now().strftime("%Y-%m-%d")

    # find time of last select trade pulled
    trades = pulltrades(today)
    trades = trades[trades["venue"] == "Select"]
    trades.sort_values("dateTime", ascending=False)
    lastTime = trades["dateTime"].values[0]

    # import .csv F2 value
    csvLocation = "P:\\Options Market Making\\LME\\F2 reports\\Trading Activity - LME Internal.csv"
    df = pd.read_csv(csvLocation, thousands=",")

    # convert tradedate column to datetime
    df["tradeDate"] = pd.to_datetime(df["tradeDate"], format="%d/%m/%Y %H:%M:%S")
    # df['tradeDate'] = df['tradeDate'].astype(str)

    # filter for tradedates we want
    df = df[df["tradeDate"] > lastTime]

    # filter for just sleect trades
    df = df[df["tradingVenue"] == "Select"]

    # filter for columns we want
    df = df[
        [
            "tradeDate",
            "productid",
            "prompt",
            "type",
            "strike",
            "originalLots",
            "price",
            "tradingVenue",
        ]
    ]

    # split df into options and futures
    dfOpt = df[df["type"].isin(["C", "P"])]
    dfFut = df[df["type"].isin(["C", "P"]) == False]

    # change prompt to datetime
    dfFut["prompt"] = pd.to_datetime(dfFut["prompt"], format="%d/%m/%y")
    dfOpt["prompt"] = pd.to_datetime(dfOpt["prompt"], format="%d/%m/%y")

    # round strike to 2 dp
    dfOpt["strike"] = dfOpt["strike"].astype(float)
    dfOpt.strike = dfOpt.strike.round(2)

    # append dataframes together
    df = pd.concat([dfFut, dfOpt])

    # filter for columns we want
    df = df[
        [
            "tradeDate",
            "productid",
            "prompt",
            "type",
            "strike",
            "originalLots",
            "price",
            "tradingVenue",
        ]
    ]

    # convert price to float from string with commas
    df["price"] = df["price"].astype(float)

    # return to camelcase
    df = df.rename(
        columns={
            "productid": "productId",
            "type": "optionTypeId",
            "originalLots": "quanitity",
        }
    )

    saveF2Trade(df, "system")


def loadLiveF2Trades2():
    # todays date in F2 format
    today = pd.datetime.now().strftime("%Y-%m-%d")

    # delete trades from sql
    deleteTrades(today)

    # import .csv F2 value
    csvLocation = (
        "P:\\Options Market Making\\LME\\F2 reports\\Detailed Open Position.csv"
    )
    df = pd.read_csv(csvLocation)

    # convert tradedate column to datetime
    df["tradedate"] = pd.to_datetime(df["tradedate"], format="%d/%m/%y")

    # filter for tradedates we want
    df = df[df["tradedate"] >= today]

    # filter for columns we want
    df = df[
        ["tradeDate", "productid", "prompt", "optiontypeid", "strike", "lots", "price"]
    ]

    # split df into options and futures
    dfOpt = df[df["optiontypeid"].isin(["C", "P"])]
    dfFut = df[df["optiontypeid"].isin(["C", "P"]) == False]

    # change prompt to datetime
    dfFut["prompt"] = pd.to_datetime(dfFut["prompt"], format="%d/%m/%Y")
    dfOpt["prompt"] = pd.to_datetime(dfOpt["prompt"], format="%d/%m/%Y")

    # round strike to 2 dp
    dfOpt["strike"] = dfOpt["strike"].astype(float)
    dfOpt.strike = dfOpt.strike.round(2)

    # append dataframes together
    df = pd.concat([dfFut, dfOpt])

    # filter for columns we want
    df = df[
        ["tradeDate", "productid", "prompt", "optiontypeid", "strike", "lots", "price"]
    ]

    # convert price to float from string with commas
    df["price"] = df["price"].str.replace(",", "").astype(float)

    # return to camelcase
    df = df.rename(
        columns={
            "productid": "productId",
            "optiontypeid": "optionTypeId",
            "lots": "quanitity",
        }
    )

    saveF2Trade(df, "system")


def readModTime():
    filePath = "P:\\Options Market Making\\LME\\F2 reports\\Trading Activity - LME Internal.csv"
    modTimesinceEpoc = os.path.getmtime(filePath)

    # Convert seconds since epoch to readable timestamp
    modificationTime = time.strftime(
        "%Y-%m-%d %H:%M:%S", time.localtime(modTimesinceEpoc)
    )
    return modificationTime


def SetModTime(modificationTime):
    # save time to redis for comparison later.
    conn.set("f2LiveUpdate", modificationTime)


def sendFIXML(fixml):
    # take WSDL and create connection to soap server
    WSDL_URL = (
        "http://live-boapp1.sucden.co.uk/services/Sucden.TradeRouter.svc?singleWsdl"
    )
    client = SoapClient(wsdl=WSDL_URL, ns="web", trace=True)

    # Discover operations
    list_of_services = [service for service in client.services]

    # Discover params
    method = client.services["TradeRouterService"]

    # send fixml to canroute trade and print the response
    response = client.RouteTrade(source="MetalVolDesk", fixml=fixml)

    return response["RouteTradeResult"]


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
    now = datetime.now().time()
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
                            dbc.DropdownMenuItem("Risk", href="/riskmatrix"),
                            dbc.DropdownMenuItem("Strike Risk", href="/strikeRisk"),
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
                            dbc.DropdownMenuItem("F2 Rec", href="/rec"),
                            dbc.DropdownMenuItem("Route Status", href="/routeStatus"),
                            dbc.DropdownMenuItem("Expiry", href="/expiry"),
                            dbc.DropdownMenuItem("Rate Curve", href="/rates"),
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


# send redis queue update for each product that has been traded
def sendPosQueueUpdate(product):
    # pic_data = pickle.dumps(product)
    conn.publish("queue:update_position", product)


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


def OLDexpiryProcess(product, ref):
    ##inputs to be entered from the page
    now = datetime.now().strftime("%Y-%m-%d")

    # load positions for product
    pos = pullPosition(product[:3].lower(), now)

    # filter for just the month we are looking at
    pos = pos[(pos["instrument"].str)[:6] == product]

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
    futC = posIC.reset_index()
    futC["instrument"] = futureName
    futC["prompt"] = thirdWed
    futC["action"] = "Exercise Future"
    futC["price"] = futC["strike"]
    futC["strike"] = None
    futC["optionTypeId"] = None

    futP = posIP.reset_index()
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
    all["tradeDate"] = pd.datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    # drop the columns we dont need
    all.drop(
        ["delta", "index", "settlePrice", "index", "ID", "dateTime"],
        axis=1,
        inplace=True,
    )

    return all


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
    all["tradeDate"] = pd.datetime.now().strftime("%d/%m/%Y %H:%M:%S")

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


def recBGM(brit_pos):
    # fetch georgia positions
    data = conn.get("positions")
    data = pickle.loads(data)
    georgia_pos = pd.DataFrame(data)
    georgia_pos.set_index("instrument", inplace=True)

    # remove special character and parse Dataframe
    brit_pos.columns = brit_pos.columns.str.replace(" ", "")
    brit_pos.columns = brit_pos.columns.str.lower()

    # convert all object types to string and strip blank spaces
    df_obj = brit_pos.select_dtypes(["object"])
    brit_pos[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())

    # select all but last row
    brit_pos = brit_pos[brit_pos["postype"] != "EOF"]

    # select only USD contracts
    brit_pos = brit_pos[brit_pos["ccy"] == "USD"]

    def apply_date(row):
        if row["type"] == "FUT":
            date = datetime.strptime(row["delivery"], "%d-%b-%y")
            date = date.strftime("%Y-%m-%d")
            product = lme_future_to_georgia(row["combinedcode"].upper())
            name = "{} {}".format(product, date)
        else:
            product = lme_option_to_georgia(
                row["combinedcode"].lower(), row["delivery"]
            )
            name = "{} {} {}".format(
                product.upper(), row["strike"].upper(), row["contract"][3]
            )

        return name

    # build instrument name from other columns in UPE format
    brit_pos["instrument"] = brit_pos.apply(apply_date, axis=1)

    # set index as instrument
    brit_pos.set_index("instrument", inplace=True)

    # rename column to quanitity
    brit_pos.rename(columns={"nett": "quanitity"}, inplace=True)

    # merge BGM and UPE position on index(instrument)
    combinded = brit_pos[["quanitity"]].merge(
        georgia_pos[["quanitity"]],
        how="outer",
        left_index=True,
        right_index=True,
        suffixes=("_BGM", "_UPE"),
    )
    combinded.fillna(0, inplace=True)

    # calc diff
    combinded["diff"] = combinded["quanitity_BGM"] - combinded["quanitity_UPE"]

    # return only rows with a non 0 diff
    return combinded[combinded["diff"] != 0]


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
        if rjo_row["contractcode"] == "AL":
            sol3_symbol += "ALI"
        else:
            sol3_symbol += "HG"
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
        if rjo_row["contractcode"] == "AL":
            sol3_symbol += (
                " "
                + str(float(rjo_row["optionstrikeprice"])).rstrip("0").rstrip(".")
                + " "
            )
        else:
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
