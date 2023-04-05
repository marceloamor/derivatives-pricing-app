# the risk matrix will take a set of varibles via the API and return a JSON message of the risk matrix for that product.
import pandas as pd
from datetime import date
from calculators import Option, VolSurface
import pickle, datetime, time, math
import orjson as json
from datetime import datetime
import numpy as np

# from TradeClass import VolSurface
from data_connections import conn


def loadStaticData():
    # pull staticdata from redis
    staticData = conn.get("staticData")
    staticData = pd.read_json(staticData)

    # filter for non expired months
    today = datetime.now()
    today = today.strftime("%Y-%m-%d")
    staticData = staticData[
        pd.to_datetime(staticData["expiry"], format="%d/%m/%Y").dt.strftime("%Y-%m-%d")
        > today
    ]

    return staticData


# load delta from redis
def getDelta(product):
    delta = conn.get(product.lower()[0:3] + "Delta")
    if delta:
        delta = pickle.loads(delta)
        return delta["quanitity"].sum()
    else:
        return 0


def getData(product):
    data = conn.get(product.lower())
    return data


# take inputs and include spot and time to buildvolsurface inputs
def buildSurfaceParams(params, product, spot, exp_date, eval_date):
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


# go to redis and get params for volsurface
def loadVolaData(product):
    product = product.lower() + "Vola"
    new_data = conn.get(product)
    new_data = json.loads(new_data)
    return new_data


def getPortfolioData(portfolio, static):
    products = static.loc[static["portfolio"] == portfolio, "product"]
    data = {}
    for product in products:
        productData = getData(product)
        if productData:
            productData = json.loads(productData)
            data[product] = productData
    return data


def getParamsList(portfolio, static):
    products = static.loc[static["portfolio"] == portfolio, "product"]
    data = {}
    for product in products:
        productData = loadVolaData(product)
        if productData:
            data[product.lower()] = productData
    return data


def getVol(model, strike):
    vol = model.get_basicVol(strike)
    return vol


def getGreeks(option, strike):
    if not math.isnan(float(strike)):
        in_greeks = option.get_all()

        return in_greeks


def resolveGreeks(product, positionGreeks, eval_date, undShock, volShock):
    try:
        # filter greeks to required underlying
        print("Starting to resolve greeks")
        greeks = positionGreeks.loc[positionGreeks.portfolio == product]

        if not greeks.empty:
            # add eval date
            eval_date = datetime.strptime(eval_date, "%d/%m/%Y")
            greeks.loc[:, "eval_date"] = eval_date

            # convert expiry to datetime
            greeks.loc[greeks.index.str[-1:].isin(["c", "p"]), "expiry"] = greeks.loc[
                greeks.index.str[-1:].isin(["c", "p"]), "expiry"
            ].apply(lambda x: date.fromtimestamp(x / 1e9))
            greeks.loc[
                greeks.index.str[-1:].isin(["c", "p"]), "third_wed"
            ] = greeks.loc[greeks.index.str[-1:].isin(["c", "p"]), "third_wed"].apply(
                lambda x: date.fromtimestamp(x / 1e9)
            )

            # fill in greeks for futures
            greeks.loc[
                ~greeks.index.str[-1:].isin(["c", "p"]), ["delta", "fullDelta"]
            ] = 1
            greeks.loc[
                ~greeks.index.str[-1:].isin(["c", "p"]),
                [
                    "theta",
                    "gamma",
                    "vega",
                    "skewSense",
                    "callSense",
                    "putSense",
                    "deltaDecay",
                    "gammaDecay",
                    "vegaDecay",
                ],
            ] = 0

            # build vol sruface from params
            def apply_VolSurface(und, vol, sk, c, p, cmax, pmax, exp_date, eval_date):
                if not math.isnan(float(vol)):
                    volModel = VolSurface(
                        s=und,
                        vol=vol,
                        sk=sk,
                        c=c,
                        p=p,
                        cmax=cmax,
                        pmax=pmax,
                        exp_date=exp_date,
                        eval_date=eval_date,
                        k=0,
                        ref=None,
                    )
                    return volModel

            # applt volsurface build
            greeks["volModel"] = np.vectorize(apply_VolSurface)(
                greeks["und_calc_price"],
                greeks["vol"],
                greeks["skew"],
                greeks["calls"],
                greeks["puts"],
                greeks["cmax"],
                greeks["pmax"],
                greeks["expiry"],
                greeks["eval_date"],
            )

            # build option class from inputs
            def apply_option(row):
                print(float(row["und_calc_price"]) + float(row["i"]))
                model = Option(
                    row["cop"],
                    float(row["und_calc_price"]) + float(row["i"]),
                    row["strike"],
                    row["eval_date"],
                    row["expiry"],
                    row["interest_rate"],
                    float(row["vol"]) + float(row["j"]),
                    params=row["volModel"],
                )
                return model

            # iterate over shocks
            results = {}

            # interate over underlying move
            for i in range(len(undShock)):
                # bucket for vol results
                volResults = {}

                # assign und shock to dataframe for vectorising later
                greeks["i"] = undShock[i]

                for j in range(len(volShock)):
                    print("und shock {}, vol shock {}".format(undShock[i], volShock[j]))
                    # calculate greeks for each strikes
                    greeks["j"] = volShock[j]

                    # if product an options build option class incudling vol and und shock
                    greeks.loc[
                        greeks.index.str[-1:].isin(["c", "p"]), "option"
                    ] = greeks[greeks.index.str[-1:].isin(["c", "p"])].apply(
                        apply_option, axis=1
                    )

                    # calculate greeks for each option class
                    greeks[
                        "calc_price",
                        "delta",
                        "theta",
                        "gamma",
                        "vega",
                        "skewSense",
                        "callSense",
                        "putSense",
                        "deltaDecay",
                        "gammaDecay",
                        "vegaDecay",
                        "fullDelta",
                        "t",
                    ] = np.vectorize(getGreeks)(greeks["option"], greeks["strike"])

                    # calc combinded greeks taking account of mult and none mult greeks
                    calc_greeks = [
                        "delta",
                        "theta",
                        "gamma",
                        "vega",
                        "skewSense",
                        "callSense",
                        "putSense",
                        "deltaDecay",
                        "gammaDecay",
                        "vegaDecay",
                        "fullDelta",
                    ]

                    multiple_greeks = ["theta", "vega"]

                    # calc total greeks including multiplier if required
                    for calc_greek in calc_greeks:
                        if calc_greek in multiple_greeks:
                            greeks["total_{}".format(calc_greek)] = (
                                greeks[calc_greek]
                                * greeks["quanitity"]
                                * greeks["multiplier"]
                            )
                        else:
                            greeks["total_{}".format(calc_greek)] = (
                                greeks[calc_greek] * greeks["quanitity"]
                            )

                    # sum greeks and convert to dict for datatable
                    volResults[volShock[j]] = (
                        greeks[
                            [
                                "total_delta",
                                "total_theta",
                                "total_gamma",
                                "total_vega",
                                "total_skewSense",
                                "total_callSense",
                                "total_putSense",
                                "total_deltaDecay",
                                "total_gammaDecay",
                                "total_vegaDecay",
                                "total_fullDelta",
                            ]
                        ]
                        .sum()
                        .to_dict()
                    )

                results[undShock[i]] = volResults

            return results
    except Exception as e:
        print(e)


def runRisk(ApiInputs):
    starttime = time.time()
    # pull in inputs from api call
    portfolio = ApiInputs["portfolio"]
    vol = ApiInputs["vol"]
    und = ApiInputs["und"]
    level = ApiInputs["level"]
    eval = ApiInputs["eval"]
    rel = ApiInputs["rel"]

    positionGreeks = pd.read_json(conn.get("greekpositions"))

    results = resolveGreeks(portfolio, positionGreeks, eval, und, vol)

    # print('That took {} seconds'.format(time.time() - starttime))
    results = json.dumps(results)
    return results


# ApiInputs = {'portfolio': 'copper', 'vol': ['-0.05', ' -0.04', ' -0.03', ' -0.02', ' -0.01', ' 0.0', ' 0.01', ' 0.02', ' 0.03', ' 0.04', ' 0.05'], 'und': ['-200.0', ' -160.0', ' -120.0', ' -80.0', ' -40.0', ' 0.0', ' 40.0', ' 80.0', ' 120.0', ' 160.0', ' 200.0'], 'level': 'high', 'eval': '23/02/2022', 'rel': 'abs'}

# runRisk(ApiInputs)
