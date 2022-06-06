from ast import Str
from pandas.core.frame import DataFrame
import redis, pickle, json
from data_connections import conn, call_function, select_from, PostGresEngine
import pandas as pd
import numpy as np
from calculators import linearinterpol
from datetime import date
from parts import settleVolsProcess, pullPrompts, pullPosition, loadStaticData
from datetime import datetime as datetime
from parts import loadStaticData, expiryProcess, buildParamMatrix


def pulVols(portfolio):
    # pull matrix inputs
    dff, sol_curve = buildParamMatrix(portfolio.capitalize())
    # create product column
    dff["product"] = dff.index
    dff["prompt"] = pd.to_datetime(dff["prompt"], format="%d/%m/%Y")
    dff = dff.sort_values(["prompt"], na_position="first")

    # convert call/put max into difference
    dff["cmax"] = dff["cmax"] - dff["vol"]
    dff["pmax"] = dff["pmax"] - dff["vol"]

    # mult them all by 100 for display
    dff.loc[:, "vol"] *= 100
    dff.loc[:, "skew"] *= 100
    dff.loc[:, "call"] *= 100
    dff.loc[:, "put"] *= 100
    dff.loc[:, "cmax"] *= 100
    dff.loc[:, "pmax"] *= 100

    cols = ["vol", "skew", "call", "put", "cmax", "pmax"]

    dff[cols] = dff[cols].round(2)

    dict = dff.to_dict("records")

    return dict, sol_curve


data_previous = pulVols("copper")

print(data_previous[0])
