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


# pos_json = conn.get("greekpositions")
# pos = pd.read_json(pos_json)
# print(pos["third_wed"])
# pos = pos[pos["product"].str[:3] == "lad"]
# print(pd.to_datetime(pos["third_wed"]))
# # print(pos.groupby("third_wed").sum())
# print(pos.groupby("third_wed").agg({"total_fullDelta": "sum", "third_wed": "first"}))

# pos = pos.round(2)

md_health = conn.get("md:health")
print(datetime.fromtimestamp(json.loads(md_health)))
