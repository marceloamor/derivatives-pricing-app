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
from parts import loadStaticData, expiryProcess

# fetch georgia positions
data = conn.get("positions")
data = pickle.loads(data)
georgia_pos = pd.DataFrame(data)
georgia_pos.set_index("instrument", inplace=True)

print(georgia_pos.loc["PBD 2022-04-20"])
