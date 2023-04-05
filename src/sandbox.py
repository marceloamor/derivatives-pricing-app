from data_connections import conn
import pandas as pd

data = conn.get("greekpositions_xext:dev")

if data != None:
    dff = pd.read_json(data)

print(dff["portfolio"])
