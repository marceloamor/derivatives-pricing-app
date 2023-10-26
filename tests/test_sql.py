import pandas as pd

import datetime as dt
import sys

sys.path.append("src/")
import sql


# what to test in parts:
def test_pullPosition():
    today = dt.datetime.now().date()
    df = sql.pullPosition("LAD", today)
    assert isinstance(df, pd.DataFrame)


# all functions in this module to be replaced after new static data is available
# no further tests needed for this module
