import pandas as pd
import sys

sys.path.append("src")

from apps import dataDownload


# good test for static data, pandas, redis conn, pickling
def test_getMonthlyPositions():
    (df, msg) = dataDownload.getMonthlyPositions()
    assert isinstance(df, pd.DataFrame)
    assert isinstance(msg, str)


# all other functions in this module covered by sftp and pandas
# if this page breaks, must be the untested dash dcc.send_file() download function
