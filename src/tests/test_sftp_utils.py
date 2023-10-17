from dash.testing.application_runners import import_app
import pandas as pd
import sys

sys.path.append("src/app.py")
# from app import app


# Test case 1: Write your test case here
import sftp_utils
import app


def test_fetch_latest_rjo_export():
    # Test case 1: Write your test case here
    file_format = "UPETRADING_csvnpos_npos_%Y%m%d.csv"
    (file_df, file_name) = sftp_utils.fetch_latest_rjo_export(file_format)

    # assert file_df is of type pandas.DataFrame and file_name is of type str
    assert isinstance(file_df, pd.DataFrame)
    assert isinstance(file_name, str)
