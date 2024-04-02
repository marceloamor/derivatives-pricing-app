import pandas as pd

import sys

sys.path.append("src/")
import sftp_utils


def test_fetch_latest_rjo_export():
    # Test case 1: Write your test case here
    file_format = "UPETRADING_csvnpos_npos_%Y%m%d.csv"
    (file_df, file_name) = sftp_utils.fetch_latest_rjo_export(file_format)

    # assert file_df is of type pandas.DataFrame and file_name is of type str
    assert isinstance(file_df, pd.DataFrame)
    assert isinstance(file_name, str)


def test_fetch_latest_sol3_export():
    # Test case 1: Write your test case here
    (
        latest_sol3_df,
        latest_sol3_filename,
    ) = sftp_utils.fetch_latest_sol3_export(
        "positions", "export_positions_cme_%Y%m%d-%H%M.csv"
    )

    # assert file_df is df and file_name is str
    assert isinstance(latest_sol3_df, pd.DataFrame)
    assert isinstance(latest_sol3_filename, str)
