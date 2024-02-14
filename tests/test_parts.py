import datetime as dt
import sys

import pandas as pd
import pytest
from upedata import static_data as upe_static

sys.path.append("src/")
import data_connections
import parts
from app import app


# what to test in parts:
def test_loadStaticData():
    sd = parts.loadStaticData()
    assert isinstance(sd, pd.DataFrame)


def test_timeStamp_returns_timeStamp():
    ts = parts.timeStamp()
    assert isinstance(ts, dt.datetime)


def test_timeStamp_returns_current_time():
    ts = parts.timeStamp()
    now = dt.datetime.now()
    expected_ts = now  # .strftime("%Y-%m-%d %H:%M:%S")
    assert ts.date() == expected_ts.date()


# basic check on redis connection, but best test for homepage.py
def test_pullPortfolioGreeks():
    df = parts.pullPortfolioGreeks()
    assert isinstance(df, pd.DataFrame)


# numpy intensive function, rudimentary test below does a simple check that the function completes calculations
def test_calculate_time_remaining():
    with app.server.app_context():
        with data_connections.Session() as session:
            expiry = (
                session.query(upe_static.Option.expiry)
                .order_by(upe_static.Option.expiry.desc())
                .first()
            )

    time_remaining = parts.calculate_time_remaining(expiry[0])
    assert time_remaining[0] > 1


@pytest.mark.parametrize(
    "year, month, expected_expiry",
    [
        (2022, 1, dt.date(2022, 1, 5)),
        (2022, 2, dt.date(2022, 2, 2)),
        (2023, 3, dt.date(2023, 3, 1)),
        (2025, 3, dt.date(2025, 3, 5)),
        (2027, 3, dt.date(2027, 3, 3)),
        (2029, 3, dt.date(2029, 3, 7)),
        (2031, 3, dt.date(2031, 3, 5)),
    ],
)
def test_get_first_wednesday(year, month, expected_expiry):
    date = parts.get_first_wednesday(year, month)
    assert date == expected_expiry


# # create fixture for test data
@pytest.fixture(scope="module")
def test_cme_pos_rec():
    csv_directory = "tests/test_assets"

    rjo_df = pd.read_csv(
        csv_directory + "/UPETRADING_csvnpos_npos_20231020.csv", sep=","
    )
    sol3_df = pd.read_csv(
        csv_directory + "/export_positions_cme_20231023-0015.csv", sep=";"
    )
    return (sol3_df, rjo_df)


def test_rec_sol3_rjo_cme_pos(test_cme_pos_rec):
    # get latest sol3 and rjo pos exports
    (latest_sol3_df, latest_rjo_df) = test_cme_pos_rec

    # drop all contracts not in sol3 (LME)
    latest_rjo_df = latest_rjo_df[
        ~latest_rjo_df["Bloomberg Exch Code"].isin(["LME", "EOP"])
    ]
    latest_rjo_df = latest_rjo_df[
        latest_rjo_df["Contract Code"].isin(list(parts.rjo_to_sol3_hash.keys()))
    ]

    rec = parts.rec_sol3_rjo_cme_pos(latest_sol3_df, latest_rjo_df)

    expected_dict = [
        {
            "instrument": "XCME FUT HG 12 2023",
            "pos_rjo": 99.0,
            "pos_sol3": 107,
            "diff": -8,
        },
        {
            "instrument": "XLME FUT LALZ 12 2099",
            "pos_rjo": 0.0,
            "pos_sol3": -1,
            "diff": 1,
        },
    ]

    assert isinstance(rec, pd.DataFrame)
    assert rec.shape == pd.DataFrame(expected_dict).shape
    # reduce dimensions to compare
    rec_dict = rec.to_dict(orient="records")
    assert rec_dict == expected_dict


# this test is good for numpy and pandas
def test_calc_lme_vol():
    settle_model_inputs = {
        "vol": 0.166,
        "s": 2203.14,
        "t": 0.016030623362918568,
        "r": 0.052613322766610515,
        "var1": 0.17400000000000002,
        "var2": 0.17310000000000003,
        "var3": 0.18600000000000003,
        "var4": 0.185,
    }
    params = pd.DataFrame(
        [
            {
                "t": 0.016030623362918568,
                "interest_rate": 0.054022,
                "settle_model_inputs": settle_model_inputs,
            }
        ]
    )

    vol = parts.calc_lme_vol(params, float(2202.89), float(2200.0))
    assert vol == 0.1661


@pytest.mark.parametrize(
    "old_symbol, expected_new_symbol",
    [
        ("lcuoz3 8400 c", "xlme-lcu-usd o 23-12-06 a-8400-c"),
        ("ladOj4 1111 p", "xlme-lad-usd o 24-04-03 a-1111-p"),
        ("LNDOK4 10 C", "xlme-lnd-usd o 24-05-01 a-10-c"),
        ("lcu 2023-11-15", "xlme-lcu-usd f 23-11-15"),
        ("lad 2023-12-20", "xlme-lad-usd f 23-12-20"),
        ("PBd 2024-12-18", "xlme-pbd-usd f 24-12-18"),
        ("xlme-pbd-usd f 24-12-18", "xlme-pbd-usd f 24-12-18"),
        ("xLME-pbd-USD f 25-12-18", "xlme-pbd-usd f 25-12-18"),
        ("xlme-lcu-usd o 23-12-06 a-8400-c", "xlme-lcu-usd o 23-12-06 a-8400-c"),
        ("xlme-lcu-usd o 23-12-06 a-8400-c", "xlme-lcu-usd o 23-12-06 a-8400-c"),
        ("xlme-lcu-usd o 23-20-06 a-8400-c", "error"),
        ("XEXT-EBM-EUR O 24-02-15 A-230-C", "XEXT-EBM-EUR O 24-02-15 A-230-C"),
        ("xext-ebm-eur o 24-02-15 a-230-c", "xext-ebm-eur o 24-02-15 a-230-c"),
    ],
)
def test_build_new_lme_symbol_from_old(old_symbol, expected_new_symbol):
    new_symbol = parts.build_new_lme_symbol_from_old(old_symbol)
    assert new_symbol == expected_new_symbol
