# # import flask_sqlalchemy
import pytest

from datetime import date
import sys

sys.path.append("src")
from apps import cashManager


@pytest.mark.parametrize(
    "symbol, expected_expiry",
    [
        # options
        ("LCUOF2 1200 P", date(2022, 1, 5)),
        ("LADOG2 8 C", date(2022, 2, 2)),
        ("pbdoh3 8000 P", date(2023, 3, 1)),
        ("LNDOJ3 1200 C", date(2023, 4, 5)),
        ("LZHOK3 9999 P", date(2023, 5, 3)),
        ("LCUOM4 1234 C", date(2024, 6, 5)),
        ("LADON5 6666 P", date(2025, 7, 2)),
        ("PBDOQ6 1440 C", date(2026, 8, 5)),
        ("LNDOU7 2000 P", date(2027, 9, 1)),
        ("ladov8 10000 C", date(2028, 10, 4)),
        ("LADOX9 1200 C", date(2029, 11, 7)),
        ("LADOZ8 1200 C", date(2028, 12, 6)),
        # futures
        ("LCU 2026-03-18", date(2026, 3, 18)),
        ("LAD 2024-10-16", date(2024, 10, 16)),
        ("PBD 2023-12-20", date(2023, 12, 20)),
        ("LND 2022-08-12", date(2022, 8, 12)),
        ("LZH 2022-09-21", date(2022, 9, 21)),
        # fails - returns past date to be filtered out
        ("LADOW2 1200 C", date(2020, 1, 1)),
        ("LAD 2023-21-09", date(2020, 1, 1)),
        ("utter nonsense", date(2020, 1, 1)),
        ("utter nonsense with more words", date(2020, 1, 1)),
    ],
)
def test_expiry_from_symbol(symbol, expected_expiry):
    # receiving it as symbol.upper()
    assert cashManager.expiry_from_symbol(symbol) == expected_expiry


# test getpricefromClo2?
