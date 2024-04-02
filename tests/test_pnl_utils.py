from datetime import date

import pandas as pd

from src import pnl_utils


def test_get_value_at_market():
    expected_df = pd.DataFrame(
        {
            "instrument_symbol": ["test"],
            "multiplier": [10],
            "quantity": [10],
            "market_price": [1.0],
            "value_at_market": [100.0],
        }
    ).set_index("instrument_symbol")
    mark_position = pd.DataFrame(
        {"instrument_symbol": ["test"], "multiplier": [10], "quantity": [10]}
    ).set_index("instrument_symbol")
    mark_price = pd.DataFrame(
        {"instrument_symbol": ["test"], "market_price": [1.0]}
    ).set_index("instrument_symbol")
    print(mark_position)
    print(mark_price)
    pd.testing.assert_frame_equal(
        pnl_utils.get_value_at_market(mark_position, mark_price), expected_df
    )


def test_get_value_at_market_w_portfolio_id():
    expected_df = pd.DataFrame(
        {
            "instrument_symbol": ["test"],
            "portfolio_id": [1],
            "multiplier": [10],
            "quantity": [10],
            "market_price": [1.0],
            "value_at_market": [100.0],
        }
    ).set_index("instrument_symbol")
    mark_position = pd.DataFrame(
        {
            "instrument_symbol": ["test"],
            "portfolio_id": [1],
            "multiplier": [10],
            "quantity": [10],
        }
    ).set_index("instrument_symbol")
    mark_price = pd.DataFrame(
        {"instrument_symbol": ["test"], "market_price": [1.0]}
    ).set_index("instrument_symbol")

    output_df = pnl_utils.get_value_at_market(mark_position, mark_price)
    print(mark_position)
    print(mark_price)
    print(output_df)
    pd.testing.assert_frame_equal(output_df, expected_df)


def test_get_aggregated_trade_price():
    expected_df = pd.DataFrame(
        {
            "instrument_symbol": ["test1", "test1", "test2"],
            "portfolio_id": [1, 2, 1],
            "multiplier": [10, 10, 5],
            "trade_date": [date(2024, 2, 1), date(2024, 2, 1), date(2024, 2, 1)],
            "total_traded_cash": [10.0, -15.0, 50.0],
            "total_quantity_traded": [2, 2, 4],
        }
    ).set_index(["instrument_symbol", "portfolio_id", "multiplier", "trade_date"])
    mark_day_trades = pd.DataFrame(
        {
            "instrument_symbol": ["test1", "test1", "test1", "test1", "test2", "test2"],
            "portfolio_id": [1, 1, 2, 2, 1, 1],
            "multiplier": [10, 10, 10, 10, 5, 5],
            "trade_date": [
                date(2024, 2, 1),
                date(2024, 2, 1),
                date(2024, 2, 1),
                date(2024, 2, 1),
                date(2024, 2, 1),
                date(2024, 2, 1),
            ],
            "quantity": [1, -1, 1, -1, 2, -2],
            "price": [100.0, 101.0, 102.0, 100.5, 250.0, 255.0],
        }
    )
    output_df = pnl_utils.get_aggregated_traded_price(mark_day_trades)

    pd.testing.assert_frame_equal(output_df, expected_df)


def test_get_per_instrument_portfolio_pnl():
    pass
