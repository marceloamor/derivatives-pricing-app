import numpy as np
import pandas as pd


def get_value_at_market(
    mark_position: pd.DataFrame, mark_price: pd.DataFrame
) -> pd.DataFrame:
    """Merges the current position with current prices and
    and generates an output table containing the `value_at_market`
    of the positions given.
    In the case where positions lack a corresponding entry in
    the `mark_price` table, they will be dropped during the
    internal joining operation.

    :param mark_position: Table with the position at mark-time,
    required columns are `instrument_symbol`, `multiplier`,
    `quantity`
    :type mark_position: pd.DataFrame
    :param mark_price: Prices at mark of instruments in
    positions, required columns are `instrument_symbol`
    `market_price`
    :type mark_price: pd.DataFrame
    :return: Positions with their values at market added,
    no aggregation step takes place so separation by
    portfolio if included on input will be preserved,
    this will include the columns: `instrument_symbol`,
    `multiplier`, `quantity`, `market_price`, `value_at_market`
    :rtype: pd.DataFrame
    """
    joined_pos_price = mark_position.merge(
        mark_price, on=["instrument_symbol"], how="inner"
    )
    joined_pos_price["value_at_market"] = (
        joined_pos_price["quantity"]
        * joined_pos_price["multiplier"]
        * joined_pos_price["market_price"]
    )
    return joined_pos_price


def get_aggregated_traded_price(mark_day_trades: pd.DataFrame) -> pd.DataFrame:
    """Generate overall trade cash cost for the given day of trading provided
    in the argument.
    Uses grouping by `instrument_symbol`, `portfolio_id`, `trade_date` to sum
    the "total traded cash" of each trade, providing a P&L value for the day's
    trades.

    :param mark_day_trades: Trades that were made on the day being marked,
    requires columns: `instrument_symbol`, `portfolio_id`, `trade_date`,
    `quantity`, `multiplier`, `price`
    :type mark_day_trades: pd.DataFrame
    :return: An aggregated table showing the daily cash-flow caused by
    trading each instrument in each portfolio, with columns:
    `instrument_symbol`, `portfolio_id`, `multiplier`, `trade_date`,
    `total_traded_cash`, `total_quantity_traded`
    :rtype: pd.DataFrame
    """
    mark_day_trades["total_traded_cash"] = (
        -1
        * mark_day_trades["quantity"]
        * mark_day_trades["multiplier"]
        * mark_day_trades["price"]
    )
    mark_day_trades["abs_quantity"] = mark_day_trades["quantity"].abs()
    summed_day_trades = mark_day_trades.groupby(
        by=["instrument_symbol", "portfolio_id", "multiplier", "trade_date"]
    ).sum(numeric_only=True)
    summed_day_trades = summed_day_trades[["total_traded_cash", "abs_quantity"]].rename(
        columns={"abs_quantity": "total_quantity_traded"}
    )
    return summed_day_trades


def get_per_instrument_portfolio_pnl(
    tm1_to_2_dated_pos: pd.DataFrame,
    tm1_trades: pd.DataFrame,
    dated_instrument_settlement_prices: pd.DataFrame,
) -> pd.DataFrame:
    """Calculates an internal estimate of gross PnL.
    Output contains additional columns to include the split
    between Trade and Position related P&L.

    :param tm1_to_2_dated_pos: Positions data with each row
    relating to the position of an instrument within a given
    portfolio on a given date with a valid settlement price.
    These columns are `instrument_symbol`, `portfolio_id`,
    `multiplier`, `position_date`, `quantity`
    :type tm1_to_2_dated_pos: pd.DataFrame
    :param tm1_trades: Trades data from the last business day
    after the `rollback_trades_since` date.
    Contains columns: `trade_datetime_utc`, `instrument_symbol`,
    `portfolio_id`, `multiplier`, `quantity`, `price`
    :type tm1_trades: pd.DataFrame
    :param dated_instrument_settlement_prices: Settlement price
    data for all instruments to have P&L calculated with columns
    `settlement_date`, `instrument_symbol`, `market_price`
    :type dated_instrument_settlement_prices: pd.DataFrame

    :return: Gross P&L information on a per-instrument-portfolio
    basis, with the columns: `instrument_symbol`, `portfolio_id`,
    `multiplier`, `position_pnl`, `trade_pnl`, `qty_traded`,
    `qty_held`, `total_gross_pnl`
    :rtype: pd.DataFrame
    """
    try:
        m1_np_timestamp, m2_np_timestamp = np.array(
            np.sort(tm1_to_2_dated_pos["position_date"].unique())[:-3:-1],
            dtype=np.datetime64,
        )  # the two most recent timestamps/dates
    except IndexError as e:
        e.add_note(
            "Less than two dates were present in the positions "
            "date column two are required for P&L processing to function"
        )
        raise e
    m1_pd_date = pd.Timestamp(m1_np_timestamp).date()
    m2_pd_date = pd.Timestamp(m2_np_timestamp).date()

    m1_pos_val = get_value_at_market(
        mark_position=tm1_to_2_dated_pos.loc[
            tm1_to_2_dated_pos["position_date"] == m1_pd_date
        ],  # .set_index(["instrument_symbol", "portfolio_id"]),
        mark_price=dated_instrument_settlement_prices.loc[
            dated_instrument_settlement_prices["settlement_date"] == m1_pd_date
        ],
    )
    m2_pos_val = get_value_at_market(
        mark_position=tm1_to_2_dated_pos.loc[
            tm1_to_2_dated_pos["position_date"] == m2_pd_date
        ],
        mark_price=dated_instrument_settlement_prices.loc[
            dated_instrument_settlement_prices["settlement_date"] == m2_pd_date
        ],
    )
    need_to_remove_placeholder = False
    if len(tm1_trades) > 0:
        trades_within_window = tm1_trades.loc[
            (
                (tm1_trades["trade_datetime_utc"] < m1_np_timestamp)
                & (tm1_trades["trade_datetime_utc"] > m2_np_timestamp)
            )
        ]
        trades_within_window["trade_date"] = trades_within_window[
            "trade_datetime_utc"
        ].dt.date
        aggregated_trades = get_aggregated_traded_price(trades_within_window)
    else:
        aggregated_trades = pd.DataFrame(
            np.array(
                [
                    (
                        "placeholder this should never match against anything fml",
                        -101,
                        1.0,
                        np.datetime64("2024-01-01 13:00:00"),
                        0.0,
                        0,
                    )
                ],
                dtype=[
                    ("instrument_symbol", "str"),
                    ("portfolio_id", "i4"),
                    ("multiplier", "float_"),
                    ("trade_date", "datetime64[s]"),
                    ("total_traded_cash", "float_"),
                    ("total_quantity_traded", "i4"),
                ],
            )
        )
        need_to_remove_placeholder = True

    pos_vals_joined = (
        m1_pos_val.merge(
            m2_pos_val,
            on=["instrument_symbol", "portfolio_id", "multiplier"],
            how="outer",
            suffixes=("_m1", "_m2"),
        )
        .merge(
            aggregated_trades,
            on=["instrument_symbol", "portfolio_id", "multiplier"],
            how="outer",
            suffixes=("", "_trades"),
        )
        .fillna(0)
        .rename(columns={"quantity_m1": "qty_held"})
        .rename(
            columns={
                "total_quantity_traded": "qty_traded",
                "total_traded_cash": "trade_pnl",
            }
        )
    )
    if need_to_remove_placeholder:
        pos_vals_joined["qty_held"] = pos_vals_joined["qty_held"].astype(int)
        pos_vals_joined["qty_traded"] = pos_vals_joined["qty_traded"].astype(int)
        pos_vals_joined = pos_vals_joined.loc[
            pos_vals_joined["portfolio_id"] != -101
        ].reset_index()

    pos_vals_joined["position_pnl"] = (
        pos_vals_joined["value_at_market_m1"] - pos_vals_joined["value_at_market_m2"]
    )
    pos_vals_joined["total_gross_pnl"] = (
        pos_vals_joined["position_pnl"] + pos_vals_joined["trade_pnl"]
    )

    return pos_vals_joined.drop(
        labels=[
            "position_date_m1",
            "settlement_date_m1",
            "market_price_m1",
            "value_at_market_m1",
            "position_date_m2",
            "quantity_m2",
            "settlement_date_m2",
            "market_price_m2",
            "value_at_market_m2",
        ],
        axis=1,
    )[
        [
            "instrument_symbol",
            "portfolio_id",
            "multiplier",
            "position_pnl",
            "trade_pnl",
            "qty_traded",
            "qty_held",
            "total_gross_pnl",
        ]
    ]
