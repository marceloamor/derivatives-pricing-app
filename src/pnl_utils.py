from datetime import datetime

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
    joined_pos_price = mark_position.join(
        mark_price, on="instrument_symbol", how="inner"
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
    `instrument_symbol`, `portfolio_id`, `trade_date`, `total_traded_cash`,
    `total_quantity_traded`
    :rtype: pd.DataFrame
    """
    mark_day_trades["total_traded_cash"] = (
        mark_day_trades["quantity"]
        * mark_day_trades["multiplier"]
        * mark_day_trades["price"]
    )
    mark_day_trades["abs_quantity"] = mark_day_trades["quantity"].abs()
    summed_day_trades = mark_day_trades.groupby(
        by=["instrument_symbol", "portfolio_id", "trade_date"]
    ).sum(numeric_only=True)
    summed_day_trades = summed_day_trades[["total_traded_cash", "abs_quantity"]].rename(
        columns={"abs_quantity": "total_quantity_traded"}
    )
    return summed_day_trades


def get_per_instrument_portfolio_pnl(
    tm1_to_2_dated_pos: pd.DataFrame,
    tm1_trades: pd.DataFrame,
    dated_instrument_settlement_prices: pd.DataFrame,
    rollback_trades_since: datetime,
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
    `settlement_date`, `instrument_symbol`, `settlement_price`
    :type dated_instrument_settlement_prices: pd.DataFrame
    :param rollback_trades_since: The datetime to roll back
    trades data to and ignore positions from
    :type rollback_trades_since: datetime
    :return: Gross P&L information on a per-instrument-portfolio
    basis, with the columns: `instrument_symbol`, `portfolio_id`,
    `multiplier`, `position_pnl`, `trade_pnl`, `qty_traded`,
    `qty_held`, `total_gross_pnl`
    :rtype: pd.DataFrame
    """
    pass
