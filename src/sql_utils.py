# utils to sit on top of SQL ORM to allow better access to DB
# gareth 4/4/2023
from data_connections import engine

import upestatic

from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import sessionmaker
import numpy.typing
import numpy as np
import sqlalchemy

from typing import (
    List,
    Tuple,
)


Base = sqlalchemy.orm.declarative_base()
LegacyBase = sqlalchemy.orm.declarative_base()


class TradesTable(Base):
    __tablename__ = "trades"

    trade_pk = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True)
    trade_datetime_utc = sqlalchemy.Column(TIMESTAMP(timezone=False))
    instrument_symbol = sqlalchemy.Column(sqlalchemy.Text)
    quantity = sqlalchemy.Column(sqlalchemy.Integer)
    price = sqlalchemy.Column(sqlalchemy.Float)
    portfolio_id = sqlalchemy.Column(sqlalchemy.Integer)
    trader_id = sqlalchemy.Column(sqlalchemy.Integer)
    notes = sqlalchemy.Column(sqlalchemy.Text)
    deleted = sqlalchemy.Column(sqlalchemy.Boolean, default=False)
    venue_name = sqlalchemy.Column(sqlalchemy.Text)
    venue_trade_id = sqlalchemy.Column(sqlalchemy.Text)
    counterparty = sqlalchemy.Column(sqlalchemy.Text)


class LegacyTradesTable(LegacyBase):
    __tablename__ = "trades"

    ID = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    dateTime = sqlalchemy.Column(TIMESTAMP(timezone=False))
    instrument = sqlalchemy.Column(sqlalchemy.Text)
    price = sqlalchemy.Column(sqlalchemy.Float)
    quanitity = sqlalchemy.Column(sqlalchemy.Float)
    theo = sqlalchemy.Column(sqlalchemy.Float)
    user = sqlalchemy.Column(sqlalchemy.Text)
    counterPart = sqlalchemy.Column(sqlalchemy.Text)
    Comment = sqlalchemy.Column(sqlalchemy.Text)
    prompt = sqlalchemy.Column(sqlalchemy.Text)
    venue = sqlalchemy.Column(sqlalchemy.Text)
    deleted = sqlalchemy.Column(sqlalchemy.Integer)
    venue_trade_id = sqlalchemy.Column(sqlalchemy.Text)


def strike_unpacker(
    strike_intervals: List[Tuple[float, float]]
) -> numpy.typing.NDArray["np.float64"]:
    """Utility static method that generates a list of strikes based on a packed
    strike interval construct, as found in static data.
    :param strike_intervals: Packed strike interval construct, using the same
    format as found in static data, e.g. [(100, 10), (200, 20), (300, -1)]
    would yield a 1D-list composed of [range(100, 200, 10), range(200, 300, 20), 300].
    A sentinel negative step width is required in the last tuple in the packed
    construct to tell the unpacker where to stop, the strike number this is attached
    to is included in the unpacked list, all strikes are then multiplied by the magnitude
    of this sentinel value (such that decimal strikes can be created).
    :type strike_intervals: List[Tuple[float, float]]
    :return: Unpacked list of strikes
    :rtype: List[float]
    """
    strike_list: List[float] = []
    strike_interval_stepwidth = 1  # initialise to prevent errors on None inputs
    for i, (strike_interval_start, strike_interval_stepwidth) in enumerate(
        strike_intervals
    ):
        if strike_interval_stepwidth < 0:
            strike_list.append(strike_interval_start)
            break
        try:
            strike_list.extend(
                range(
                    strike_interval_start,
                    strike_intervals[i + 1][0],
                    strike_interval_stepwidth,
                )
            )
        except IndexError:
            pass
    return np.array(strike_list) * abs(strike_interval_stepwidth)


# load all products from DB
def loadProducts():
    Session = sessionmaker(bind=engine)

    with Session() as session:
        products = session.query(upestatic.Product).all()
        return products


productList = [
    {"label": product.long_name.title(), "value": product.symbol}
    for product in loadProducts()
]


def strike_range(product):
    Session = sessionmaker(bind=engine)

    with Session() as session:
        intervals = (
            session.query(upestatic.Option.strike_intervals)
            .filter_by(product_symbol=product)
            .all()
        )
        strikes = strike_unpacker(intervals[0][0])
        return strikes
