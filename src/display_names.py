from datetime import datetime
from typing import Dict, Optional

import numpy as np
import sqlalchemy
import upedata.static_data as upestatic
import upedata.template_language.parser as upe_parsing
from dateutil.relativedelta import relativedelta
from sqlalchemy import orm
from upedata.static_data.option import strike_unpacker
from zoneinfo import ZoneInfo


def _process_option_data_to_display_name_map(
    option_sd: upestatic.Option,
) -> Dict[str, str]:
    strikes = np.array(strike_unpacker(option_sd.strike_intervals))
    strikes_formatted = np.where(
        np.floor(strikes) - strikes == 0.0,
        strikes.astype(np.int64).astype(np.str_),
        # similar naive assumption as elsewhere that float strike resolution will be
        # no greater than 5dp ¯\_(ツ)_/¯
        strikes.round(5).astype(np.str_),
    )
    calls = np.full_like(strikes, "c", dtype=np.str_)
    puts = np.full_like(strikes, "p", dtype=np.str_)
    strikes = np.concatenate([strikes, strikes])
    strikes_formatted = np.concatenate([strikes_formatted, strikes_formatted])
    op_types = np.concatenate([calls, puts])
    subtype_spacer = np.full_like(strikes, "-", dtype=np.str_)
    instrument_symbols = np.char.add(
        option_sd.symbol,
        np.char.add(
            np.char.add(subtype_spacer, strikes_formatted),
            np.char.add(subtype_spacer, op_types),
        ),
    )

    # the code called here is quite slow in comparison to everything else,
    # possible that for some loss of generality we could get way quicker!
    # it's from a slower freddie vintage that aimed for maximum safety
    # and cool things like that
    if option_sd.display_name is not None and len(option_sd.display_name) > 0:
        display_names = upe_parsing.generate_display_names_late_evaluation(
            option_sd, strike=strikes_formatted.tolist(), call_or_put=op_types.tolist()
        )
    else:
        display_names = instrument_symbols

    option_display_name_df = dict(zip(instrument_symbols, display_names))

    return option_display_name_df


def get_display_name_map(
    db_session: orm.Session, expiry_cutoff: Optional[datetime] = None
) -> Dict[str, str]:
    if expiry_cutoff is None:
        expiry_cutoff = datetime.now(tz=ZoneInfo("UTC")) - relativedelta(weeks=1)

    get_options_query = sqlalchemy.select(upestatic.Option).where(
        upestatic.Option.expiry > expiry_cutoff
    )
    options_to_generate = db_session.execute(get_options_query).scalars().all()
    display_names: Dict[str, str] = {}
    for option in options_to_generate:
        display_names |= _process_option_data_to_display_name_map(option)

    # pull futures symbols and display names, then merge in the comprehension
    # generated dict into the existing display names from the options data
    get_futures_query = sqlalchemy.select(
        upestatic.Future.symbol, upestatic.Future.display_name
    ).where(upestatic.Future.expiry > expiry_cutoff)
    future_display_map = db_session.execute(get_futures_query).tuples().all()

    display_names |= {
        symbol: display_name
        if (display_name is not None and len(display_name) > 0)
        else symbol
        for symbol, display_name in future_display_map
    }

    return display_names
