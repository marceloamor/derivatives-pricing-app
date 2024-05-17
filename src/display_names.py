import re
import traceback
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np
import orjson
import sqlalchemy
import upedata.static_data as upestatic
import upedata.template_language.parser as upe_parsing
from dateutil.relativedelta import relativedelta
from flask import g
from sqlalchemy import orm
from upedata.static_data.option import strike_unpacker
from zoneinfo import ZoneInfo

try:
    import data_connections
    import parts
except ImportError:
    print(traceback.format_exc())
except Exception:
    print(
        "Unable to import data connections or parts, some tests may fail if "
        "their targets require them"
    )


_SECONDS_PER_SIX_HOURS = 3600 * 6
SUBSTITUTED_SLICE_PATTERN = re.compile(r"@{[^\$]*?}\£")


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

    option_display_name_dict = dict(zip(instrument_symbols, display_names))

    return option_display_name_dict


def trim_option_display_name(option_display_name: str) -> str:
    return re.sub(SUBSTITUTED_SLICE_PATTERN, "", option_display_name).rstrip(" -_.")


def get_display_name_map(
    db_session: orm.Session, expiry_cutoff: Optional[datetime] = None
) -> Dict[str, str]:
    if expiry_cutoff is None:
        expiry_cutoff = datetime.now(tz=ZoneInfo("UTC")) - relativedelta(weeks=2)

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

    print("Regenerated display name map")
    return display_names


def get_sd_sym_display_name_map(
    db_session: orm.Session, expiry_cutoff: Optional[datetime] = None
) -> Dict[str, str]:
    if expiry_cutoff is None:
        expiry_cutoff = datetime.now(tz=ZoneInfo("UTC")) - relativedelta(weeks=2)

    get_options_query = sqlalchemy.select(
        upestatic.Option.symbol, upestatic.Option.display_name
    ).where(upestatic.Option.expiry > expiry_cutoff)
    options_to_process = db_session.execute(get_options_query).tuples().all()
    display_names: Dict[str, str] = {}
    for option_symbol, option_display_name in options_to_process:
        if option_display_name is None:
            display_names[option_symbol] = option_symbol
            continue
        display_names[option_symbol] = trim_option_display_name(option_display_name)

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

    print("Regenerated trim symbol display name map")

    return display_names


def refresh_g_display_names(
    db_session: orm.Session, expiry_cutoff: Optional[datetime] = None
) -> Dict[str, str]:
    g.display_name_map = get_display_name_map(db_session, expiry_cutoff=expiry_cutoff)
    return g.display_name_map


def refresh_g_sym_display_names(
    db_session: orm.Session, expiry_cutoff: Optional[datetime] = None
) -> Dict[str, str]:
    g.display_name_sym_map = get_sd_sym_display_name_map(
        db_session, expiry_cutoff=expiry_cutoff
    )
    return g.display_name_sym_map


def map_symbols_to_display_names(
    symbols: List[str] | str, __refreshed_name_map=False
) -> List[str] | str:
    loaded_from_redis = False
    display_name_map = g.get("display_name_map")
    if isinstance(symbols, str):
        symbols = [symbols]
    if display_name_map is None:
        redis_disp_name_map = data_connections.conn.get(
            "frontend:display_name_map" + parts.dev_key_redis_append
        )
        if redis_disp_name_map is None:
            __refreshed_name_map = True
            with data_connections.shared_session() as session:
                display_names = refresh_g_display_names(session)
                # alright orjson is being a complete country boy here
                # somehow the display_names contains non-str keys but when
                # I iterate through them all and print any that are non-str
                # nothing prints... One of us is completely delusional
                # I've tried saving to files and deserialising/reserialising
                # them, no error, it literally only happens when here in this
                # bit of code doing this bloody thing
                # this is perhaps one of the most annoying bugs i've ever had
                # to deal with all the keys are of str type as far as python
                # `isinstance` is concerned and yet orjson can't get over itself
                # i know the flag slows things down slightly but don't remove it
                # perhaps i've overlooked something stupid and wasted hours of
                # my life...
                serialised_dname_map = orjson.dumps(
                    display_names, option=orjson.OPT_NON_STR_KEYS
                )
                data_connections.conn.set(
                    "frontend:display_name_map" + parts.dev_key_redis_append,
                    serialised_dname_map,
                    ex=_SECONDS_PER_SIX_HOURS,
                )
        else:
            g.display_name_map = orjson.loads(redis_disp_name_map)
            loaded_from_redis = True

    display_name_map = g.get("display_name_map")
    try:
        display_names = [display_name_map[symbol] for symbol in symbols]
    except KeyError as e:
        if not __refreshed_name_map:
            g.pop("display_name_map")
            if loaded_from_redis:
                # in the case we got this bad key from redis we have to clear
                # the redis cache as it's out of date like out local one
                data_connections.conn.delete(
                    "frontend:display_name_map" + parts.dev_key_redis_append
                )
            return map_symbols_to_display_names(symbols)
        e.add_note(
            "Unable to map symbol to display name, this could be "
            "caused by an improperly filled out display name column, "
            "static data being missing for the symbol of interest or "
            "a malformed symbol existing in the table."
        )
        raise e
    if len(display_names) == 1:
        display_names = display_names[0]
    return display_names


def map_sd_exp_symbols_to_display_names(
    symbols: List[str] | str, __refreshed_name_map=False
) -> List[str] | str:
    loaded_from_redis = False
    display_name_map = g.get("display_name_sym_map")
    if isinstance(symbols, str):
        symbols = [symbols]
    if display_name_map is None:
        redis_disp_name_map = data_connections.conn.get(
            "frontend:display_name_sym_map" + parts.dev_key_redis_append
        )
        if redis_disp_name_map is None:
            __refreshed_name_map = True
            with data_connections.shared_session() as session:
                display_names = refresh_g_sym_display_names(session)
                serialised_dname_map = orjson.dumps(
                    display_names, option=orjson.OPT_NON_STR_KEYS
                )
                data_connections.conn.set(
                    "frontend:display_name_sym_map" + parts.dev_key_redis_append,
                    serialised_dname_map,
                    ex=_SECONDS_PER_SIX_HOURS,
                )
        else:
            g.display_name_sym_map = orjson.loads(redis_disp_name_map)
            loaded_from_redis = True

    display_name_map = g.get("display_name_sym_map")
    try:
        display_names = [display_name_map[symbol] for symbol in symbols]
    except KeyError as e:
        if not __refreshed_name_map:
            g.pop("display_name_sym_map")
            if loaded_from_redis:
                # in the case we got this bad key from redis we have to clear
                # the redis cache as it's out of date like out local one
                data_connections.conn.delete(
                    "frontend:display_name_sym_map" + parts.dev_key_redis_append
                )
            return map_sd_exp_symbols_to_display_names(symbols)
        e.add_note(
            "Unable to map symbol to display name, this could be "
            "caused by an improperly filled out display name column, "
            "static data being missing for the symbol of interest or "
            "a malformed symbol existing in the table."
        )
        raise e
    if len(display_names) == 1:
        display_names = display_names[0]
    return display_names
