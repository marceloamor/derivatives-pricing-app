from data_connections import (
    conn,
    engine,
    Session,
    PostGresEngine,
)
from parts import (
    GEORGIA_LME_SYMBOL_VERSION_OLD_NEW_MAP,
    topMenu,
    codeToMonth,
    build_new_lme_symbol_from_old,
    get_valid_counterpart_dropdown_options,
    get_first_wednesday,
)
import sftp_utils
import sql_utils

import upestatic
from upedata import static_data as upe_static
from upedata import dynamic_data as upe_dynamic

from dash.dependencies import Input, Output, State
from dateutil.relativedelta import relativedelta
import dash_bootstrap_components as dbc
from dash import dash_table as dtable
from dash import dcc, html, ctx
from flask import request
import dash_daq as daq
import sqlalchemy.orm
import pandas as pd
import sqlalchemy

from datetime import datetime, date
from typing import List, Dict
from copy import deepcopy
import traceback
import tempfile
import pickle
import time
import json
import os
import re

# georgia_db2_engine = get_new_postgres_db_engine()  # gets prod engine
legacyEngine = PostGresEngine()  # gets legacy engine

ENABLE_CARRY_BOOK = os.getenv("ENABLE_CARRY_BOOK", "false").lower() in [
    "t",
    "y",
    "true",
    "yes",
]
USE_DEV_KEYS = os.getenv("USE_DEV_KEYS", "false").lower() in [
    "t",
    "y",
    "true",
    "yes",
]


dev_key_redis_append = "" if not USE_DEV_KEYS else ":dev"

# METAL_LIMITS = {"lad": 150, "lcu": 90, "lzh": 60, "pbd": 60, "lnd": 90}
METAL_LIMITS_PRE_3M = {"lad": 250, "lcu": 150, "lzh": 150, "pbd": 150, "lnd": 150}
METAL_LIMITS_POST_3M = {"lad": 150, "lcu": 50, "lzh": 75, "pbd": 75, "lnd": 50}

# regex to allow for RJO reporting with C, MC, M3 symbols
market_close_regex = r"^(MC\+[+-]?\d+(\.\d+)?|M3\+[+-]?\d+(\.\d+)?|MC-[+-]?\d+(\.\d+)?|M3-[+-]?\d+(\.\d+)?|C[+-]?\d+(\.\d+)?|[+-]?\d+(\.\d+)?)$|^(MC|M3|C)$"


def get_product_holidays(product_symbol: str, _session=None) -> List[date]:
    """Fetches and returns all FULL holidays associated with a given
    product, ignoring partially weighted holidays

    :param product_symbol: Georgia new symbol for `Product`
    :type product_symbol: str
    :return: List of dates associated with full holidays for the given
    product
    :rtype: List[date]
    """
    product_symbol = product_symbol.lower()
    with Session() as session:
        product: upe_static.Product = session.get(upe_static.Product, product_symbol)
        if product is None and _session is None:
            print(
                f"`get_product_holidays(...)` in lme_carry.py was supplied with "
                f"an old format symbol: {product_symbol}\nbloody migrate "
                f"whatever's calling this!"
            )
            return get_product_holidays(
                GEORGIA_LME_SYMBOL_VERSION_OLD_NEW_MAP[product_symbol.lower()],
                _session=session,
            )
        elif product is None and _session is not None:
            raise KeyError(
                f"Failed to find product: {product_symbol} in new static data"
            )

        valid_holiday_dates = []
        for holiday in product.holidays:
            if holiday.holiday_weight == 1.0:
                valid_holiday_dates.append(holiday.holiday_date)

    return valid_holiday_dates


def gen_conditional_carry_table_style(
    selected_row_ids=[],
    account_selector_value="global",
    selected_metal="copper",
):
    three_m_date = datetime.strptime(conn.get("3m").decode("utf8"), r"%Y%m%d").date()

    conditional_formatting_data = [
        {"if": {"column_id": "date"}, "display": "None"},
        {
            "if": {"column_id": "row-formatter"},
            "display": "None",
        },
        {
            "if": {
                "filter_query": "{row-formatter} contains *",
                "column_id": "id",
            },
            "fontWeight": "900",
            "fontStyle": "italic",
            # "border": "2px solid #3D9970",
            "textDecoration": "underline 1px",
            "backgroundColor": "#3D9970",
            # "textDecorationThickness": "2px",
        },
        {
            "if": {
                "filter_query": "{row-formatter} contains _",
                "column_id": "id",
            },
            "fontWeight": "900",
            "backgroundColor": "#3D9970",
        },
        {
            "if": {"filter_query": "{row-formatter} contains n"},
            "backgroundColor": "#606060",
        },
        {
            "if": {
                "filter_query": "{row-formatter} contains $",
            },
            "borderTop": "2px solid #FF4136",
            "borderBottom": "2px solid #FF4136",
        },
        {
            "if": {
                "filter_query": "{row-formatter} contains +",
                "column_id": "id",
            },
            "fontWeight": "800",
            "border": "2px dashed black",
            "backgroundColor": "#39CCCC",
        },
        {"if": {"row_index": selected_row_ids}, "backgroundColor": "#FF851B"},
    ]
    if account_selector_value in ("global", "carry"):
        limit_abs_level_pre_3m = METAL_LIMITS_PRE_3M[selected_metal]
        limit_abs_level_post_3m = METAL_LIMITS_POST_3M[selected_metal]

        conditional_formatting_data.extend(
            [
                {  # pre 3m, over limit
                    "if": {
                        "filter_query": r"{date} <= "
                        + str(three_m_date)
                        + r" && {total} > "
                        + str(limit_abs_level_pre_3m),
                    },
                    "backgroundColor": "#FF4136",
                    "color": "#FFFFFF",
                },
                {  # pre 3m, under limit * -1
                    "if": {
                        "filter_query": r"{date} <= "
                        + str(three_m_date)
                        + r" && {total} < "
                        + str(-1 * limit_abs_level_pre_3m),
                    },
                    "backgroundColor": "#FF4136",
                    "color": "#FFFFFF",
                },
                {  # post 3m, over limit
                    "if": {
                        "filter_query": r"{date} > "
                        + str(three_m_date)
                        + r" && {total} > "
                        + str(limit_abs_level_post_3m),
                    },
                    "backgroundColor": "#FF4136",
                    "color": "#FFFFFF",
                },
                {  # post 3m, under limit * -1
                    "if": {
                        "filter_query": r"{date} > "
                        + str(three_m_date)
                        + r" && {total} < "
                        + str(-1 * limit_abs_level_post_3m),
                    },
                    "backgroundColor": "#FF4136",
                    "color": "#FFFFFF",
                },
            ]
        )

    return conditional_formatting_data


def gen_tables(holiday_list: List[date], *args, **kwargs):
    """Tables produced by this function have a special formatting column
    that need to be filled for things like third wednesday highlighting etc.
    There are:
        - `n` refers to a non-prompt or weekend, which highlights the entire
        row a grey colour
        - `+` highlights the ID column (which corresponds to the day of the
        month) by giving it a black border and bolding text inside
        - `*` highlights the ID column (see above) with a green highlighting,
        used for highlighting the 3m date and the current date
    """
    now_dt = datetime.utcnow()
    if now_dt.hour > 21:
        now_dt += relativedelta(days=1, hour=1)
    now_date = now_dt.date()
    lme_3m_date = conn.get("3m")
    working_days_passed = 0
    lme_cash_date = now_date + relativedelta(days=1)
    while (
        lme_cash_date.weekday() in [5, 6]
        or lme_cash_date in holiday_list
        or working_days_passed < 1
    ):
        if not (lme_cash_date.weekday() in [5, 6] or lme_cash_date in holiday_list):
            working_days_passed += 1
        lme_cash_date += relativedelta(days=1)
    if lme_3m_date is not None:
        three_m_date = datetime.strptime(lme_3m_date.decode("utf8"), r"%Y%m%d").date()
    else:
        raise ValueError("Unable to retrieve LME 3m date from Redis on `3m` key")
    table_set = []
    table_ids = []
    for i in range(4):
        month_forward_date = now_date.replace(day=1) + relativedelta(months=i)
        current_month_name = month_forward_date.strftime("%B").upper()
        dtable_id = f"carry-data-table-{i+1}"
        table_ids.append(dtable_id)
        pre_gen_table_data = []
        current_date = deepcopy(month_forward_date)
        month_third_wednesday = month_forward_date + relativedelta(
            days=((2 - month_forward_date.weekday() + 7) % 7 + 14)
        )

        while current_date.month == month_forward_date.month:
            row_formatting_string = ""
            # TODO use a match/case when upgraded to py 3.10
            if current_date in holiday_list:
                row_formatting_string = "n"
            elif current_date.weekday() in [5, 6]:
                row_formatting_string = "n"
            else:
                if current_date == three_m_date:
                    row_formatting_string = "*"
                if current_date == now_date:
                    row_formatting_string = "_"
                if current_date == month_third_wednesday:
                    row_formatting_string += "+"
                if current_date == lme_cash_date:
                    row_formatting_string += "$"

            pre_gen_table_data.append(
                {
                    "id": current_date.day,
                    "date": deepcopy(current_date),
                    "row-formatter": row_formatting_string,
                    "net-pos": 0,
                    "total": 0,
                }
            )
            current_date += relativedelta(days=1)

        table_set.append(
            dbc.Col(
                dtable.DataTable(
                    data=pre_gen_table_data,
                    columns=[
                        {
                            "name": [current_month_name, "Day"],
                            "id": "id",
                            "selectable": False,
                        },
                        {
                            "name": [current_month_name, "Date"],
                            "id": "date",
                            "selectable": False,
                        },
                        {
                            "name": [current_month_name, "Row Formatter"],
                            "id": "row-formatter",
                            "selectable": False,
                        },
                        {
                            "name": [current_month_name, "Net"],
                            "id": "net-pos",
                            "selectable": False,
                            "type": "numeric",
                            "format": dtable.Format.Format(
                                decimal_delimiter=".",
                            )
                            .precision(2)
                            .scheme(dtable.Format.Scheme.fixed),
                        },
                        {
                            "name": [current_month_name, "Cum"],
                            "id": "total",
                            "selectable": False,
                            "type": "numeric",
                            "format": dtable.Format.Format(
                                decimal_delimiter=".",
                            )
                            .precision(2)
                            .scheme(dtable.Format.Scheme.fixed),
                        },
                    ],
                    merge_duplicate_headers=True,
                    id=dtable_id,
                    cell_selectable=False,
                    row_selectable="multi",
                    style_data_conditional=gen_conditional_carry_table_style(**kwargs),
                    style_header_conditional=[
                        {"if": {"column_id": "date"}, "display": "None"},
                        {
                            "if": {"column_id": "row-formatter"},
                            "display": "None",
                        },
                    ],
                )
            )
        )
    return table_set, table_ids


def gen_2_year_monthly_pos_table():
    now_dt = datetime.utcnow()
    if now_dt.hour > 21:
        now_dt += relativedelta(days=1, hour=1)
    today_date = now_dt.date()

    prebuilt_data = []
    for i in range(18):
        prebuilt_data.append(
            {
                "id": (today_date + relativedelta(months=i)).strftime(r"%b-%y"),
                "net": 0.0,
                "net-pos": 0.0,
                "total": 0.0,
                "cumulative": 0.0,
            }
        )
    monthly_running_table = dtable.DataTable(
        data=prebuilt_data[4:],
        columns=[
            {
                "name": "",
                "id": "id",
                "selectable": False,
            },
            {
                "name": "date",
                "id": "date",
                "selectable": False,
            },
            {
                "name": "row-formatter",
                "id": "row-formatter",
                "selectable": False,
            },
            {
                "name": "Net",
                "id": "net-pos",
                "selectable": False,
                "type": "numeric",
                "format": dtable.Format.Format(
                    decimal_delimiter=".",
                )
                .precision(2)
                .scheme(dtable.Format.Scheme.fixed),
            },
            {
                "name": "Cum",
                "id": "total",
                "selectable": False,
                "type": "numeric",
                "format": dtable.Format.Format(
                    decimal_delimiter=".",
                )
                .precision(2)
                .scheme(dtable.Format.Scheme.fixed),
            },
        ],
        cell_selectable=False,
        row_selectable="multi",
        id="monthly-running-table",
        style_header_conditional=[
            {"if": {"column_id": "date"}, "display": "None"},
            {
                "if": {"column_id": "row-formatter"},
                "display": "None",
            },
        ],
    )
    return monthly_running_table


def update_carry_rows_to_match_counterparties_up(
    trade_table_data, prev_trade_table_data, selected_rows
):
    carry_link_update_data_map = {}
    updated_row_data = None
    updated_carry_link = None
    updated_row_element = None
    for i in range(len(trade_table_data)):
        row_new = trade_table_data[i]
        row_old = prev_trade_table_data[i]
        if row_new["Carry Link"] is None or row_new["Carry Link"] == "":
            continue

        row_update_data = {
            "updated_column_id": None,
            "row_index": i,
            "updated_data": None,
        }
        if row_new["Carry Link"] == row_old["Carry Link"]:
            for linked_column in ["Qty", "Account ID", "Counterparty"]:
                if row_new[linked_column] != row_old[linked_column]:
                    row_update_data["updated_column_id"] = linked_column
                    row_update_data["updated_data"] = row_new[linked_column]
                    updated_carry_link = row_new["Carry Link"]
                    updated_data = row_new[linked_column]
                    if linked_column == "Qty":
                        updated_data *= -1
                    updated_row_element = [linked_column, updated_data]
            try:
                carry_link_update_data_map[row_new["Carry Link"]].append(
                    row_update_data
                )
            except KeyError:
                carry_link_update_data_map[row_new["Carry Link"]] = [row_update_data]

    if updated_carry_link is not None:
        if len(carry_link_update_data_map[updated_carry_link]) > 1:
            for updated_row_data in carry_link_update_data_map[updated_carry_link]:
                if updated_row_data["updated_column_id"] is None:
                    trade_table_data[updated_row_data["row_index"]][
                        updated_row_element[0]
                    ] = updated_row_element[1]

    return trade_table_data, selected_rows


def cleanup_trade_data_table(trade_table_data):
    for i in range(len(trade_table_data)):
        trade_table_data_row = trade_table_data[i]
        trade_table_data_row["Qty"] = round(float(trade_table_data_row["Qty"]))
        if type(trade_table_data_row["Basis"]) != str:
            trade_table_data_row["Basis"] = round(
                float(trade_table_data_row["Basis"]), 2
            )
        try:
            trade_table_data_row["Carry Link"] = int(trade_table_data_row["Carry Link"])
        except TypeError:
            trade_table_data_row["Carry Link"] = None
        trade_table_data[i] = trade_table_data_row
    return trade_table_data


def convert_legacy_georgia_product_symbol_to_datetime(symbol):
    split_symbol = symbol.split(" ")
    if len(split_symbol) == 2:
        return datetime.strptime(split_symbol[1], r"%Y-%m-%d").date()
    elif len(split_symbol) == 1:
        temp_dt = datetime.strptime(
            f"01-{codeToMonth(symbol.upper())}-2{symbol[-1]}".lower(), r"%d-%b-%y"
        )
        third_wed = temp_dt + relativedelta(days=((2 - temp_dt.weekday() + 7) % 7 + 14))
        return third_wed.date()
    else:
        # in case a new symbol gets in here somehow, you never know
        return datetime.strptime(split_symbol[2], r"%y-%m-%d").date()


def initialise_callbacks(app):
    # This absolute monstrosity can be rewritten when we upgrade dash to 2.9
    # in the meantime, this blight shall remain upon this codebase :o
    @app.callback(
        [
            Output("carry-data-table-1", "style_data_conditional"),
            Output("carry-data-table-2", "style_data_conditional"),
            Output("carry-data-table-3", "style_data_conditional"),
            Output("carry-data-table-4", "style_data_conditional"),
            Output("monthly-running-table", "style_data_conditional"),
            Output("carry-data-table-1", "selected_rows"),
            Output("carry-data-table-2", "selected_rows"),
            Output("carry-data-table-3", "selected_rows"),
            Output("carry-data-table-4", "selected_rows"),
            Output("monthly-running-table", "selected_rows"),
            Output("selected-carry-dates", "data"),
        ],
        [
            Input("carry-data-table-1", "selected_rows"),
            Input("carry-data-table-2", "selected_rows"),
            Input("carry-data-table-3", "selected_rows"),
            Input("carry-data-table-4", "selected_rows"),
            Input("monthly-running-table", "selected_rows"),
            Input("account-selector", "value"),
            Input("carry-portfolio-selector", "value"),
            State("carry-data-table-1", "data"),
            State("carry-data-table-2", "data"),
            State("carry-data-table-3", "data"),
            State("carry-data-table-4", "data"),
            State("monthly-running-table", "data"),
            State("carry-data-table-1", "style_data_conditional"),
            State("carry-data-table-2", "style_data_conditional"),
            State("carry-data-table-3", "style_data_conditional"),
            State("carry-data-table-4", "style_data_conditional"),
            State("monthly-running-table", "style_data_conditional"),
            State("selected-carry-dates", "data"),
        ],
    )
    def row_selection_formatter(
        selected_row_indices_1: List[int],
        selected_row_indices_2: List[int],
        selected_row_indices_3: List[int],
        selected_row_indices_4: List[int],
        selected_row_indices_monthly: List[int],
        selected_account: str,
        selected_metal: str,
        table_data_1: List,
        table_data_2: List,
        table_data_3: List,
        table_data_4: List,
        table_data_monthly: List,
        table_conditional_style_1,
        table_conditional_style_2,
        table_conditional_style_3,
        table_conditional_style_4,
        table_conditional_style_monthly,
        selected_carry_dates: List[Dict[str, int]],
    ):
        trigger_table_id = ctx.triggered_id

        if selected_carry_dates is None:
            selected_carry_dates = []

        if trigger_table_id is None:
            base_conditional_style = gen_conditional_carry_table_style(
                account_selector_value=selected_account, selected_metal=selected_metal
            )
            startup_structure = [base_conditional_style for i in range(5)]
            # 5 to account for the selected-carry-dates data that also needs
            # to be pushed
            startup_structure.extend([[] for i in range(6)])
            return tuple(startup_structure)

        combined_table_map = {
            "carry-data-table-1": [
                selected_row_indices_1,
                table_data_1,
                table_conditional_style_1,
            ],
            "carry-data-table-2": [
                selected_row_indices_2,
                table_data_2,
                table_conditional_style_2,
            ],
            "carry-data-table-3": [
                selected_row_indices_3,
                table_data_3,
                table_conditional_style_3,
            ],
            "carry-data-table-4": [
                selected_row_indices_4,
                table_data_4,
                table_conditional_style_4,
            ],
            "monthly-running-table": [
                selected_row_indices_monthly,
                table_data_monthly,
                table_conditional_style_monthly,
            ],
        }
        if (
            trigger_table_id == "account-selector"
            or trigger_table_id == "carry-portfolio-selector"
        ):
            for table_id, table_combined_data in combined_table_map.items():
                combined_table_map[table_id][2] = gen_conditional_carry_table_style(
                    table_combined_data[0],
                    account_selector_value=selected_account,
                    selected_metal=selected_metal,
                )
        else:
            selected_row_indices, table_data, _ = combined_table_map[trigger_table_id]
            selected_row_indices = [] if None else selected_row_indices

            while len(selected_row_indices) > 2:
                del selected_row_indices[-1]

            # using direct copies is possible because this triggers on a per-select
            # basis, so maximum change will be one element on each call within all
            # these loops, there are likely further optimisations that can be made
            for i, selected_index in enumerate(selected_row_indices[:]):
                try:
                    if table_data[selected_index]["row-formatter"] == "n":
                        del selected_row_indices[i]
                except:
                    pass

            final_row_index_already_selected = False
            for i, selected_carry_date_dict in enumerate(selected_carry_dates[:]):
                if selected_carry_date_dict["table_id"] == trigger_table_id:
                    if selected_carry_date_dict["row_id"] not in selected_row_indices:
                        del selected_carry_dates[i]
                    elif selected_carry_date_dict["row_id"] == selected_row_indices[-1]:
                        final_row_index_already_selected = True

            if len(selected_carry_dates) < 2:
                if (
                    not final_row_index_already_selected
                    and len(selected_row_indices) > 0
                ):
                    selected_carry_dates.append(
                        {
                            "table_id": trigger_table_id,
                            "row_id": selected_row_indices[-1],
                            "row_data": table_data[selected_row_indices[-1]],
                        }
                    )
            elif not final_row_index_already_selected and len(selected_row_indices) > 0:
                del selected_row_indices[-1]

            combined_table_map[trigger_table_id][0] = selected_row_indices
            combined_table_map[trigger_table_id][2] = gen_conditional_carry_table_style(
                selected_row_indices,
                selected_metal=selected_metal,
                account_selector_value=selected_account,
            )

        output_pre_structure = {"indices": [], "c_formatting": []}
        for table_id in [
            "carry-data-table-1",
            "carry-data-table-2",
            "carry-data-table-3",
            "carry-data-table-4",
            "monthly-running-table",
        ]:
            mapped_table_info = combined_table_map[table_id]
            output_pre_structure["indices"].append(mapped_table_info[0])
            output_pre_structure["c_formatting"].append(mapped_table_info[2])
        output_list = (
            output_pre_structure["c_formatting"]
            + output_pre_structure["indices"]
            + [selected_carry_dates]
        )

        return tuple(output_list)

    @app.callback(
        [
            Output("create-carry-button", "disabled"),
            Output("create-outright-button", "disabled"),
            Output("carry-basis-input", "disabled"),
            Output("carry-spread-input", "disabled"),
            Output("carry-quantity-input", "disabled"),
            Output("carry-quantity-input", "value"),
            Output("carry-basis-input", "value"),
            Output("carry-spread-input", "value"),
        ],
        Input("selected-carry-dates", "data"),
        Input("fcp-data", "data"),
        Input("back-switch", "on"),
        State("carry-quantity-input", "value"),
        State("carry-basis-input", "value"),
        State("carry-spread-input", "value"),
    )
    def enable_buttons_inputs_on_leg_selection(
        selected_carry_trade_data,
        fcp_data,
        back_switch,
        carry_quantity,
        carry_basis,
        carry_spread,
    ):
        carry_legs = len(selected_carry_trade_data)

        if carry_legs == 2:
            sorted_legs = sorted(
                selected_carry_trade_data,
                key=lambda leg_info: datetime.strptime(
                    leg_info["row_data"]["date"], r"%Y-%m-%d"
                ),
            )
            try:
                # front vs back leg switching
                front = 1 if back_switch else 0
                back = 0 if back_switch else 1

                carry_basis = round(
                    fcp_data[
                        datetime.strptime(
                            sorted_legs[front]["row_data"]["date"], r"%Y-%m-%d"
                        ).strftime(r"%Y%m%d")
                    ],
                    2,
                )
                carry_spread = round(
                    fcp_data[
                        datetime.strptime(
                            sorted_legs[back]["row_data"]["date"], r"%Y-%m-%d"
                        ).strftime(r"%Y%m%d")
                    ]
                    - carry_basis,
                    2,
                )
                if back_switch:
                    carry_spread *= -1
            except KeyError:
                pass

            return (
                False,
                True,
                False,
                False,
                False,
                carry_quantity,
                carry_basis,
                carry_spread,
            )
        elif carry_legs == 1:
            try:
                carry_basis = fcp_data[
                    datetime.strptime(
                        selected_carry_trade_data[0]["row_data"]["date"],
                        r"%Y-%m-%d",
                    ).strftime(r"%Y%m%d")
                ]
            except KeyError:
                pass

            return (
                True,
                False,
                False,
                True,
                False,
                carry_quantity,
                carry_basis,
                None,
            )
        else:
            return True, True, True, True, True, None, None, None

    @app.callback(
        [
            Output("submit-carry-trade", "disabled"),
            Output("report-carry-trade", "disabled"),
            Output("delete-carry-trades", "disabled"),
        ],
        [
            Input("carry-trade-data-table", "selected_rows"),
            Input("carry-trade-data-table", "data"),
        ],
    )
    def enable_trade_buttons_on_trade_selection(selected_trade_rows, trade_table_data):
        # validate instrument names
        # for i in selected_trade_rows:
        #     if (
        #         build_new_lme_symbol_from_old(trade_table_data[i]["Instrument"])
        #         == "error"
        #     ):
        #         return True, True, False

        selected_trade_rows = [] if selected_trade_rows is None else selected_trade_rows
        trade_table_data = [] if trade_table_data is None else trade_table_data
        carry_link_matchoff_dict = {}
        market_close_symbol_used = False
        for selected_index in selected_trade_rows:
            if trade_table_data[selected_index]["Carry Link"] is not None:
                try:
                    carry_link_matchoff_dict[
                        trade_table_data[selected_index]["Carry Link"]
                    ].append(trade_table_data[selected_index]["Qty"])
                except KeyError:
                    carry_link_matchoff_dict[
                        trade_table_data[selected_index]["Carry Link"]
                    ] = [trade_table_data[selected_index]["Qty"]]
            else:
                carry_link_matchoff_dict[0] = [0, 0]

            if (
                trade_table_data[selected_index]["Counterparty"] == ""
                or trade_table_data[selected_index]["Counterparty"] is None
            ):
                return True, True, False

            if not re.match(
                market_close_regex, str(trade_table_data[selected_index]["Basis"])
            ):
                return True, True, False
            if str(trade_table_data[selected_index]["Basis"])[0] == "C":
                market_close_symbol_used = True

        for carry_quantities in carry_link_matchoff_dict.values():
            if sum(carry_quantities) != 0 or len(carry_quantities) != 2:
                return True, True, False

        if market_close_symbol_used:
            return True, False, False

        if not carry_link_matchoff_dict:
            return True, True, True
        else:
            return False, False, False

    @app.callback(
        [
            Output("carry-trade-data-table", "data"),
            Output("carry-trade-data-table", "selected_rows"),
        ],
        [
            Input("create-carry-button", "n_clicks"),
            Input("create-outright-button", "n_clicks"),
            Input("delete-carry-trades", "n_clicks"),
            Input("carry-trade-data-table", "data"),
            State("selected-carry-dates", "data"),
            State("carry-basis-input", "value"),
            State("carry-spread-input", "value"),
            State("carry-portfolio-selector", "value"),
            State("account-selector", "value"),
            State("carry-quantity-input", "value"),
            State("carry-trade-data-table", "selected_rows"),
            State("carry-trade-data-table", "data_previous"),
            State("back-switch", "on"),
        ],
    )
    def create_trade(
        create_carry_nclicks,
        create_outright_nclicks,
        delete_trade_nclicks,
        trade_table_data,
        selected_carry_dates,
        basis_price,
        spread_price,
        selected_portfolio,
        selected_account,
        trade_quantity,
        selected_trade_leg_indices,
        prev_carry_trade_table_data,
        back_switch,
    ):
        if ctx.triggered_id == "carry-trade-data-table":
            trade_table_data = cleanup_trade_data_table(trade_table_data)
            return update_carry_rows_to_match_counterparties_up(
                trade_table_data,
                prev_carry_trade_table_data,
                selected_trade_leg_indices,
            )

        trade_table_data = [] if trade_table_data is None else trade_table_data
        selected_trade_leg_indices = (
            [] if selected_trade_leg_indices is None else selected_trade_leg_indices
        )
        basis_price = 0.0 if basis_price is None else basis_price
        spread_price = 0.0 if spread_price is None else spread_price
        trade_quantity = 1 if trade_quantity is None else trade_quantity

        account_id_map = {"all-f": 1, "global": 1, "carry": 2}
        account_id = account_id_map[selected_account]
        try:
            if ctx.triggered_id == "create-carry-button":
                assert len(selected_carry_dates) == 2
                current_carry_link_value = 1
                for existing_trade_row in trade_table_data:
                    if existing_trade_row["Carry Link"] is not None:
                        if existing_trade_row["Carry Link"] >= current_carry_link_value:
                            current_carry_link_value = (
                                existing_trade_row["Carry Link"] + 1
                            )
                sorted_selected_legs = sorted(
                    selected_carry_dates,
                    key=lambda row_data: datetime.strptime(
                        row_data["row_data"]["date"], r"%Y-%m-%d"
                    ),
                )

                trade_row_date_front = sorted_selected_legs[0]["row_data"]["date"]
                trade_row_date_back = sorted_selected_legs[1]["row_data"]["date"]
                # handle front leg scenario
                if not back_switch:
                    trade_table_data.append(
                        {
                            "Instrument": f"{selected_portfolio} {trade_row_date_front}".upper(),
                            "Qty": trade_quantity,
                            "Basis": basis_price,
                            "Carry Link": current_carry_link_value,
                            "Account ID": account_id,
                            "Counterparty": None,
                        }
                    )
                    trade_table_data.append(
                        {
                            "Instrument": f"{selected_portfolio} {trade_row_date_back}".upper(),
                            "Qty": -1 * trade_quantity,
                            "Basis": float(basis_price) + spread_price,
                            "Carry Link": current_carry_link_value,
                            "Account ID": account_id,
                            "Counterparty": None,
                        }
                    )
                # handle back leg scenario
                else:
                    trade_table_data.append(
                        {
                            "Instrument": f"{selected_portfolio} {trade_row_date_front}".upper(),
                            "Qty": trade_quantity,
                            "Basis": float(basis_price) + (-spread_price),
                            "Carry Link": current_carry_link_value,
                            "Account ID": account_id,
                            "Counterparty": None,
                        }
                    )
                    trade_table_data.append(
                        {
                            "Instrument": f"{selected_portfolio} {trade_row_date_back}".upper(),
                            "Qty": -1 * trade_quantity,
                            "Basis": basis_price,
                            "Carry Link": current_carry_link_value,
                            "Account ID": account_id,
                            "Counterparty": None,
                        }
                    )

            elif ctx.triggered_id == "create-outright-button":
                assert len(selected_carry_dates) == 1
                trade_row_date = selected_carry_dates[0]["row_data"]["date"]
                instrument_symbol = f"{selected_portfolio} {trade_row_date}"
                trade_table_data.append(
                    {
                        "Instrument": instrument_symbol.upper(),
                        "Qty": trade_quantity,
                        "Basis": basis_price,  # round(basis_price, 2),
                        "Carry Link": None,
                        "Account ID": account_id,
                        "Counterparty": None,
                    }
                )
            elif ctx.triggered_id == "delete-carry-trades":
                for row_index_to_delete in sorted(
                    selected_trade_leg_indices, reverse=True
                ):
                    del trade_table_data[row_index_to_delete]
                    del selected_trade_leg_indices[
                        selected_trade_leg_indices.index(row_index_to_delete)
                    ]
            else:
                return trade_table_data, selected_trade_leg_indices
        except AssertionError:
            trade_table_data = cleanup_trade_data_table(trade_table_data)
            return trade_table_data, selected_trade_leg_indices

        trade_table_data = cleanup_trade_data_table(trade_table_data)
        return trade_table_data, selected_trade_leg_indices

    @app.callback(
        [
            Output("carry-data-table-1", "data"),
            Output("carry-data-table-2", "data"),
            Output("carry-data-table-3", "data"),
            Output("carry-data-table-4", "data"),
            Output("monthly-running-table", "data"),
        ],
        [
            Input("carry-portfolio-selector", "value"),
            Input("account-selector", "value"),
            Input("position-data-interval", "n_intervals"),
            State("carry-data-table-1", "data"),
            State("carry-data-table-2", "data"),
            State("carry-data-table-3", "data"),
            State("carry-data-table-4", "data"),
            State("monthly-running-table", "data"),
        ],
    )
    def update_carry_table_contents(
        portfolio_selected: str,
        account_selected: str,
        interval_counter: int,
        table_data_1: List,
        table_data_2: List,
        table_data_3: List,
        table_data_4: List,
        monthly_running_table: List,
    ):
        holiday_list = []
        if ctx.triggered_id == "carry-portfolio-selector":
            holiday_list = get_product_holidays(portfolio_selected)
        pipeline = conn.pipeline()
        pipeline.get("positions")
        pipeline.get("greekpositions")
        positions_df, greekpositions_df = pipeline.execute()
        if positions_df is None:
            print("Positions DF was empty in Redis")
            return (
                table_data_1,
                table_data_2,
                table_data_3,
                table_data_4,
                monthly_running_table,
            )
        # can't get greekpos and looking at non-carry-book
        if greekpositions_df is None and account_selected == "global":
            print("Greekpositions DF was empty in Redis")
            return (
                table_data_1,
                table_data_2,
                table_data_3,
                table_data_4,
                monthly_running_table,
            )

        greekpositions_df = greekpositions_df.decode("utf-8")
        greekpositions_df: pd.DataFrame = pd.read_json(greekpositions_df)

        # switching positions to a database call to get around pickling issues
        # with Session() as session:
        #     query = session.query(upe_dynamic.Position).filter(
        #         upe_dynamic.Position.instrument_symbol.like(f"{portfolio_selected}%"),
        #     )
        #     positions_df = session.execute(query)

        positions_df: pd.DataFrame = pickle.loads(positions_df)
        positions_df.columns = positions_df.columns.str.lower()
        positions_df = positions_df[positions_df["quanitity"] != 0]
        positions_df["instrument"] = positions_df["instrument"].str.lower()
        positions_df = positions_df.loc[
            positions_df["instrument"].str.startswith(portfolio_selected)
        ]
        positions_df = positions_df.drop(
            columns=[
                "datetime",
                "settleprice",
                "delta",
                "prompt",
                "third_wed",
                "position_id",
            ]
        )

        if account_selected == "global":
            greekpositions_df = greekpositions_df[
                greekpositions_df["instrument"].str.startswith(portfolio_selected)
            ]
            greekpositions_df = greekpositions_df.loc[:, ["product", "total_fullDelta"]]
            greekpositions_df["dt_date_prompt"] = greekpositions_df.loc[
                :, "product"
            ].apply(convert_legacy_georgia_product_symbol_to_datetime)
            positions_df = greekpositions_df.groupby(
                "dt_date_prompt", as_index=False
            ).sum()
            positions_df["dt_date_prompt"] = pd.to_datetime(
                positions_df["dt_date_prompt"]
            )
            positions_df["day"] = positions_df["dt_date_prompt"].dt.day
            positions_df["month"] = positions_df["dt_date_prompt"].dt.month
            positions_df["year"] = positions_df["dt_date_prompt"].dt.year
            positions_df["quanitity"] = positions_df["total_fullDelta"].round(2)
        elif account_selected == "all-f":
            positions_df = positions_df[
                positions_df["instrument"]
                .str.split(" ")
                .apply(lambda split_symbol: len(split_symbol) == 2)
            ]
            if len(positions_df) == 0:
                # This is here to stop an error caused by the dataframe being empty
                if ctx.triggered_id == "account-selector":
                    front_carry_tables, _ = gen_tables(
                        get_product_holidays(portfolio_selected),
                        account_selector_value=account_selected,
                        selected_metal=portfolio_selected,
                    )
                    two_year_forward_table = gen_2_year_monthly_pos_table()
                    table_data_1 = front_carry_tables[0].children.data
                    table_data_2 = front_carry_tables[1].children.data
                    table_data_3 = front_carry_tables[2].children.data
                    table_data_4 = front_carry_tables[3].children.data
                    monthly_running_table = two_year_forward_table.data
                return (
                    table_data_1,
                    table_data_2,
                    table_data_3,
                    table_data_4,
                    monthly_running_table,
                )

            positions_df["prompt"] = positions_df["instrument"].apply(
                lambda split_symbol: split_symbol.split(" ")[1]
            )
            positions_df["dt_date_prompt"] = pd.to_datetime(
                positions_df["prompt"].apply(
                    lambda prompt_str: datetime.strptime(prompt_str, r"%Y-%m-%d").date()
                )
            )
            positions_df["day"] = positions_df["dt_date_prompt"].dt.day
            positions_df["month"] = positions_df["dt_date_prompt"].dt.month
            positions_df["year"] = positions_df["dt_date_prompt"].dt.year
        elif account_selected == "carry":
            with sqlalchemy.orm.Session(engine) as session:
                stmt = sqlalchemy.text(
                    """
                    SELECT instrument_symbol, net_quantity FROM positions
                        WHERE LEFT(instrument_symbol, 3) = :metal_three_letter
                            AND portfolio_id = 2 
                            AND net_quantity != 0"""
                )
                positions = session.execute(
                    stmt,
                    params={"metal_three_letter": portfolio_selected.lower()},
                )
                positions_df = pd.DataFrame(
                    positions.fetchall(), columns=["instrument_symbol", "net_quantity"]
                )

            positions_df["quanitity"] = positions_df["net_quantity"]
            positions_df["prompt"] = positions_df["instrument_symbol"].apply(
                lambda split_symbol: split_symbol.split(" ")[1]
            )
            positions_df["dt_date_prompt"] = pd.to_datetime(
                positions_df["prompt"].apply(
                    lambda prompt_str: datetime.strptime(prompt_str, r"%Y-%m-%d").date()
                )
            )
            positions_df["day"] = positions_df["dt_date_prompt"].dt.day
            positions_df["month"] = positions_df["dt_date_prompt"].dt.month
            positions_df["year"] = positions_df["dt_date_prompt"].dt.year

        prev_cumulative_count = 0
        for i, table_data in enumerate(
            [
                table_data_1,
                table_data_2,
                table_data_3,
                table_data_4,
            ]
        ):
            for i, data_row in enumerate(table_data):
                row_date = pd.to_datetime(
                    datetime.strptime(data_row["date"], r"%Y-%m-%d").date()
                )
                try:
                    date_qty_net = positions_df[
                        positions_df["dt_date_prompt"] == row_date
                    ]["quanitity"].sum()
                except KeyError:
                    date_qty_net = 0

                data_row["net-pos"] = date_qty_net
                prev_cumulative_count += date_qty_net
                data_row["total"] = prev_cumulative_count
                if holiday_list:
                    if data_row["row-formatter"] == "n" and row_date.weekday() not in [
                        5,
                        6,
                    ]:
                        data_row["row-formatter"] = ""
                    if (
                        row_date.date() in holiday_list
                        and data_row["row-formatter"] != "n"
                    ):
                        data_row["row-formatter"] = "n"

                table_data[i] = data_row
        prev_cumulative_count = 0
        pre_table_date_range_end = datetime.strptime(
            "01-" + monthly_running_table[0]["id"], r"%d-%b-%y"
        ).date() - relativedelta(days=1)
        prev_cumulative_count += positions_df[
            pd.to_datetime(positions_df["dt_date_prompt"]).apply(
                lambda pd_dt: pd_dt.to_pydatetime().date()
            )
            <= pre_table_date_range_end
        ]["quanitity"].sum()
        for i, data_row in enumerate(monthly_running_table):
            row_date = datetime.strptime("01-" + data_row["id"], r"%d-%b-%y").date()
            row_month = row_date.month
            row_year = row_date.year
            third_wed = get_first_wednesday(row_year, row_month) + relativedelta(
                days=14
            )
            data_row["date"] = third_wed.strftime(r"%Y-%m-%d")
            month_position = positions_df[
                (positions_df["month"] == row_month)
                & (positions_df["year"] == row_year)
            ]["quanitity"].sum()
            data_row["net-pos"] = month_position
            prev_cumulative_count += month_position
            data_row["total"] = prev_cumulative_count
            monthly_running_table[i] = data_row

        return (
            table_data_1,
            table_data_2,
            table_data_3,
            table_data_4,
            monthly_running_table,
        )

    @app.callback(
        Output("fcp-data", "data"), Input("carry-portfolio-selector", "value")
    )
    def update_closing_prices_on_portfolio_selection(selected_product):
        georgia_lme_product_map = {
            "lcu": "copper",
            "lad": "aluminium",
            "pbd": "lead",
            "lzh": "zinc",
            "lnd": "nickel",
        }
        lme_product = georgia_lme_product_map[selected_product]
        pipeline = conn.pipeline()
        pipeline.get(f"{lme_product}Prompt")
        pipeline.get(f"{lme_product}Curve")
        metal_fcp_data, full_curve = pipeline.execute()
        full_curve = pickle.loads(full_curve)
        # full_curve = pd.read_pickle(full_curve)
        lme_3m_date = conn.get("3m").decode("utf8")
        if metal_fcp_data is None:
            return []
        fcp_data = json.loads(metal_fcp_data.decode())
        try:
            fcp_data[lme_3m_date] = full_curve.loc[int(lme_3m_date), "price"]
        except KeyError:
            next_prior_date = datetime.strptime(lme_3m_date, r"%Y%m%d") - relativedelta(
                days=1
            )
            while next_prior_date.strftime(r"%Y%m%d") not in list(fcp_data.keys()):
                next_prior_date -= relativedelta(days=1)

            fcp_data[lme_3m_date] = fcp_data[next_prior_date.strftime(r"%Y%m%d")]
        return fcp_data

    @app.callback(
        [
            Output("trade-report-success", "is_open"),
            Output("trade-report-failure", "is_open"),
        ],
        [
            Input("report-carry-trade", "n_clicks"),
            State("carry-trade-data-table", "data"),
            State("carry-trade-data-table", "selected_rows"),
        ],
    )
    def report_carry_trade_rjo(submit_trade_clicks, trade_table_data, selected_rows):
        RJO_COLUMNS = [
            "Type",
            "Client",
            "Buy/Sell",
            "Lots",
            "Commodity",
            "Prompt",
            "Strike",
            "C/P",
            "Price",
            "Broker",
            "Clearer",
            "clearer/executor/normal",
            "Volatility",
            "Hit Account",
            "Price2",
        ]
        LME_METAL_MAP = {
            "LZH": "ZSD",
            "LAD": "AHD",
            "LCU": "CAD",
            "PBD": "PBD",
            "LND": "NID",
        }
        if selected_rows is None or not selected_rows:
            return False, False

        to_send_df = pd.DataFrame(
            columns=RJO_COLUMNS, index=list(range(len(selected_rows)))
        )
        user = request.headers.get("X-MS-CLIENT-PRINCIPAL-NAME")
        if user is None:
            user = "LOCAL TEST"

        routing_dt = datetime.utcnow()
        routing_trade = sftp_utils.add_routing_trade(
            routing_dt, user, "PENDING", "Failed to build formatted trade"
        )

        to_send_df["Client"] = "LJ4UPLME"
        to_send_df["Broker"] = "RJO"
        to_send_df["clearer/executor/normal"] = "clearer"

        for i, selected_index in enumerate(selected_rows):
            trade_data = trade_table_data[selected_index]

            clearer = sftp_utils.get_clearer_from_counterparty(
                trade_data["Counterparty"].upper().strip()
            )
            if clearer is not None:
                to_send_df.loc[i, "Clearer"] = clearer
            else:
                print(
                    f"Unable to find clearer for given counterparty "
                    f"`{trade_data['Counterparty'].upper().strip()}`"
                )
                routing_trade = sftp_utils.update_routing_trade(
                    routing_trade,
                    "FAILED",
                    f"Unable to find clearer for given counterparty "
                    f"`{trade_data['Counterparty'].upper().strip()}`",
                )
                return False, True

            if clearer == "RJO":
                to_send_df.loc[i, "clearer/executor/normal"] = "normal"
                to_send_df.loc[i, "Client"] = (
                    trade_data["Counterparty"].upper().strip()
                    + "_"
                    + to_send_df.loc[i, "Client"]
                )

            try:
                to_send_df.loc[i, "Commodity"] = LME_METAL_MAP[
                    trade_data["Instrument"][:3].upper()
                ]
            except KeyError:
                print(
                    f"Symbol entered incorrectly for LME mapping: `{trade_data['Instrument'].upper()}`"
                    f" parser uses the first three characters of this to find LME symbol."
                )
                routing_trade = sftp_utils.update_routing_trade(
                    routing_trade,
                    "FAILED",
                    f"Invalid symbol found: `{trade_data['Instrument'].upper()}`",
                )
                return False, True

            to_send_df.loc[i, "Price"] = trade_data["Basis"]
            to_send_df.loc[i, "Buy/Sell"] = "B" if int(trade_data["Qty"]) > 0 else "S"
            to_send_df.loc[i, "Lots"] = abs(int(trade_data["Qty"]))
            if trade_data["Carry Link"] is None:
                to_send_df.loc[i, "Hit Account"] = ""
                to_send_df.loc[i, "Type"] = "OUTRIGHT"
            else:
                to_send_df.loc[i, "Hit Account"] = str(trade_data["Carry Link"])
                to_send_df.loc[i, "Type"] = "CARRY"
            to_send_df.loc[i, "Prompt"] = datetime.strptime(
                trade_data["Instrument"].split(" ")[1], r"%Y-%m-%d"
            ).strftime(r"%Y%m%d")
            to_send_df.loc[i, "Strike"] = ""
            to_send_df.loc[i, "C/P"] = ""
            to_send_df.loc[i, "Price2"] = ""
            to_send_df.loc[i, "Volatility"] = ""

        routing_trade = sftp_utils.update_routing_trade(
            routing_trade,
            "PENDING",
            routing_dt,
            trade_table_data[0]["Counterparty"],
        )
        file_name = f"LJ4UPLME_{routing_dt.strftime(r'%Y%m%d_%H%M%S%f')}"
        att_name = file_name + ".csv"

        temp_file_sftp = tempfile.NamedTemporaryFile(
            mode="w+b", dir="./", prefix=f"{file_name}_", suffix=".csv"
        )
        to_send_df.to_csv(temp_file_sftp, mode="b", index=False)

        try:
            sftp_utils.submit_to_stfp(
                "/Allocations",
                att_name,
                temp_file_sftp.name,
            )
        except Exception as e:
            temp_file_sftp.close()
            formatted_traceback = traceback.format_exc()
            routing_trade = sftp_utils.update_routing_trade(
                routing_trade,
                "FAILED",
                error=formatted_traceback,
            )
            return False, True

        routing_trade = sftp_utils.update_routing_trade(
            routing_trade, "ROUTED", error=None
        )

        return True, False

    @app.callback(
        [
            Output("trade-sub-success", "is_open"),
            Output("trade-sub-failure", "is_open"),
        ],
        [
            Input("submit-carry-trade", "n_clicks"),
            State("carry-trade-data-table", "data"),
            State("carry-trade-data-table", "selected_rows"),
        ],
    )
    def book_carry_trade_georgia(submit_trade_clicks, trade_table_data, selected_rows):
        if ctx.triggered_id is None:
            return False, False
        # elif user is None:
        #     print("Unable to retrieve user for trade booking, found None")
        #     return False, True

        user = request.headers.get("X-MS-CLIENT-PRINCIPAL-NAME")
        if user is None:
            user = "TEST"
        packaged_trades_to_send_legacy = []
        packaged_trades_to_send_new = []
        trader_id = 0
        with engine.connect() as pg_db2_connection:
            stmt = sqlalchemy.text(
                "SELECT trader_id FROM traders WHERE email = :user_email"
            )
            result = pg_db2_connection.execute(
                stmt, {"user_email": user.lower()}
            ).scalar_one_or_none()
            if result is None:
                trader_id = -101
            else:
                trader_id = result
        upsert_pos_params = []
        trade_time_ns = time.time_ns()
        for trade_row_index in selected_rows:
            trade_row = trade_table_data[trade_row_index]

            # new_instrument_name = build_new_lme_symbol_from_old(trade_row["Instrument"])
            # if new_instrument_name == "error":
            #     print(
            #         f"Issue building new instrument name for carry booking: `{trade_row['Instrument']}`"
            #     )
            #     return False, True

            processed_user = user.replace(" ", "").split("@")[0]
            georgia_trade_id = f"gcarrylme.{processed_user}.{trade_time_ns}:{selected_rows.index(trade_row_index)}"
            booking_dt = datetime.utcnow()
            packaged_trades_to_send_legacy.append(
                sql_utils.LegacyTradesTable(
                    dateTime=booking_dt,
                    instrument=trade_row["Instrument"].upper(),
                    price=trade_row["Basis"],
                    quanitity=trade_row["Qty"],
                    theo=0.0,
                    user=user,
                    counterPart=trade_row["Counterparty"],
                    Comment="Carry Page",
                    prompt=trade_row["Instrument"].split(" ")[1],
                    venue="Georgia",
                    deleted=0,
                    venue_trade_id=georgia_trade_id,
                )
            )
            packaged_trades_to_send_new.append(
                sql_utils.TradesTable(
                    trade_datetime_utc=booking_dt,
                    instrument_symbol=trade_row["Instrument"].upper(),
                    quantity=trade_row["Qty"],
                    price=trade_row["Basis"],
                    portfolio_id=trade_row["Account ID"],
                    trader_id=trader_id,
                    notes="Carry Page",
                    venue_name="Georgia",
                    venue_trade_id=georgia_trade_id,
                    counterparty=trade_row["Counterparty"],
                )
            )
            upsert_pos_params.append(
                {
                    "qty": trade_row["Qty"],
                    "instrument": trade_row["Instrument"].upper(),
                    "tstamp": booking_dt,
                }
            )

        try:
            with sqlalchemy.orm.Session(engine, expire_on_commit=False) as session:
                session.add_all(packaged_trades_to_send_new)
                session.commit()
        except Exception as e:
            print("Exception while attempting to book trade in new standard table")
            print(traceback.format_exc())
            return False, True
        try:
            with sqlalchemy.orm.Session(legacyEngine) as session:
                session.add_all(packaged_trades_to_send_legacy)
                pos_upsert_statement = sqlalchemy.text(
                    "SELECT upsert_position(:qty, :instrument, :tstamp)"
                )
                _ = session.execute(pos_upsert_statement, params=upsert_pos_params)
                session.commit()
        except Exception as e:
            print("Exception while attempting to book trade in legacy table")
            print(traceback.format_exc())
            for trade in packaged_trades_to_send_new:
                trade.deleted = True
            # to clear up new trades table assuming they were booked correctly
            # on there
            with sqlalchemy.orm.Session(engine) as session:
                session.add_all(packaged_trades_to_send_new)
                session.commit()
            return False, True

        try:
            with legacyEngine.connect() as pg_connection:
                trades = pd.read_sql("trades", pg_connection)
                positions = pd.read_sql("positions", pg_connection)

            trades.columns = trades.columns.str.lower()
            positions.columns = positions.columns.str.lower()

            pipeline = conn.pipeline()
            pipeline.set("trades" + dev_key_redis_append, pickle.dumps(trades))
            pipeline.set("positions" + dev_key_redis_append, pickle.dumps(positions))
            pipeline.execute()
        except Exception as e:
            print("Exception encountered while trying to update redis trades/posi")
            print(traceback.format_exc())
            return False, True

        return True, False


INITIAL_METAL_VALUE = "lcu"
product_dropdown = dcc.Dropdown(
    id="carry-portfolio-selector",
    value=INITIAL_METAL_VALUE,
    options=[
        {"label": "Copper", "value": "lcu"},
        {"label": "Aluminium", "value": "lad"},
        {"label": "Lead", "value": "pbd"},
        {"label": "Zinc", "value": "lzh"},
        {"label": "Nickel", "value": "lnd"},
    ],
    clearable=False,
)
account_dropdown_options = [
    {"label": "All LME", "value": "global"},
    {"label": "All Fut", "value": "all-f"},
]
if ENABLE_CARRY_BOOK:
    account_dropdown_options.append({"label": "Carry", "value": "carry"})
account_dropdown = dcc.Dropdown(
    id="account-selector",
    value="global",
    options=account_dropdown_options,
    clearable=False,
)

basis_input = dcc.Input(
    id="carry-basis-input",
    placeholder="Basis",
    autoComplete="false",
    disabled=True,
    style={"width": "8em"},
    pattern=market_close_regex,
)
spread_input = dcc.Input(
    id="carry-spread-input",
    placeholder="Spread",
    type="number",
    inputMode="numeric",
    autoComplete="false",
    disabled=True,
    step=0.01,
    max=999.99,
    min=-999.99,
    style={"width": "7em"},
    className="mx-3",
)
quantity_input = dcc.Input(
    id="carry-quantity-input",
    placeholder="Qty",
    type="number",
    inputMode="numeric",
    autoComplete="false",
    disabled=True,
    step=1,
    max=999,
    min=-999,
    style={"width": "5em"},
)

create_trade_button_group = dbc.ButtonGroup(
    [
        dbc.Button("Create Outright", id="create-outright-button", disabled=True),
        dbc.Button(
            "Create Carry",
            id="create-carry-button",
            disabled=True,
        ),
    ]
)
trade_button_group = dbc.ButtonGroup(
    [
        dbc.Button("Trade", id="submit-carry-trade", disabled=True),
        dbc.Button("Report", id="report-carry-trade", disabled=True),
    ],
    className="mx-4",
)
delete_trades_button = dbc.Button(
    "Delete",
    id="delete-carry-trades",
    disabled=True,
)

alert_banner_div = html.Div(
    [
        dbc.Alert(
            "Trade Submitted",
            id="trade-sub-success",
            dismissable=True,
            is_open=False,
            duration=5000,
            color="success",
            className="pb-3",
        ),
        dbc.Alert(
            "Trade Submission Failure",
            id="trade-sub-failure",
            dismissable=True,
            is_open=False,
            duration=15000,
            color="danger",
            className="pb-3",
        ),
        dbc.Alert(
            "Trade Reported",
            id="trade-report-success",
            dismissable=True,
            is_open=False,
            duration=5000,
            color="success",
            className="pb-3",
        ),
        dbc.Alert(
            "Trade Report Failure",
            id="trade-report-failure",
            dismissable=True,
            is_open=False,
            duration=15000,
            color="danger",
            className="pb-3",
        ),
    ]
)

trade_table_account_options = []
with engine.connect() as db_conn:
    stmt = sqlalchemy.text(
        "SELECT portfolio_id, display_name FROM portfolios WHERE"
        " LEFT(display_name, 3) = 'LME'"
    )
    result = db_conn.execute(stmt)
    for portfolio_id, display_name in result.fetchall():
        trade_table_account_options.append(
            {"label": display_name, "value": portfolio_id}
        )

trade_table = dtable.DataTable(
    columns=[
        {"id": "Instrument", "name": "Instrument", "editable": False},
        {
            "id": "Qty",
            "name": "Qty",
            "type": "numeric",
            "format": dtable.Format.Format()
            .precision(0)
            .scheme(dtable.Format.Scheme.decimal_integer),
        },
        {
            "id": "Basis",
            "name": "Basis",
            # "type": "numeric",
            "format": dtable.Format.Format(
                decimal_delimiter=".",
                # symbol=dtable.Format.Symbol.yes,
            )
            .precision(2)
            .scheme(dtable.Format.Scheme.fixed)
            # .symbol_prefix("$"),
        },
        {"id": "Account ID", "name": "Account ID", "presentation": "dropdown"},
        {"id": "Counterparty", "name": "Counterparty", "presentation": "dropdown"},
        {"id": "Carry Link", "name": "Carry Link"},
    ],
    id="carry-trade-data-table",
    row_selectable="multi",
    editable=True,
    dropdown={
        "Account ID": {
            "clearable": False,
            "options": trade_table_account_options,
        },
        "Counterparty": {
            "clearable": False,
            "options": get_valid_counterpart_dropdown_options("xlme"),
        },
    },
    style_data_conditional=[
        {"if": {"column_id": "Instrument"}, "backgroundColor": "#f1f1f1"},
    ],
    style_cell={"textAlign": "left"},
)

carry_table_layout, _ = gen_tables(
    get_product_holidays(INITIAL_METAL_VALUE),
    account_selector_value="global",
    selected_metal=INITIAL_METAL_VALUE,
)
monthly_cumulative_table = gen_2_year_monthly_pos_table()
carry_table_layout.append(dbc.Col(monthly_cumulative_table))

# front/back leg
backSwitch = daq.BooleanSwitch(id="back-switch", on=False)
backTooltip = dbc.Tooltip(
    "Front / Back Leg",
    id="tooltip",
    target="back-switch",
)
layout = html.Div(
    [
        topMenu("LME Carry"),
        html.Div(
            [
                dbc.Row(
                    [
                        dbc.Col(
                            product_dropdown,
                            width=1,
                        ),
                        dbc.Col(account_dropdown, width=1),
                        dbc.Col(
                            html.Div(
                                [
                                    dbc.ButtonGroup(
                                        [
                                            basis_input,
                                            spread_input,
                                            quantity_input,
                                            backSwitch,
                                            backTooltip,
                                        ]
                                    )
                                ]
                            )
                        ),
                        # dbc.Col(
                        #     html.Div([basis_input, spread_input, quantity_input]),
                        #     width=4,
                        # ),
                        dbc.Col(
                            html.Div(
                                [
                                    create_trade_button_group,
                                    trade_button_group,
                                ]
                            ),
                            width=5,
                        ),
                        dbc.Col(delete_trades_button, width=1),
                    ],
                    className="pb-3",
                ),
                dbc.Row(alert_banner_div),
                dbc.Row(trade_table, className="pb-3"),
                dbc.Row(carry_table_layout),
            ],
            className="mx-3 my-3",
        ),
        dcc.Store(id="greekpos-store"),
        dcc.Store(id="holiday-store"),
        # Stored in format
        #   [
        #       {"table_id": table_id_1, "row_id": table_1_row_index},
        #       {"table_id": table_id_2, "row_id": table_2_row_index}
        #   ]
        # These can be in the same table and there can only be two
        dcc.Store(id="selected-carry-dates", data=[]),
        dcc.Store(id="fcp-data", data=[]),
        dcc.Interval(id="position-data-interval", interval=3 * 1000),
    ],
)
