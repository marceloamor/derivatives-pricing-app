import logging
import traceback
from datetime import datetime
from functools import partial
from typing import Callable, Dict, List, Set, Tuple

import dash_bootstrap_components as dbc
import pandas as pd
import sftp_utils
import sqlalchemy
import upedata.static_data as upe_static
from dash import Input, Output, State, dash_table, dcc, html
from dateutil.relativedelta import relativedelta
from parts import build_georgia_symbol_from_rjo, shared_engine, topMenu
from sqlalchemy import orm

logger = logging.getLogger("frontend")


def multiply_rjo_positions(rjo_row: pd.Series) -> int:
    pos = rjo_row["quantity"]
    if rjo_row["buysellcode"] == 2:
        pos = pos * -1
    return pos


def reconcile_rjo(
    exchange_symbol: str, rjo_symbol_map: Dict[str, str]
) -> Tuple[str, pd.DataFrame]:
    rjo_platform_map: Dict[int, str] = {}
    with shared_engine.connect() as connection:
        for portfolio_id, platform_account_id in connection.execute(
            sqlalchemy.text(
                "SELECT portfolio_id, platform_account_id FROM "
                "platform_account_portfolio_associations WHERE platform = 'RJO'"
            )
        ):
            rjo_platform_map[portfolio_id] = platform_account_id
        georgia_pos_df: pd.DataFrame = pd.read_sql(
            sqlalchemy.text("SELECT * FROM positions WHERE net_quantity != 0"),
            connection,
        )
    rjo_symbols = []
    for rjo_symbol, georgia_symbol in rjo_symbol_map.items():
        if georgia_symbol.startswith(exchange_symbol):
            rjo_symbols.append(rjo_symbol)
    georgia_pos_df = georgia_pos_df[
        georgia_pos_df["instrument_symbol"].str.startswith(exchange_symbol)
    ]
    georgia_pos_df["accountnumber"] = georgia_pos_df["portfolio_id"].map(
        rjo_platform_map
    )
    georgia_pos_df.set_index(keys=["instrument_symbol", "accountnumber"], inplace=True)

    rjo_pos_df, rjo_filename = sftp_utils.fetch_latest_rjo_export(
        r"UPETRADING_csvnpos_npos_%Y%m%d.csv"
    )
    rjo_pos_df = rjo_pos_df[rjo_pos_df["Record Code"] == "P"]
    rjo_pos_df = rjo_pos_df[
        rjo_pos_df["Account Number"].isin(rjo_platform_map.values())
    ]
    rjo_pos_df = rjo_pos_df[rjo_pos_df["Contract Code"].isin(rjo_symbols)]
    rjo_pos_df.columns = rjo_pos_df.columns.str.replace(" ", "")
    rjo_pos_df.columns = rjo_pos_df.columns.str.lower()
    rjo_pos_df["net_quantity"] = rjo_pos_df.apply(multiply_rjo_positions, axis=1)

    product_month_to_expiry_map: Dict[str, Dict[str, datetime]] = {}
    with orm.Session(shared_engine) as session:
        exchange_orm = session.get(upe_static.Exchange, exchange_symbol)
        for product in exchange_orm.products:
            product_month_to_expiry_map[product.symbol] = {}
            # if there's more than one expiry in a given month for a product's
            # futures then this won't work and we'll be in pain, since RJO's
            # file standard is complete hog
            month_expiry_dict_count: Dict[str, int] = {}
            for future in sorted(product.futures, key=lambda fut: fut.expiry):
                ym_formatted = future.expiry.strftime(r"%Y%m")
                product_month_to_expiry_map[product.symbol][ym_formatted] = (
                    future.expiry
                )
                next_month_str = (future.expiry + relativedelta(months=1)).strftime(
                    r"%Y%m"
                )
                # prev_month_str = (future.expiry - relativedelta(month=1)).strftime(
                #     r"%Y%m"
                # )
                try:
                    _ = product_month_to_expiry_map[product.symbol][next_month_str]
                except KeyError:
                    product_month_to_expiry_map[product.symbol][next_month_str] = (
                        future.expiry
                    )
            #     try:
            #         month_expiry_dict_count[ym_formatted] += 1
            #     except KeyError:
            #         month_expiry_dict_count[ym_formatted] = 1
            # for ym_formatted, expiries in month_expiry_dict_count.items():
            #     if expiries > 1:
            #         del product_month_to_expiry_map[product.symbol][ym_formatted]

    georgia_from_rjo_func_partial = partial(
        build_georgia_symbol_from_rjo, rjo_symbol_map, product_month_to_expiry_map
    )
    rjo_pos_df["instrument_symbol"] = rjo_pos_df.apply(
        georgia_from_rjo_func_partial, axis=1
    )
    rjo_pos_df.set_index(keys=["instrument_symbol", "accountnumber"], inplace=True)
    rjo_pos_df = rjo_pos_df[["net_quantity"]]
    rjo_pos_df = rjo_pos_df.groupby(
        ["instrument_symbol", "accountnumber"], as_index=True
    ).agg({"net_quantity": "sum"})

    combined_pos_df = rjo_pos_df[["net_quantity"]].merge(
        georgia_pos_df[["net_quantity"]],
        how="outer",
        left_index=True,
        right_index=True,
        suffixes=("_RJO", "_UPE"),
    )
    combined_pos_df.fillna(0, inplace=True)

    # calc diff
    combined_pos_df["diff"] = (
        combined_pos_df["net_quantity_RJO"] - combined_pos_df["net_quantity_UPE"]
    )

    # return only rows with a non 0 diff
    combined_pos_df = combined_pos_df[combined_pos_df["diff"] != 0]
    return rjo_filename, combined_pos_df


DESTINATION_REC_MAP: Dict[
    str, Callable[[str, Dict[str, str]], Tuple[str, pd.DataFrame]]
] = {"rjo": reconcile_rjo}


def initialise_callbacks(app):
    @app.callback(
        [
            Output("rec-exchange-dropdown", "options"),
            Output("rec-exchange-dropdown", "disabled"),
            Output("rec-exchange-dropdown", "value"),
            Output("rec-exchange-destination-map-store", "data"),
            Output("rec-third-party-symbol-map", "data"),
        ],
        Input("rec-page-10min-refresh", "n_intervals"),
        State("rec-exchange-dropdown", "value"),
    )
    def populate_exchange_dropdown(n_intervals, current_selected):
        dropdown_options: List[Dict[str, str]] = []
        exchange_destination_map: Dict[str, List[str]] = {}
        values = []
        third_party_symbol_map: Dict[str, Dict[str, str]] = {}
        with orm.Session(shared_engine) as session:
            for exchange in (
                session.execute(sqlalchemy.select(upe_static.Exchange)).scalars().all()
            ):
                exchange_destinations: Set[str] = set()

                for product in exchange.products:
                    for third_party_symbol in product.third_party_symbols:
                        exchange_destinations.add(
                            third_party_symbol.platform_name.name.lower()
                        )
                        try:
                            third_party_symbol_map[
                                third_party_symbol.platform_name.name.lower()
                            ][
                                third_party_symbol.platform_symbol
                            ] = third_party_symbol.product_symbol
                        except KeyError:
                            third_party_symbol_map[
                                third_party_symbol.platform_name.name.lower()
                            ] = {
                                third_party_symbol.platform_symbol: third_party_symbol.product_symbol
                            }
                if len(exchange_destinations) > 0:
                    dropdown_options.append(
                        {"label": exchange.name, "value": exchange.symbol}
                    )
                    values.append(exchange.symbol)
                exchange_destination_map[exchange.symbol] = list(exchange_destinations)

        default = (
            current_selected
            if current_selected in values
            else dropdown_options[0]["value"]
        )
        return (
            dropdown_options,
            False,
            default,
            exchange_destination_map,
            third_party_symbol_map,
        )

    @app.callback(
        [
            Output("rec-destination-dropdown", "options"),
            Output("rec-destination-dropdown", "disabled"),
            Output("rec-destination-dropdown", "value"),
        ],
        Input("rec-exchange-dropdown", "value"),
        State("rec-destination-dropdown", "value"),
        State("rec-exchange-destination-map-store", "data"),
    )
    def populate_destination_dropdown(
        exchange_symbol: str,
        current_selected_destination: str,
        destination_map_data: Dict[str, List[str]],
    ):
        if exchange_symbol is None:
            return [], True, None

        dropdown_destination_list = destination_map_data[exchange_symbol]
        dropdown_destinations = [
            {"label": destination.upper(), "value": destination.lower()}
            for destination in dropdown_destination_list
        ]
        current_selected_destination = (
            current_selected_destination
            if current_selected_destination in dropdown_destination_list
            else dropdown_destination_list[0]
        )

        return dropdown_destinations, False, current_selected_destination

    @app.callback(
        [
            Output("rec-datatable-spinner", "children"),
        ],
        [
            Input("rec-generate-rec-button", "n_clicks"),
            State("rec-exchange-dropdown", "value"),
            State("rec-destination-dropdown", "value"),
            State("rec-third-party-symbol-map", "data"),
        ],
    )
    def calculate_rec(
        button_clicks,
        exchange_dropdown_value,
        destination_dropdown_value,
        third_party_symbol_map,
    ):
        if button_clicks is None:
            return [html.Br()]
        try:
            destination_rec_function = DESTINATION_REC_MAP[
                destination_dropdown_value.lower()
            ]
        except KeyError:
            return [f"Destination {destination_dropdown_value.upper()} not supported"]

        try:
            rjo_filename, rec_dataframe = destination_rec_function(
                exchange_dropdown_value,
                third_party_symbol_map[destination_dropdown_value.lower()],
            )
        except KeyError:
            logger.exception(
                "KeyError encountered when attempting to reconcile %s on %s",
                exchange_dropdown_value,
                destination_dropdown_value,
            )
            return [
                "Something didn't match up, likely a product missing from mapping table!\n\n"
                + traceback.format_exc()
            ]
        except Exception:
            logger.exception("Exception encountered running position reconciliation")
            return [traceback.format_exc()]
        columns = [
            {"id": "instrument_symbol", "name": "Instrument"},
            {"id": "accountnumber", "name": "Account"},
            {"id": "net_quantity_UPE", "name": "Georgia"},
            {
                "id": f"net_quantity_{destination_dropdown_value.upper()}",
                "name": destination_dropdown_value.upper(),
            },
            {"id": "diff", "name": "Diff"},
        ]
        return_datatable = dash_table.DataTable(
            id="rec-table",
            data=rec_dataframe.reset_index().to_dict("records"),
            columns=columns,
        )
        return [html.Div([rjo_filename, return_datatable])]


layout = html.Div(
    [
        topMenu("Rec"),
        html.Div(
            [
                dbc.Row(
                    [
                        dbc.Col(
                            dcc.Dropdown(
                                id="rec-exchange-dropdown",
                                options=[],
                                style={"width": "10em"},
                                disabled=True,
                                clearable=False,
                            ),
                            width="auto",
                        ),
                        dbc.Col(
                            dcc.Dropdown(
                                id="rec-destination-dropdown",
                                disabled=True,
                                options=[],
                                style={"width": "10em"},
                                clearable=False,
                            ),
                            width="auto",
                        ),
                        dbc.Col(
                            dbc.Button(
                                "Reconcile",
                                id="rec-generate-rec-button",
                            )
                        ),
                    ],
                    class_name="my-3",
                ),
                dbc.Row(
                    dbc.Spinner(
                        id="rec-datatable-spinner",
                        children=[html.Br()],
                        show_initially=False,
                    ),
                ),
            ],
            className="mx-3",
        ),
        dcc.Interval(id="rec-page-10min-refresh", interval=10 * 60 * 1013),
        dcc.Store(id="rec-exchange-destination-map-store"),
        dcc.Store(id="rec-third-party-symbol-map"),
    ],
)
