import traceback
from datetime import datetime
from typing import Any, Dict, List, Never, Tuple

import dash_bootstrap_components as dbc
import display_names
import numpy as np
import orjson
import pandas as pd
import plotly.graph_objects as go
import sqlalchemy
import upedata.dynamic_data as upedynamic
import upedata.static_data as upestatic
from dash import ctx, dash_table, dcc, html
from dash.dependencies import Input, Output, State
from dateutil import relativedelta
from parts import (
    conn,
    dev_key_redis_append,
    shared_engine,
    shared_session,
    topMenu,
    lme_linear_interpolation_model,
)
from scipy import interpolate
from zoneinfo import ZoneInfo
from icecream import ic


def fit_vals_to_settlement_spline(
    vol_matrix_table_data: List[Dict[str, Any]], selected_rows: List[int]
) -> List[Dict[str, Any]]:
    # this function will need to pull the settlement curve for the specific option
    # and then run a minimiser to find the optimal parameters for the specific vol
    # model/exchange to meet that settlement curve, this may well end up being
    # inaccurate, for LME it will just pull the settlement spline params as they
    # map 1:1 to the model we use

    # collection of options missing settlement params
    options_missing_params = []

    # fit params logic implemented for lme only at the moment
    if vol_matrix_table_data[0]["option_symbol"].lower()[:4] != "xlme":
        print("volMatrix: params fitting not implemented for this exchange")
        return vol_matrix_table_data, options_missing_params
    else:
        with shared_engine.connect() as db_conn:
            for index in selected_rows:
                option_symbol = vol_matrix_table_data[index]["option_symbol"].lower()
                vol_model = vol_matrix_table_data[index]["model_type"]
                vol_surface_id = vol_matrix_table_data[index]["vol_surface_id"]
                sql_query = sqlalchemy.text(
                    "SELECT * FROM lme_settlement_spline_params WHERE option_symbol = :option_symbol ORDER BY settlement_date DESC LIMIT 1"
                )
                settle_params = db_conn.execute(
                    sql_query, {"option_symbol": option_symbol}
                ).fetchone()
                if settle_params is not None:
                    # build dictionary entry and replace current row w settlement params
                    new_row_data = {
                        "+10 DIFF": round(settle_params[6], 5),  # 6
                        "+25 DIFF": round(settle_params[5], 5),  # 5
                        "-10 DIFF": round(settle_params[2], 5),  # 2
                        "-25 DIFF": round(settle_params[3], 5),  # 3
                        "50 Delta": round(settle_params[4], 5),  # 4
                        "model_type": vol_model,
                        "option_symbol": option_symbol.upper(),
                        "vol_surface_id": vol_surface_id,
                    }
                    vol_matrix_table_data[index] = new_row_data
                else:
                    options_missing_params.append(option_symbol.upper())
                    print(f"settle params not found for {option_symbol}")

    return vol_matrix_table_data, options_missing_params


def initialise_callbacks(app):
    @app.callback(
        [
            Output("new-vol-send-failure", "is_open"),
            Output("new-vol-send-success", "is_open"),
        ],
        [
            Input("vol-matrix-submit-params-button", "n_clicks"),
            State("vol-matrix-dynamic-table", "data"),
            State("vol-matrix-product-option-symbol-map", "data"),
            State("vol-matrix-dynamic-table", "selected_rows"),
        ],
    )
    def submit_vol_params(
        submit_vols_button_nclicks: int,
        vol_matrix_table: List[Dict[str, Any]],
        backend_stored_data,
        selected_rows: List[int],
    ):
        if (
            submit_vols_button_nclicks == 0
            or vol_matrix_table is None
            or submit_vols_button_nclicks is None
            or not backend_stored_data
        ):
            return False, False
        vol_surface_param_updates: List[Dict[str, Any]] = []
        for i in selected_rows:
            vol_matrix_row = vol_matrix_table[i]
            new_row_data = {"vol_surface_id": vol_matrix_row["vol_surface_id"]}
            current_params: Dict[str, float] = {}
            for col_key, col_value in vol_matrix_row.items():
                if col_key in (
                    "option_symbol",
                    "option_display_name",
                    "model_type",
                    "vol_surface_id",
                ):
                    continue
                current_params[col_key] = float(col_value)
            new_row_data["params"] = current_params
            vol_surface_param_updates.append(new_row_data)
        try:
            with shared_session() as session:
                session.execute(
                    sqlalchemy.update(upedynamic.VolSurface), vol_surface_param_updates
                )
                session.commit()
            conn.publish(
                "v2:compute" + dev_key_redis_append,
                orjson.dumps(
                    {
                        "type": "staticdata",
                        "product_symbols": [
                            vol_matrix_table[0]["option_symbol"].split(" ")[0]
                        ],
                    }
                ),
            )
        except Exception:
            traceback.print_exc()
            return True, False

        return False, True

    @app.callback(
        [Output("vol-matrix-param-graph-spinner", "children")],
        [
            Input("vol-matrix-refresh-graphs-button", "n_clicks"),
            Input("vol-matrix-dynamic-table", "selected_rows"),
            State("vol-matrix-dynamic-table", "data"),
            State("vol-matrix-product-option-symbol-map", "data"),
            State("vol-matrix-product-dropdown", "value"),
            State("vol-matrix-lme-settlement-spline-params", "data"),
            State("vol-matrix-lme-expiry-dates", "data"),
            State("vol-matrix-lme-inr-curve", "data"),
        ],
    )
    def generate_graphs(
        vol_mat_refresh_button_clicks: int,
        vol_matrix_selected_rows: List[int],
        vol_matrix_data: List[Dict[str, Any]],
        vol_matrix_stored_data: Dict[
            str, Tuple[Tuple[List[str], List[str], List[int]], List[str]]
        ],
        vol_matrix_selected_product: str,
        lme_settlement_spline_params: Dict[str, List[int]],
        lme_expiry_dates: Dict[str, datetime],
        inr_curve: Dict[int, float],
    ):
        if vol_matrix_data is None or not vol_matrix_selected_rows:
            return [html.Br()]

        (
            (option_symbols, vol_model_types, vol_surface_ids),
            param_column_keys,
        ) = vol_matrix_stored_data[vol_matrix_selected_product]

        redis_pipeline = conn.pipeline()
        for selected_row_index in vol_matrix_selected_rows:
            option_expiry_symbol = option_symbols[selected_row_index].lower()
            redis_pipeline.get(option_expiry_symbol + dev_key_redis_append)
            redis_pipeline.get(
                "v2:gli:" + option_expiry_symbol + ":fcp" + dev_key_redis_append
            )
            redis_pipeline.get(
                "v2:gli:" + option_expiry_symbol + ":osp" + dev_key_redis_append
            )

        option_engine_outputs = redis_pipeline.execute()
        base_data_indices = range(0, len(option_engine_outputs), 3)
        live_greek_data = option_engine_outputs[::3]

        if not live_greek_data:
            return [dbc.Alert("No live greek data found!", color="danger")]

        param_figures: Dict[str, go.Figure] = {
            "vol_delta_curve": go.Figure(
                data=[],
                layout=go.Layout(
                    title="Volatility BS Delta Curve",
                    xaxis={"title": "BS Delta"},
                    yaxis={"title": "Volatility"},
                ),
            ),
            "vol_strike_curve": go.Figure(
                data=[],
                layout=go.Layout(
                    title="Volatility Strike Curve",
                    xaxis={"title": "Strike"},
                    yaxis={"title": "Volatility"},
                ),
            ),
        } | {
            param_key: go.Figure(
                data=[],
                layout=go.Layout(
                    title=f"{param_key.upper()} Historical",
                    xaxis={"title": "Update Timestamp"},
                    yaxis={"title": param_key.upper()},
                ),
            )
            for param_key in param_column_keys
        }
        get_historical_vol_data_query = sqlalchemy.select(
            upedynamic.HistoricalVolSurface.update_datetime,
            upedynamic.HistoricalVolSurface.params,
        )
        # pull lme settle params here

        with shared_engine.connect() as connection:
            for selected_row_index, base_data_index in zip(
                vol_matrix_selected_rows, base_data_indices
            ):
                option_greeks = option_engine_outputs[base_data_index]

                if option_greeks is None:
                    continue
                historical_vol_data = pd.read_sql(
                    get_historical_vol_data_query.where(
                        upedynamic.HistoricalVolSurface.vol_surface_id
                        == vol_surface_ids[selected_row_index]
                    ),
                    connection,
                )
                historical_vol_data: pd.DataFrame = pd.concat(
                    [
                        historical_vol_data.drop("params", axis=1),
                        pd.json_normalize(historical_vol_data["params"]),
                    ],
                    axis=1,
                )
                historical_vol_data.columns = historical_vol_data.columns.str.lower()
                historical_vol_data.set_index("update_datetime")
                historical_vol_data = historical_vol_data.sort_index()

                option_greeks = pd.DataFrame(orjson.loads(option_greeks))

                option_greeks = option_greeks[option_greeks["option_types"] == 1]
                option_symbol = option_symbols[selected_row_index]

                future_settlement = option_engine_outputs[base_data_index + 1]
                options_settlement_vols = option_engine_outputs[base_data_index + 2]

                plot_lme_settlement = False
                if vol_matrix_selected_product[:4] == "xlme":
                    if lme_settlement_spline_params.get(option_symbol.lower()) is None:
                        print(
                            f"LME settle params not found for option {option_symbol.lower()}"
                        )
                    else:
                        plot_lme_settlement = True
                        # save und, t_to_expiry from op_eng output
                        und = option_greeks["underlying_prices"][0]
                        t_to_expiry = option_greeks["t_to_expiry"][0]
                        # pull relevant settle params from stored lme_settlement_spline_params
                        settle_params = lme_settlement_spline_params[
                            option_symbol.lower()
                        ]
                        # pull rate from curve
                        rate = inr_curve.get(
                            lme_expiry_dates[option_symbol].replace("-", "")
                        )
                        # then create both columns necessary to plot against the existing strikes column

                        atm_vol, p25_diff, m25_diff, p10_diff, m10_diff = settle_params
                        lme_spline_model = lme_linear_interpolation_model(
                            und,
                            t_to_expiry,
                            rate,
                            atm_vol,
                            p25_diff,
                            m25_diff,
                            p10_diff,
                            m10_diff,
                        )
                        lme_splined_settlement_vols = lme_spline_model(
                            option_greeks["strikes"]
                        )
                        lme_deltas = [0.1, 0.25, 0.5, 0.75, 0.9]
                        lme_vols = [
                            p10_diff + atm_vol,
                            p25_diff + atm_vol,
                            atm_vol,
                            m25_diff + atm_vol,
                            m10_diff + atm_vol,
                        ]

                plot_settlement = True
                if (
                    None
                    in (future_settlement, options_settlement_vols)
                    # and not plot_lme_settlement
                ):
                    plot_settlement = False

                else:
                    future_settlement = orjson.loads(future_settlement)
                    options_settlement_vols = orjson.loads(options_settlement_vols)
                    intraday_move = (
                        option_greeks["underlying_prices"][0] - future_settlement
                    )
                    option_greeks["settlement_vols"] = interpolate.UnivariateSpline(
                        np.array(options_settlement_vols["strike"] + intraday_move),
                        options_settlement_vols["volatility"],
                        k=2,
                        ext=3,
                        s=0,
                    )(option_greeks["strikes"])

                param_figures["vol_strike_curve"].add_scatter(
                    x=option_greeks["strikes"],
                    y=option_greeks["volatilities"],
                    name=option_symbol.upper().split(" ")[2],
                )
                param_figures["vol_delta_curve"].add_scatter(
                    x=option_greeks["deltas"],
                    y=option_greeks["volatilities"],
                    name=option_symbol.upper().split(" ")[2],
                )
                if plot_settlement:
                    param_figures["vol_strike_curve"].add_scatter(
                        x=option_greeks["strikes"],
                        y=option_greeks["settlement_vols"] / 100,
                        name=option_symbol.upper().split(" ")[2] + "\nSettle",
                    )
                    param_figures["vol_delta_curve"].add_scatter(
                        x=option_greeks["deltas"],
                        y=option_greeks["settlement_vols"] / 100,
                        name=option_symbol.upper().split(" ")[2] + "\nSettle",
                    )
                if plot_lme_settlement:
                    # copy of the above, but using lme data
                    param_figures["vol_strike_curve"].add_scatter(
                        x=option_greeks["strikes"],
                        y=lme_splined_settlement_vols,
                        name=option_symbol.upper().split(" ")[2] + "\nSettle",
                    )
                    lme_vols_splined = interpolate.UnivariateSpline(
                        lme_deltas, lme_vols, k=2, ext=3, s=0
                    )(option_greeks["deltas"])
                    param_figures["vol_delta_curve"].add_scatter(
                        x=option_greeks["deltas"],
                        y=lme_vols_splined,
                        name=option_symbol.upper().split(" ")[2] + "\nSettle",
                    )

                for param_key in param_column_keys:
                    param_figures[param_key].add_scatter(
                        x=historical_vol_data["update_datetime"].to_list(),
                        y=historical_vol_data[param_key.lower()].to_list(),
                        name=option_symbol.upper().split(" ")[2],
                    )

        figure_collection = [
            dbc.Row(
                children=[
                    dbc.Col(dcc.Graph(figure=param_figures["vol_strike_curve"])),
                    dbc.Col(dcc.Graph(figure=param_figures["vol_delta_curve"])),
                ]
            )
        ]
        num_params = len(param_column_keys)
        for i in range(num_params)[::2]:
            new_plot_row_children = []
            left_plot_param_key = param_column_keys[i]
            new_plot_row_children.append(
                dbc.Col(dcc.Graph(figure=param_figures[left_plot_param_key]))
            )
            if i + 1 < num_params:
                right_plot_param_key = param_column_keys[i + 1]
                new_plot_row_children.append(
                    dbc.Col(dcc.Graph(figure=param_figures[right_plot_param_key]))
                )
            figure_collection.append(dbc.Row(children=new_plot_row_children))

        return [html.Div(children=figure_collection)]

    @app.callback(
        [
            Output("vol-matrix-product-dropdown", "options"),
            Output("vol-matrix-product-option-symbol-map", "data"),
            Output("vol-matrix-product-dropdown", "disabled"),
            Output("vol-matrix-table-temp-placeholder-child", "children"),
            Output("vol-matrix-plots-hr", "hidden"),
            Output("vol-matrix-lme-settlement-spline-params", "data"),
            Output("vol-matrix-lme-expiry-dates", "data"),
            Output("vol-matrix-lme-inr-curve", "data"),
        ],
        [
            Input("fifteen-min-interval", "n_intervals"),
        ],
    )
    def get_products_with_options_dropdown_data(
        _,
    ) -> Tuple[
        Dict[str, str],
        Dict[str, Tuple[Tuple[List[str], List[str], List[int]], List[str]]],
        bool,
        List[Never],
        bool,
    ]:
        product_dropdown_choices = []
        product_options_map = {}
        now_dt_m12hr = datetime.now(tz=ZoneInfo("UTC")) - relativedelta.relativedelta(
            hours=12
        )
        lme_settlement_spline_params = {}
        lme_expiry_dates = {}
        with shared_session() as session:
            get_prods_w_ops = sqlalchemy.select(upestatic.Product)
            product_data = session.execute(get_prods_w_ops)
            get_lme_settlement_spline_params = sqlalchemy.text(
                """
                            SELECT option_symbol, atm_vol, m10_diff, m25_diff, p10_diff, p25_diff
                            FROM lme_settlement_spline_params
                            WHERE settlement_date = (
                                SELECT MAX(settlement_date)
                                FROM lme_settlement_spline_params
                            );
                            """
            )
            for row in session.execute(get_lme_settlement_spline_params):
                lme_settlement_spline_params[row.option_symbol] = [
                    row.atm_vol,
                    row.p25_diff,
                    row.m25_diff,
                    row.p10_diff,
                    row.m10_diff,
                ]
            for product in product_data.scalars().all():
                options = product.options
                options = sorted(options, key=lambda option_obj: option_obj.expiry)
                vol_model_param_keys = []
                if len(options) == 0:
                    continue
                # product_sym -> (option_symbol[], vol_model_type[], vol_surface_id[])
                option_data_arr = ([], [], [])
                option_vol_surface_models = set()

                for option in options:
                    option: upestatic.Option
                    if now_dt_m12hr >= option.expiry:
                        continue
                    option_data_arr[0].append(option.symbol)
                    option_data_arr[1].append(option.vol_surface.model_type)
                    option_data_arr[2].append(option.vol_surface_id)
                    option_vol_surface_models.add(option.vol_surface.model_type)
                    if option.symbol.lower()[:4] == "xlme":
                        lme_expiry_dates[option.symbol] = option.expiry.date()

                if len(option_vol_surface_models) != 1:
                    print(
                        f"Tried pregenerating vol matrix data for {option.symbol}, "
                        "failed due to multiple vol model types"
                    )
                    continue
                # Do this here so failed products don't appear in dropdown
                product_dropdown_choices.append(
                    {"label": product.long_name.upper(), "value": product.symbol}
                )
                vol_model_param_keys = list(options[0].vol_surface.params.keys())
                product_options_map[product.symbol] = (
                    option_data_arr,
                    vol_model_param_keys,
                )

        # pull inr curve used for lme products
        inr_curve = orjson.loads(
            conn.get(f"prep:cont_interest_rate:usd" + dev_key_redis_append)
        )

        # product_sym -> (option_symbol[], vol_model_type[], vol_surface_id[])
        return (
            product_dropdown_choices,
            product_options_map,
            False,
            [],
            False,
            lme_settlement_spline_params,
            lme_expiry_dates,
            inr_curve,
        )

    @app.callback(
        [
            Output("vol-matrix-dynamic-table", "columns"),
            Output("vol-matrix-dynamic-table", "data"),
            Output("vol-matrix-dynamic-table", "selected_rows"),
            Output("fit-params-pull-failure", "children"),
            Output("fit-params-pull-failure", "is_open"),
        ],
        [
            Input("vol-matrix-product-option-symbol-map", "data"),
            Input("vol-matrix-product-dropdown", "value"),
            Input("vol-matrix-fit-params-button", "n_clicks"),
            Input("vol-matrix-select-all-button", "n_clicks"),
            State("vol-matrix-dynamic-table", "selected_rows"),
            State("vol-matrix-dynamic-table", "data"),
            State("vol-matrix-dynamic-table", "columns"),
        ],
    )
    def update_vol_param_table(
        stored_product_options_map: Dict[
            str, Tuple[Tuple[List[str], List[str], List[int]], List[str]]
        ],
        selected_product_symbol: str,
        _vol_matrix_fit_params_nclicks: int,
        _vol_matrix_select_all_nclicks: int,
        selected_rows: List[int],
        vol_matrix_table_data: List[Dict[str, Any]],
        vol_matrix_column_data: List[int],
    ):
        if selected_rows is None:
            selected_rows = []
        if None in (
            selected_product_symbol,
            stored_product_options_map,
        ):
            return [], [], [], "", False
        # selects all if all not selected, deselects all if all are selected
        # ic(stored_product_options_map)
        if ctx.triggered_id == "vol-matrix-select-all-button":
            if set(selected_rows) == set(range(len(vol_matrix_table_data))):
                return (
                    vol_matrix_column_data,
                    vol_matrix_table_data,
                    [],
                    "",
                    False,
                )

            return (
                vol_matrix_column_data,
                vol_matrix_table_data,
                list(range(len(vol_matrix_table_data))),
                "",
                False,
            )
        # pulls settlement params for selected rows
        if ctx.triggered_id == "vol-matrix-fit-params-button":
            # all exchanges lead into this function, only lme has working logic in it atm
            vol_matrix_table_data, options_missing_params = (
                fit_vals_to_settlement_spline(vol_matrix_table_data, selected_rows)
            )
            if options_missing_params:
                return (
                    vol_matrix_column_data,
                    vol_matrix_table_data,
                    selected_rows,
                    f"Settlement params not found for: {len(options_missing_params)} option(s): {', '.join(options_missing_params)}",
                    True,
                )
            else:
                return (
                    vol_matrix_column_data,
                    vol_matrix_table_data,
                    selected_rows,
                    "",
                    False,
                )

        (
            (option_symbols, vol_model_types, vol_surface_ids),
            param_column_keys,
        ) = stored_product_options_map[selected_product_symbol]
        new_param_column_data = [
            {
                "id": "option_display_name",
                "name": "Option Display Name",
                "editable": False,
                "selectable": False,
            },
            {
                "id": "model_type",
                "name": "Vol Model",
                "editable": False,
                "selectable": False,
            },
            {
                "id": "vol_surface_id",
                "name": "vol_surface_id",
                "editable": False,
                "selectable": False,
            },
        ] + [
            {
                "id": column_key,
                "name": column_key.upper(),
                "editable": True,
                "selectable": True,
            }
            for column_key in param_column_keys
        ]
        new_vol_matrix_data = []
        with shared_session() as session:
            vol_surfaces = (
                session.execute(
                    sqlalchemy.select(upedynamic.VolSurface)
                    .where(upedynamic.VolSurface.vol_surface_id.in_(vol_surface_ids))
                    .order_by(sqlalchemy.asc(upedynamic.VolSurface.expiry))
                )
                .scalars()
                .all()
            )
            for option_symbol, vol_surface in zip(option_symbols, vol_surfaces):
                new_row_data = {
                    "option_symbol": option_symbol.upper(),  # add display_name handling here
                    "option_display_name": display_names.map_sd_exp_symbols_to_display_names(
                        option_symbol
                    ).upper(),
                    "model_type": vol_surface.model_type,
                    "vol_surface_id": vol_surface.vol_surface_id,
                }
                new_row_data.update(
                    {
                        param_key: param_value
                        for param_key, param_value in vol_surface.params.items()
                    }
                )
                new_vol_matrix_data.append(new_row_data)

        num_tab_rows = len(new_vol_matrix_data)
        if selected_rows:
            for i, row_index in list(enumerate(selected_rows))[::-1]:
                if row_index >= num_tab_rows:
                    del selected_rows[i]

        return new_param_column_data, new_vol_matrix_data, selected_rows, "", False


layout = html.Div(
    [
        topMenu("VolMatrixNew"),
        html.Div(
            [
                dbc.Row(
                    [
                        dbc.Col(
                            dcc.Dropdown(
                                id="vol-matrix-product-dropdown",
                                options=[],
                                value="xlme-lad-usd",
                                style={"width": "24em"},
                                disabled=True,
                            ),
                            width="auto",
                            className="my-3",
                        ),
                        dbc.Col(
                            html.Div(
                                [
                                    dbc.Button(
                                        "Select All",
                                        id="vol-matrix-select-all-button",
                                        className="mx-4",
                                    ),
                                    dbc.Button(
                                        "Fit Params", id="vol-matrix-fit-params-button"
                                    ),
                                    dbc.Button(
                                        "Refresh Graphs",
                                        id="vol-matrix-refresh-graphs-button",
                                        className="mx-4",
                                    ),
                                    dbc.Button(
                                        "Submit Params",
                                        id="vol-matrix-submit-params-button",
                                        color="warning",
                                    ),
                                ],
                            ),
                            width="auto",
                            className="my-3",
                        ),
                        dbc.Col(
                            html.Div(
                                [
                                    dbc.Alert(
                                        "Failure sending vol params",
                                        duration=5000,
                                        color="danger",
                                        id="new-vol-send-failure",
                                        is_open=False,
                                    ),
                                    dbc.Alert(
                                        "Success sending vol params",
                                        duration=5000,
                                        color="success",
                                        id="new-vol-send-success",
                                        is_open=False,
                                    ),
                                    dbc.Alert(
                                        # text through callback, shows which options missing settle params
                                        duration=10000,
                                        color="danger",
                                        id="fit-params-pull-failure",
                                        is_open=False,
                                    ),
                                ]
                            ),
                            width="auto",
                        ),
                    ],
                ),
                dbc.Row(
                    [
                        dbc.Spinner(
                            children=html.Br(
                                id="vol-matrix-table-temp-placeholder-child",
                            ),
                            id="vol-matrix-table-temp-placeholder",
                        )
                    ],
                ),
                dbc.Row(
                    [
                        dash_table.DataTable(
                            id="vol-matrix-dynamic-table",
                            data=[],
                            style_data_conditional=[
                                {
                                    "if": {"column_id": "vol_surface_id"},
                                    "display": "None",
                                },
                                {
                                    "if": {"row_index": "odd"},
                                    "backgroundColor": "rgb(248, 248, 248)",
                                },
                                {
                                    "if": {"column_id": "option_symbol"},
                                    "backgroundColor": "#f1f1f1",
                                },
                                {
                                    "if": {"column_id": "model_type"},
                                    "backgroundColor": "#f1f1f1",
                                },
                                {
                                    "if": {"column_id": "vol_surface_id"},
                                    "backgroundColor": "#f1f1f1",
                                },
                            ],
                            style_header_conditional=[
                                {
                                    "if": {"column_id": "vol_surface_id"},
                                    "display": "None",
                                },
                            ],
                            row_selectable="multi",
                        )
                    ]
                ),
                dbc.Row(
                    html.Div(
                        html.Hr(
                            id="vol-matrix-plots-hr",
                            hidden=True,
                            style={"borderColor": "#AAAAAA", "size": "2px"},
                        )
                    )
                ),
                dbc.Row(
                    dbc.Spinner(
                        id="vol-matrix-param-graph-spinner",
                        children=[html.Br()],
                        delay_show=300,
                    )
                ),
            ],
            className="mx-3 my-3",
        ),
        dcc.Interval(id="fifteen-min-interval", interval=1000 * 60 * 15),
        dcc.Store(id="vol-matrix-product-option-symbol-map", data=[]),
        dcc.Store(id="vol-matrix-lme-settlement-spline-params", data=[]),
        dcc.Store(id="vol-matrix-lme-expiry-dates", data=[]),
        dcc.Store(id="vol-matrix-lme-inr-curve", data=[]),
    ],
)
#
