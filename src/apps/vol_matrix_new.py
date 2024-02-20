from datetime import datetime
from typing import Any, Dict, List, Tuple

import dash_bootstrap_components as dbc
import sqlalchemy
import upedata.dynamic_data as upedynamic
import upedata.static_data as upestatic
from dash import ctx, dash_table, dcc, html
from dash.dependencies import Input, Output, State
from dateutil import relativedelta
from parts import shared_session, topMenu
from zoneinfo import ZoneInfo


def fit_vals_to_settlement_spline(
    vol_matrix_table_data: List[Dict[str, Any]], selected_rows: List[int]
) -> List[Dict[str, Any]]:
    # this function will need to pull the settlement curve for the specific option
    # and then run a minimiser to find the optimal parameters for the specific vol
    # model/exchange to meet that settlement curve, this may well end up being
    # inaccurate, for LME it will just pull the settlement spline params as they
    # map 1:1 to the model we use
    return vol_matrix_table_data


def initialise_callbacks(app):
    @app.callback(
        [
            Output("vol-matrix-product-dropdown", "options"),
            Output("vol-matrix-product-option-symbol-map", "data"),
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
    ]:
        product_dropdown_choices = []
        product_options_map = {}
        now_dt_p12hr = datetime.now(tz=ZoneInfo("UTC")) + relativedelta.relativedelta(
            hours=12
        )
        with shared_session() as session:
            get_prods_w_ops = sqlalchemy.select(upestatic.Product)
            product_data = session.execute(get_prods_w_ops)
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
                    if now_dt_p12hr >= option.expiry:
                        continue
                    option_data_arr[0].append(option.symbol)
                    option_data_arr[1].append(option.vol_surface.model_type)
                    option_data_arr[2].append(option.vol_surface_id)
                    option_vol_surface_models.add(option.vol_surface.model_type)
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

        # product_sym -> (option_symbol[], vol_model_type[], vol_surface_id[])
        return product_dropdown_choices, product_options_map

    @app.callback(
        [
            Output("vol-matrix-dynamic-table", "columns"),
            Output("vol-matrix-dynamic-table", "data"),
            Output("vol-matrix-dynamic-table", "selected_rows"),
        ],
        [
            Input("vol-matrix-product-option-symbol-map", "data"),
            Input("vol-matrix-product-dropdown", "value"),
            Input("vol-matrix-fit-params-button", "n_clicks"),
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
        selected_rows: List[int],
        vol_matrix_table_data: List[Dict[str, Any]],
        vol_matrix_column_data: List[int],
    ):
        if None in (
            selected_product_symbol,
            stored_product_options_map,
        ):
            return [], [], []
        if ctx.triggered_id == "vol-matrix-fit-params-button":
            return (
                vol_matrix_column_data,
                fit_vals_to_settlement_spline(vol_matrix_table_data, selected_rows),
                selected_rows,
            )
        (
            (option_symbols, vol_model_types, vol_surface_ids),
            param_column_keys,
        ) = stored_product_options_map[selected_product_symbol]
        new_param_column_data = [
            {
                "id": "option_symbol",
                "name": "Option Symbol",
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

        return new_param_column_data, new_vol_matrix_data, []


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
                                options=[
                                    {"label": "LME ALUMINIUM", "value": "xlme-lad-usd"}
                                ],
                                value="xlme-lad-usd",
                            ),
                            width=3,
                        ),
                        dbc.Col(
                            html.Div(
                                [
                                    dbc.Button(
                                        "Fit Params", id="vol-matrix-fit-params-button"
                                    ),
                                    dbc.Button(
                                        "Submit Params",
                                        id="vol-matrix-submit-params-button",
                                        className="mx-4",
                                    ),
                                ],
                            ),
                            width=3,
                        ),
                    ],
                    className="mb-2",
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
            ],
            className="mx-3 my-3",
        ),
        dcc.Interval(id="fifteen-min-interval", interval=1000 * 60 * 15),
        dcc.Store(id="vol-matrix-product-option-symbol-map", data=[]),
    ],
)
