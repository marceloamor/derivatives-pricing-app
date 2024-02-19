from datetime import datetime
from typing import Dict, List, Tuple

import dash_bootstrap_components as dbc
import sqlalchemy
import upedata.dynamic_data as upedynamic
import upedata.static_data as upestatic
from dash import dash_table, dcc, html
from dash.dependencies import Input, Output
from dateutil import relativedelta
from parts import shared_session, topMenu
from zoneinfo import ZoneInfo


def initialise_callbacks(app):
    @app.callback(
        [
            Output("vol-matrix-product-dropdown", "options"),
            Output("vol-matrix-product-option-symbol-map", "data"),
        ],
        Input("fifteen-min-interval", "n_intervals"),
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
                vol_model_param_keys = []
                if len(options) == 0:
                    continue
                # product_sym -> (option_symbol[], vol_model_type[], vol_surface_id[])
                option_data_arr = ([], [], [])
                option_vol_surface_models = set()
                product_dropdown_choices.append(
                    {"label": product.long_name.upper(), "value": product.symbol}
                )
                vol_model_param_keys = list(options[0].vol_surface.params.keys())
                for option in options:
                    option: upestatic.Option
                    if now_dt_p12hr <= option.expiry:
                        continue
                    option_data_arr[0].append(option.symbol)
                    option_data_arr[1].append(option.vol_surface.model_type)
                    option_data_arr[2].append(option.vol_surface_id)
                    # option_data_arr.append(
                    #     (
                    #         option.symbol,
                    #         option.vol_surface.model_type,
                    #         option.vol_surface_id,
                    #     )
                    # )
                    option_vol_surface_models.add(option.vol_surface.model_type)
                if len(option_vol_surface_models) != 1:
                    print(
                        f"Tried pregenerating vol matrix data for {option.symbol}, "
                        "failed due to multiple vol model types"
                    )
                    continue
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
        ],
        [
            Input("vol-matrix-product-dropdown", "value"),
            Input("vol-matrix-product-option-symbol-map", "data"),
        ],
    )
    def update_vol_param_table(
        selected_product_symbol: str,
        stored_product_options_map: Dict[
            str, Tuple[Tuple[List[str], List[str], List[int]], List[str]]
        ],
    ):
        if None in (selected_product_symbol, stored_product_options_map):
            return [], []
        (
            (option_symbols, vol_model_types, vol_surface_ids),
            param_column_keys,
        ) = stored_product_options_map[selected_product_symbol]
        with shared_session() as session:
            vol_surfaces = (
                session.execute(
                    sqlalchemy.select(upedynamic.VolSurface).where(
                        upedynamic.VolSurface.vol_surface_id.in_(vol_surface_ids)
                    )
                )
                .scalars()
                .all()
            )
            for vol_surface in vol_surfaces:
                pass

        return [], []


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
                                        id="vol-matrix-fit-params-button",
                                        className="mx-4",
                                    ),
                                ],
                            ),
                            width=3,
                        ),
                    ]
                ),
                dbc.Row(
                    [
                        dash_table.DataTable(
                            id="vol-matrix-dynamic-table",
                            data=[],
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
