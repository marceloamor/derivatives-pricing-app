from parts import topMenu, onLoadPortFolio, ringTime
from data_connections import conn

from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
from dash import dash_table as dtable
from dash import dcc, html
import pandas as pd
import numpy as np

import os


positionLocationLME = os.getenv("POS_LOCAITON_LME", default="greekpositions")
positionLocationEUR = os.getenv("POS_LOCAITON_EUR", default="greekpositions_xext:dev")


# 1 sec interval
interval = str(1000 * 1)

columns = [
    {"name": "Product", "id": "contract_symbol"},
    {"name": "Delta", "id": "total_delta"},
    {"name": "Full Delta", "id": "total_fullDelta"},
    {"name": "Vega", "id": "total_vega"},
    {"name": "Theta", "id": "total_theta"},
    {"name": "Gamma", "id": "total_gamma"},
    {"name": "Full Gamma", "id": "total_fullGamma"},
    {"name": "Delta Decay", "id": "total_deltaDecay"},
    {"name": "Vega Decay", "id": "total_vegaDecay"},
    {"name": "Gamma Decay", "id": "total_gammaDecay"},
    {"name": "Gamma Breakeven", "id": "total_gammaBreakEven"},
]

# product dropdown and label
options = onLoadPortFolio()
options.append({"label": "Milling Wheat", "value": "xext-ebm-eur"})
productDropdown = dcc.Dropdown(
    id="portfolio-selector",
    value="copper",
    options=options,
)
productLabel = html.Label(
    ["Product:"], style={"font-weight": "bold", "text-align": "left"}
)

selectors = dbc.Row(
    [
        dbc.Col(
            [productLabel, productDropdown],
            width=4,
        ),
    ]
)


table = dbc.Row(
    [
        dbc.Col(
            [
                dtable.DataTable(
                    id="portfolios",
                    columns=columns,
                    data=[{}],
                    style_data_conditional=[
                        {
                            "if": {"row_index": "odd"},
                            "backgroundColor": "rgb(248, 248, 248)",
                        }
                    ],
                )
            ]
        )
    ]
)

layout = html.Div(
    [
        topMenu("Portfolio Risk"),
        dcc.Interval(
            id="live-update", interval=1 * 1000, n_intervals=0  # in milliseconds
        ),
        selectors,
        table,
    ]
)


def initialise_callbacks(app):
    # pull greeks
    @app.callback(
        Output("portfolios", "data"),
        [Input("live-update", "n_intervals"), Input("portfolio-selector", "value")],
    )
    def update_greeks(interval, portfolio):
        if portfolio == "xext-ebm-eur":
            # pull list of porducts from static data
            data = conn.get(positionLocationEUR)
            if data != None:
                dff = pd.read_json(data)
                # aggregate by product name
                dff = dff.groupby("contract_symbol", as_index=False).sum(
                    numeric_only=True
                )
                dff["multiplier"] = 50
                dff["total_gammaBreakEven"] = 0.0
                valid_befg_df = dff.loc[
                    (dff["total_fullGamma"] * dff["total_theta"] < 0.0)
                    & (dff["total_fullGamma"].abs() > 1e-6),
                    :,
                ]

                dff.loc[
                    (dff["total_fullGamma"] * dff["total_theta"] < 0.0)
                    & (dff["total_fullGamma"].abs() > 1e-6),
                    "total_gammaBreakEven",
                ] = np.sqrt(
                    -2
                    * valid_befg_df["total_theta"]
                    / (valid_befg_df["multiplier"] * valid_befg_df["total_fullGamma"])
                )

                # sort on expiry
                dff.sort_values("T_cal_to_underlying_expiry", inplace=True)
                dff.sum(numeric_only=True, axis=0)

                # sort by date
                dff["expiry"] = dff["contract_symbol"].str.split(" ").str[2]
                dff.sort_values("expiry", inplace=True, ascending=True)

                # calc total row and re label
                dff.loc["Total"] = dff.sum(numeric_only=True, axis=0)
                dff.loc["Total", "contract_symbol"] = "Total"

                return dff.round(3).to_dict("records")
        else:
            dff = conn.get(positionLocationLME)
            dff = pd.read_json(dff)

            if not dff.empty:
                # sort on expiry
                dff.sort_values("expiry", inplace=True)
                # group on product
                dff = (
                    dff[dff["portfolio"] == portfolio]
                    .groupby("product")
                    .sum(numeric_only=True)
                    .round(3)
                    .reset_index()
                )
                dff["total_gammaBreakEven"] = 0.0

                # sort based on product name
                dff[["first_value", "last_value"]] = dff["product"].str.extract(
                    r"([ab])?(\d)"
                )
                dff = dff.sort_values(by=["first_value", "last_value"])
                dff.drop(columns=["last_value", "first_value"], inplace=True)

                # calc total row and re label
                dff.loc["Total"] = dff.sum(numeric_only=True, axis=0)
                dff.loc["Total", "product"] = "Total"

                # rename product to match euronext
                dff.rename(columns={"product": "contract_symbol"}, inplace=True)

                return dff.round(3).to_dict("records")

    @app.callback(Output("ring", "children"), [Input("live-update", "n_intervals")])
    def updareRing(interval):
        return ringTime()
