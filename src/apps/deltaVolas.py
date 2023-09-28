from parts import topMenu, loadStaticData, retriveParams, volCalc, onLoadPortfolio
from data_connections import conn
from datetime import date

from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
from dash import dash_table as dtable
from scipy.stats import norm
from dash import dcc, html
import datetime as dt
import pandas as pd

import math


def BSStrikeFromDelta(S0, T, r, sigma, delta, right):
    right = right.lower()
    if right == "c":
        strike = S0 * math.exp(
            -norm.ppf(delta * math.exp(r * T)) * sigma * math.sqrt(T)
            + ((sigma**2) / 2) * T
        )
        return strike
    elif right == "p":
        strike = S0 * math.exp(
            norm.ppf(delta * math.exp(r * T)) * sigma * math.sqrt(T)
            + ((sigma**2) / 2) * T
        )
        return strike


columns = [
    {"name": ["", "Product Code"], "id": "Product Code"},
    {"name": ["-10Delta", "Strike"], "id": "-10Delta Strike"},
    {"name": ["-10Delta", "Vola"], "id": "-10Delta"},
    {"name": ["-25Delta", "Strike"], "id": "-25Delta Strike"},
    {"name": ["-25Delta", "Vola"], "id": "-25Delta"},
    {"name": ["50Delta", "Strike"], "id": "50Delta Strike"},
    {"name": ["50Delta", "Vola"], "id": "50Delta"},
    {"name": ["+25Delta", "Strike"], "id": "+25Delta Strike"},
    {"name": ["+25Delta", "Vola"], "id": "+25Delta"},
    {"name": ["+10Delta", "Strike"], "id": "+10Delta Strike"},
    {"name": ["+10Delta", "Vola"], "id": "+10Delta"},
]

table = dbc.Row(
    [
        dbc.Col(
            [
                dtable.DataTable(
                    id="volasTable",
                    columns=columns,
                    data=[{}],
                    merge_duplicate_headers=True,
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

options = dbc.Row(
    [
        dbc.Col(
            [
                dcc.Dropdown(
                    id="portfolio-selector-volas",
                    value="copper",
                    options=onLoadPortfolio(),
                )
            ],
            width=2,
        ),
        dbc.Col(
            [
                dcc.Dropdown(
                    id="diff",
                    value="diff",
                    options=[
                        {"label": "Diff", "value": "diff"},
                        {"label": "Volas", "value": "volas"},
                    ],
                )
            ],
            width=2,
        ),
    ]
)

layout = html.Div([topMenu("Vola by delta"), options, table])


def initialise_callbacks(app):
    @app.callback(
        Output("volasTable", "data"),
        [Input("portfolio-selector-volas", "value"), Input("diff", "value")],
    )
    def productVolas(portfolio, diff):
        # pull staticdata for
        staticData = loadStaticData()

        # filter for current portfolio
        staticData = staticData.loc[staticData["portfolio"] == portfolio.lower()]

        # list products
        products = staticData["product"].values

        # assign multiplier level depending on diff
        if diff == "diff":
            multiplier = 1
        else:
            multiplier = 0

        portfolioVolas = []
        # go collect params and turn into delta space volas
        for product in products:
            params = retriveParams(product.lower())

            # current undlying
            greeks = pd.read_json(conn.get(product.lower()), orient="index")

            und = greeks.iloc[0]["und_calc_price"]

            # find expiry to find days to expiry
            now = dt.datetime.now()
            expiry = int(greeks.iloc[0]["expiry"])
            expiry = date.fromtimestamp(expiry / 1e3)

            if isinstance(now, dt.date):
                (day, month, year) = now.day, now.month, now.year
                d0 = date(int(year), int(month), int(day))
            else:
                d0 = now
            d1 = expiry
            t = (d1 - d0).days / 365

            # calc the atm vol for relative using multiplier to turn on/off
            atm = params["vol"] * 100 * multiplier

            # find interest rate
            rate = greeks.iloc[0]["interest_rate"]

            vol90 = volCalc(
                1.28155,
                params["vol"],
                params["skew"],
                params["call"],
                params["put"],
                params["cmax"],
                params["pmax"],
            )
            vol75 = volCalc(
                0.67449,
                params["vol"],
                params["skew"],
                params["call"],
                params["put"],
                params["cmax"],
                params["pmax"],
            )
            vol25 = volCalc(
                -0.67449,
                params["vol"],
                params["skew"],
                params["call"],
                params["put"],
                params["cmax"],
                params["pmax"],
            )
            vol10 = volCalc(
                -1.28155,
                params["vol"],
                params["skew"],
                params["call"],
                params["put"],
                params["cmax"],
                params["pmax"],
            )

            # build product voals per strike
            volas = {
                "Product Code": product,
                "-10Delta Strike": round(
                    BSStrikeFromDelta(und, t, rate / 100, vol90 / 100, 0.9, "c"), 0
                ),
                "-10Delta": round(vol90 - atm, 2),
                "-25Delta Strike": round(
                    BSStrikeFromDelta(und, t, rate / 100, vol75 / 100, 0.75, "c"), 0
                ),
                "-25Delta": round(vol75 - atm, 2),
                "50Delta": round(params["vol"] * 100, 2),
                "50Delta Strike": round(und, 0),
                "+25Delta Strike": round(
                    BSStrikeFromDelta(und, t, rate / 100, vol25 / 100, 0.25, "c"), 0
                ),
                "+25Delta": round(vol25 - atm, 2),
                "+10Delta Strike": round(
                    BSStrikeFromDelta(und, t, rate / 100, vol10 / 100, 0.1, "c"), 0
                ),
                "+10Delta": round(vol10 - atm, 2),
            }
            # append to bucket
            portfolioVolas.append(volas)

        return portfolioVolas
