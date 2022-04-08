from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_core_components as dcc
from datetime import datetime as dt
import dash_bootstrap_components as dbc
import dash_table as dtable
import pandas as pd

from parts import topMenu, onLoadPortFolio, ringTime
from data_connections import conn

# 1 sec interval
interval = str(1000 * 1)

columns = [
    {"name": "Product", "id": "product"},
    {"name": "Delta", "id": "total_delta"},
    {"name": "Full Delta", "id": "total_fullDelta"},
    {"name": "Vega", "id": "total_vega"},
    {"name": "Theta", "id": "total_theta"},
    {"name": "Gamma", "id": "total_gamma"},
    {"name": "Delta Decay", "id": "total_deltaDecay"},
    {"name": "Vega Decay", "id": "total_vegaDecay"},
    {"name": "Gamma Decay", "id": "total_gammaDecay"},
]

options = dbc.Row(
    [
        dbc.Col(
            [
                dcc.Dropdown(
                    id="portfolio-selector", value="copper", options=onLoadPortFolio()
                )
            ]
        )
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
        options,
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
        dff = conn.get("greekpositions")
        dff = pd.read_json(dff)

        if not dff.empty:
            # sort on expiry
            dff.sort_values("expiry", inplace=True)
            # group on product
            dff = (
                dff[dff["portfolio"] == portfolio]
                .groupby("product")
                .sum()
                .round(3)
                .reset_index()
            )
            # calc total row and re label
            dff.loc["Total"] = dff.sum(numeric_only=True, axis=0)
            dff.loc["Total", "product"] = "Total"

            return dff.round(3).to_dict("records")

    @app.callback(Output("ring", "children"), [Input("live-update", "n_intervals")])
    def updareRing(interval):
        return ringTime()
