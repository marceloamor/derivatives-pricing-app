from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from dash import no_update, dcc
from dash import dcc, html, ctx

# from dash import dcc
import dash_bootstrap_components as dbc
from dash import dash_table as dtable
import pandas as pd
import json
from flask import request
import numpy as np
import os

from sql import histroicParams
from parts import (
    topMenu,
    loadRedisData,
    buildParamMatrix,
    sumbitVolas,
    onLoadPortFolio,
    lme_option_to_georgia,
)
from data_connections import Connection

# Inteval time for trades table refresh
interval = 1000 * 2
# column options for trade table
columns = [
    {"name": "product", "id": "product", "editable": False},
    {"name": "skew", "id": "skew", "editable": True},
    {"name": "call", "id": "call", "editable": True},
    {"name": "put", "id": "put", "editable": True},
    {"name": "cmax", "id": "cmax", "editable": True},
    {"name": "pmax", "id": "pmax", "editable": True},
    {"name": "-10 Delta", "id": "90 delta", "editable": True},
    {"name": "-25 Delta", "id": "75 delta", "editable": True},
    {"name": "vol", "id": "vol", "editable": True},
    {"name": "+25 Delta", "id": "25 delta", "editable": True},
    {"name": "+10 Delta", "id": "10 delta", "editable": True},
    {"name": "ref", "id": "ref", "editable": True},
]

USE_DEV_KEYS = os.getenv("USE_DEV_KEYS", "false").lower() in [
    "true",
    "t",
    "1",
    "y",
    "yes",
]


def pulVols(portfolio):
    # pull matrix inputs
    dff, sol_curve = buildParamMatrix(portfolio.capitalize())
    # create product column
    dff["product"] = dff.index
    dff["prompt"] = pd.to_datetime(dff["prompt"], format="%d/%m/%Y")
    dff = dff.sort_values(["prompt"], na_position="first")

    # convert call/put max into difference
    dff["cmax"] = dff["cmax"] - dff["vol"]
    dff["pmax"] = dff["pmax"] - dff["vol"]
    dff["10 delta"] = dff["10 delta"] - dff["vol"]
    dff["25 delta"] = dff["25 delta"] - dff["vol"]
    dff["75 delta"] = dff["75 delta"] - dff["vol"]
    dff["90 delta"] = dff["90 delta"] - dff["vol"]

    # mult them all by 100 for display
    dff.loc[:, "vol"] *= 100
    dff.loc[:, "skew"] *= 100
    dff.loc[:, "call"] *= 100
    dff.loc[:, "put"] *= 100
    dff.loc[:, "cmax"] *= 100
    dff.loc[:, "pmax"] *= 100
    dff.loc[:, "10 delta"] *= 100
    dff.loc[:, "25 delta"] *= 100
    dff.loc[:, "75 delta"] *= 100
    dff.loc[:, "90 delta"] *= 100

    cols = [
        "vol",
        "skew",
        "call",
        "put",
        "cmax",
        "pmax",
        "10 delta",
        "25 delta",
        "75 delta",
        "90 delta",
    ]

    dff[cols] = dff[cols].round(2)

    dict = dff.to_dict("records")

    return dict, sol_curve


def draw_param_graphTraces(results, sol_vols, param):

    # merge params and sol3
    if not sol_vols.empty:
        sol_vols.index = sol_vols.index.astype(int)
        results = results.merge(sol_vols, left_on="strike", right_index=True)

    # sort data on date and adjust current dataframe
    results.sort_values(by=["strike"], inplace=True)

    data = []
    # extract graph inputs from results and sol_vols
    strikes = results["strike"]

    # current georgia vols
    params = np.array(results[param])
    data.append({"x": strikes, "y": params, "type": "line", "name": "Vola"})

    # settlement vols
    settleVolas = np.array(results["settle_vola"])
    data.append(
        {"x": strikes, "y": settleVolas, "type": "line", "name": "Settlement Vols"}
    )

    if not sol_vols.empty:
        sol_vol = np.array(results["v"])
        data.append({"x": strikes, "y": sol_vol, "type": "line", "name": "Sol Vols"})

    # data = [
    #     {"x": strikes, "y": params, "type": "line", "name": "Vola"},
    #     {"x": strikes, "y": settleVolas, "type": "line", "name": "Settlement Vols"},
    #     {"x": strikes, "y": sol_vol, "type": "line", "name": "Sol Vols"},
    # ]

    return {"data": data}


def shortName(product):
    if product == None:
        return "LCU"

    if product.lower() == "aluminium":
        return "LAD"
    elif product.lower() == "lead":
        return "PBD"
    elif product.lower() == "copper":
        return "LCU"
    elif product.lower() == "nickel":
        return "LND"
    elif product.lower() == "zinc":
        return "LZH"
    else:
        return []


graphs = html.Div(
    [
        # dcc.Loading(
        #     type="circle",
        #     children=[html.Div([dcc.Graph(id="Vol_surface")])],
        #     className="rows",
        # ),
        html.Div([dcc.Graph(id="Vol_surface")]),
        html.Div(
            [
                dcc.Loading(
                    type="circle",
                    children=[html.Div([dcc.Graph(id="volGraph")])],
                    className="six columns",
                ),
                dcc.Loading(
                    type="circle",
                    children=[html.Div([dcc.Graph(id="skewGraph")])],
                    className="six columns",
                ),
            ],
            className="row",
        ),
        html.Div(
            [
                dcc.Loading(
                    type="circle",
                    children=[html.Div([dcc.Graph(id="callGraph")])],
                    className="six columns",
                ),
                dcc.Loading(
                    type="circle",
                    children=[html.Div([dcc.Graph(id="putGraph")])],
                    className="six columns",
                ),
            ],
            className="row",
        ),
    ]
)

# data stores for vol and params data.
hidden = html.Div(
    [
        dcc.Store(id="volIntermediate-value"),
        dcc.Store(id="volGreeks"),
        dcc.Store(id="sol_vols"),
    ],
    className="row",
)

options = dbc.Row(
    [
        dbc.Col(
            [
                dcc.Dropdown(
                    id="volProduct",
                    value=onLoadPortFolio()[0]["value"],
                    options=onLoadPortFolio(),
                )
            ],
            width=3,
        ),
        dbc.Col(
            [
                html.Button("Fit Vals", id="fit-val", n_clicks=0),
            ],
            width=3,
        ),
    ]
)

layout = html.Div(
    [
        dcc.Interval(
            id="vol-update", interval=interval, n_intervals=0  # in milliseconds
        ),
        topMenu("Vola Matrix"),
        options,
        dtable.DataTable(
            id="volsTable",
            columns=columns,
            editable=True,
            data=[{}],
            style_data_conditional=[
                {"if": {"row_index": "odd"}, "backgroundColor": "rgb(248, 248, 248)"}
            ],
        ),
        html.Button("Submit Vols", id="submitVol"),
        hidden,
        graphs,
    ]
)


def initialise_callbacks(app):
    # pulltrades use hiddien inputs to trigger update on new trade
    @app.callback(
        Output("volsTable", "data"),
        [Input("volProduct", "value"), Input("fit-val", "n_clicks")],
        [State("volsTable", "data")],
    )
    def update_trades(portfolio, click, data):
        # figure out which button triggered the callback
        button_id = ctx.triggered_id if not None else "No clicks yet"

        if portfolio:
            if button_id == "fit-val":
                # retrive settlement volas
                settlement_vols = pd.read_sql(
                    "SELECT * from public.get_settlement_vols()",
                    Connection("Sucden-sql-soft", "LME"),
                )

                # create instruemnt from LME values
                settlement_vols["instrument"] = settlement_vols.apply(
                    lambda row: lme_option_to_georgia(row["Product"], row["Series"]),
                    axis=1,
                )
                settlement_vols["instrument"] = settlement_vols[
                    "instrument"
                ].str.upper()

                # convert data to dataframe
                data = pd.DataFrame.from_dict(data)

                # resent the indexes to product
                settlement_vols.set_index("instrument", inplace=True)
                data.set_index("product", inplace=True)

                # replace columns
                data["vol"] = settlement_vols["50 Delta"]
                data["cmax"] = settlement_vols["+10 DIFF"]
                data["pmax"] = settlement_vols["-10 DIFF"]
                data["10 delta"] = settlement_vols["+10 DIFF"]
                data["25 delta"] = settlement_vols["+25 DIFF"]
                data["75 delta"] = settlement_vols["-25 DIFF"]
                data["90 delta"] = settlement_vols["-10 DIFF"]

                # round dataframe and reset index
                data.round(2)
                data = data.reset_index(level=0)

                # convert to dict
                dict = data.to_dict("records")

                return dict

            else:
                dict, sol_vol = pulVols(portfolio)
                return dict
        else:
            no_update

    # load sol3 vols
    @app.callback(
        Output("sol_vols", "data"),
        [Input("volProduct", "value"), Input("vol-update", "n_intervals")],
    )
    def update_sol_vols(portfolio, interval):
        if portfolio:
            dict, sol_vol = pulVols(portfolio)
            return sol_vol
        else:
            no_update

    # loop over table and send all vols to redis
    @app.callback(
        Output("volProduct", "value"),
        [Input("submitVol", "n_clicks")],
        [State("volsTable", "data"), State("volProduct", "value")],
    )
    def update_trades(clicks, data, portfolio):
        if clicks != None:
            data_previous = pulVols(portfolio)

            for row, prev_row in zip(data, data_previous[0]):

                if row == prev_row:

                    continue
                else:
                    # collect data for vol submit
                    product = row["product"]
                    cleaned_df = {
                        "spread": float(row["spread"]),
                        "vola": float(row["vol"]) / 100,
                        "skew": float(row["skew"]) / 100,
                        "calls": float(row["call"]) / 100,
                        "puts": float(row["put"]) / 100,
                        "cmax": (float(row["cmax"]) + float(row["vol"])) / 100,
                        "pmax": (float(row["pmax"]) + float(row["vol"])) / 100,
                        "10 delta": (float(row["10 delta"]) + float(row["vol"])) / 100,
                        "25 delta": (float(row["25 delta"]) + float(row["vol"])) / 100,
                        "75 delta": (float(row["75 delta"]) + float(row["vol"])) / 100,
                        "90 delta": (float(row["90 delta"]) + float(row["vol"])) / 100,
                        "ref": float(row["ref"]),
                    }
                    user = request.headers.get("X-MS-CLIENT-PRINCIPAL-NAME")

                    # submit vol to redis and DB
                    sumbitVolas(
                        product.lower(), cleaned_df, user, dev_keys=USE_DEV_KEYS
                    )

            return portfolio
        else:
            return no_update

    # Load greeks for active cell
    @app.callback(
        Output("Vol_surface", "figure"),
        [Input("volsTable", "active_cell"), Input("vol-update", "n_intervals")],
        [State("volsTable", "data"), State("sol_vols", "data")],
    )
    def updateData(cell, interval, data, sol_vols):
        if data and cell:
            product = data[cell["row"]]["product"]
            if product:
                # load current greek data
                if not USE_DEV_KEYS:
                    data = loadRedisData(product.lower())
                else:
                    data = loadRedisData(product.lower() + ":dev")

                # load sol_vols
                if product in sol_vols:
                    sol_vols = pd.DataFrame.from_dict(sol_vols[product], orient="index")
                else:
                    sol_vols = pd.DataFrame()

                # if data then un pack into data frame and send to graph builder
                if data != None:
                    data = json.loads(data)
                    dff = pd.DataFrame.from_dict(data, orient="index")

                    if len(dff) > 0:
                        figure = draw_param_graphTraces(dff, sol_vols, "vol")
                        return figure

                else:
                    figure = {"data": (0, 0)}
                    return figure
        else:
            return no_update

    ##update graphs on data update
    @app.callback(
        [
            Output("volGraph", "figure"),
            Output("skewGraph", "figure"),
            Output("callGraph", "figure"),
            Output("putGraph", "figure"),
        ],
        [Input("volsTable", "active_cell")],
        [State("volsTable", "data")],
    )
    def load_param_graph(cell, data):
        if cell == None:
            return no_update, no_update, no_update, no_update
        else:
            if data[0] and cell:
                product = data[cell["row"]]["product"]
                if product:
                    df = histroicParams(product)
                    dates = df["saveddate"].values
                    volFig = {
                        "data": [
                            {
                                "x": dates,
                                "y": df["atm_vol"].values * 100,
                                "type": "line",
                                "name": "Vola",
                            }
                        ]
                    }
                    skewFig = {
                        "data": [
                            {
                                "x": dates,
                                "y": df["skew"].values * 100,
                                "type": "line",
                                "name": "Skew",
                            }
                        ]
                    }
                    callFig = {
                        "data": [
                            {
                                "x": dates,
                                "y": df["calls"].values * 100,
                                "type": "line",
                                "name": "Call",
                            }
                        ]
                    }
                    putFig = {
                        "data": [
                            {
                                "x": dates,
                                "y": df["puts"].values * 100,
                                "type": "line",
                                "name": "Put",
                            }
                        ]
                    }

                    return volFig, skewFig, callFig, putFig
            else:
                return no_update, no_update, no_update, no_update
