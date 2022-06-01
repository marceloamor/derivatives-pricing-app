from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from dash import no_update
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_table as dtable
import pandas as pd
import json
from flask import request
import numpy as np

from sql import histroicParams
from parts import topMenu, loadRedisData, buildParamMatrix, sumbitVolas, onLoadPortFolio

# Inteval time for trades table refresh
interval = 1000 * 1
# column options for trade table
columns = [
    {"name": "product", "id": "product", "editable": False},
    {"name": "vol", "id": "vol", "editable": True},
    {"name": "skew", "id": "skew", "editable": True},
    {"name": "call", "id": "call", "editable": True},
    {"name": "put", "id": "put", "editable": True},
    {"name": "cmax", "id": "cmax", "editable": True},
    {"name": "pmax", "id": "pmax", "editable": True},
    {"name": "ref", "id": "ref", "editable": True},
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

    # mult them all by 100 for display
    dff.loc[:, "vol"] *= 100
    dff.loc[:, "skew"] *= 100
    dff.loc[:, "call"] *= 100
    dff.loc[:, "put"] *= 100
    dff.loc[:, "cmax"] *= 100
    dff.loc[:, "pmax"] *= 100

    cols = ["vol", "skew", "call", "put", "cmax", "pmax"]

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
    
    #current georgia vols
    params = np.array(results[param])
    data.append({"x": strikes, "y": params, "type": "line", "name": "Vola"})
    
    #settlement vols
    settleVolas = np.array(results["settle_vola"])
    data.append({"x": strikes, "y": settleVolas, "type": "line", "name": "Settlement Vols"})    
    
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
        dcc.Loading(
            type="circle",
            children=[html.Div([dcc.Graph(id="Vol_surface")])],
            className="rows",
        ),
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
            [dcc.Dropdown(id="volProduct", value=onLoadPortFolio()[0]['value'], options=onLoadPortFolio())],
            width=3,
        )
    ]
)

layout = html.Div(
    [
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
        [Output("volsTable", "data"), Output("sol_vols", "data")],
        [Input("volProduct", "value")],
    )
    def update_trades(portfolio):
        if portfolio:
            dict, sol_vol = pulVols(portfolio)
            return dict, sol_vol
        else:
            no_update, no_update

    # loop over table and send all vols to redis
    @app.callback(
        Output("volProduct", "value"),
        [Input("submitVol", "n_clicks")],
        [State("volsTable", "data"), State("volProduct", "value") ],
    )
    def update_trades(clicks, data, portfolio):
        if clicks != None:
            data_previous = pulVols(portfolio)

            for row, prev_row in zip(data, data_previous):
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
                        "ref": float(row["ref"]),
                    }
                    user = request.headers.get("X-MS-CLIENT-PRINCIPAL-NAME")
                    print(cleaned_df)
                    # submit vol to redis and DB
                    sumbitVolas(product.lower(), cleaned_df, user)

            return portfolio
        else:
            return no_update

    # Load greeks for active cell
    @app.callback(
        Output("Vol_surface", "figure"),
        [Input("volsTable", "active_cell")],
        [State("volsTable", "data"), State("sol_vols", "data")],
    )
    def updateData(cell, data, sol_vols):
        if data and cell:
            product = data[cell["row"]]["product"]
            if product:
                # load current greek data
                data = loadRedisData(product.lower())

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
