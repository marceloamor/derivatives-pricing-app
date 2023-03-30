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
from data_connections import Connection, georgiadatabase, Session

import upestatic

# Inteval time for trades table refresh
interval = 1000 * 2
# column options for trade table
LMEcolumns = [
    {"name": "product", "id": "product", "editable": False},
    {"name": "spread", "id": "spread", "editable": True},
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


def loadEURProducts():
    with Session() as session:
        products = session.query(upestatic.Product).all()
        return products


EURProductList = [
    {"label": product.long_name.title(), "value": product.symbol}
    for product in loadEURProducts()
]


def loadEUROptions(optionSymbol):
    with Session() as session:
        product = (
            session.query(upestatic.Product)
            .where(upestatic.Product.symbol == optionSymbol)
            .first()
        )
        optionsList = (option for option in product.options)
        return optionsList


def pulVols(portfolio):
    # pull matrix inputs
    dff, sol_curve = buildParamMatrix(portfolio.capitalize(), dev_keys=USE_DEV_KEYS)
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
        "spread",
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


# needs to to pull from staticdata to be dynamic
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

LMEoptions = dbc.Row(
    [
        dbc.Col(
            [
                dcc.Dropdown(
                    id="tab1-volProduct",
                    # needs to be changed so that it is dynamic per exchange/portfolio
                    value=onLoadPortFolio()[0]["value"],
                    options=onLoadPortFolio(),
                )
            ],
            width=3,
        ),
        dbc.Col(
            [
                html.Button("Fit Vals", id="tab1-fit-val", n_clicks=0),
            ],
            width=3,
        ),
    ]
)

EURoptions = dbc.Row(
    [
        dbc.Col(
            [
                dcc.Dropdown(
                    id="tab2-volProduct",
                    # needs to be changed so that it is dynamic per exchange/portfolio
                    value=EURProductList[0]["value"],
                    options=EURProductList,
                )
            ],
            width=3,
        ),
        dbc.Col(
            [
                html.Button("Fit Vals", id="tab2-fit-val", n_clicks=0),
            ],
            width=3,
        ),
    ]
)

# tab 1 layout
tab1_content = dbc.Card(
    dbc.CardBody(
        [
            LMEoptions,
            dtable.DataTable(
                id="tab1-volsTable",
                columns=LMEcolumns,
                editable=True,
                data=[{}],
                style_data_conditional=[
                    {
                        "if": {"row_index": "odd"},
                        "backgroundColor": "rgb(248, 248, 248)",
                    }
                ],
            ),
            html.Button("Submit Vols", id="tab1-submitVol"),
        ]
    ),
    className="mt-3",
)

tab2_content = dbc.Card(
    dbc.CardBody(
        [
            EURoptions,
            dtable.DataTable(
                id="tab2-volsTable",
                #something like options[0]["params"].keys()
                columns=LMEcolumns,
                editable=True,
                data=[{}],
                style_data_conditional=[
                    {
                        "if": {"row_index": "odd"},
                        "backgroundColor": "rgb(248, 248, 248)",
                    }
                ],
            ),
            html.Button("Submit Vols", id="tab2-submitVol"),
        ]
    ),
    className="mt-3",
)

# main tab holder
tabs = dbc.Tabs(
    [
        dbc.Tab(tab1_content, label="LME"),
        dbc.Tab(tab2_content, label="Euronext"),
    ]
)

layout = html.Div(
    [
        dcc.Interval(
            id="vol-update", interval=interval, n_intervals=0  # in milliseconds
        ),
        topMenu("Vola Matrix"),
        tabs,
        # options,
        hidden,
        graphs,
    ]
)


def initialise_callbacks(app):
    # pulltrades use hiddien inputs to trigger update on new trade
    @app.callback(
        Output("tab1-volsTable", "data"),
        [Input("tab1-volProduct", "value"), Input("tab1-fit-val", "n_clicks")],
        [State("tab1-volsTable", "data")],
    )
    def update_trades(portfolio, click, data):
        # figure out which button triggered the callback
        button_id = ctx.triggered_id if not None else "No clicks yet"

        if portfolio:
            if button_id == "fit-val":
                # retrive settlement volas
                settlement_vols = pd.read_sql(
                    "SELECT * from public.get_settlement_vols()",
                    Connection("Sucden-sql-soft", georgiadatabase),
                )

                # create instruemnt from LME values
                settlement_vols["instrument"] = settlement_vols.apply(
                    lambda row: lme_option_to_georgia(row["Product"], row["Series"]),
                    axis=1,
                )
                settlement_vols["instrument"] = settlement_vols[
                    "instrument"
                ].str.upper()
                settlement_vols = settlement_vols[
                    ~settlement_vols["instrument"].duplicated(keep="first")
                ]

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

    # update euronext vols table 
    @app.callback(
        Output("tab2-volsTable", "data"),
        [Input("tab2-volProduct", "value"), Input("tab2-fit-val", "n_clicks")],
        [State("tab2-volsTable", "data")],
    )
    def update_trades(portfolio, click, data):
        # figure out which button triggered the callback
        button_id = ctx.triggered_id if not None else "No clicks yet"

        if portfolio:
            optionsList = loadEUROptions(portfolio)
            list2 = []
            for option in optionsList:
               print(option.vol_surfaces.params)
               #print(option)
            
            #first = optionsList.first().symbol
            print(list2[0])
            #this is just a test to see if I can get the options from the portfolio    
            # [
            #     {
            #         # "holiday_id": holiday.holiday_id,
            #         "params": holiday.holiday_date,
            #     }
            #     for holiday in product.holidays
            # ]
            # [
            #     {
            #         # "holiday_id": holiday.holiday_id,
            #         "params": vol.params,
            #     }
            #     for vol in option.vol_surfaces
            # ] 
                    

        return


    # load sol3 vols
    @app.callback(
        Output("sol_vols", "data"),
        [Input("tab1-volProduct", "value"), Input("vol-update", "n_intervals")],
    )
    def update_sol_vols(portfolio, interval):
        if portfolio:
            dict, sol_vol = pulVols(portfolio)
            return sol_vol
        else:
            no_update

    # loop over table and send all vols to redis
    @app.callback(
        Output("tab1-volProduct", "value"),
        [Input("tab1-submitVol", "n_clicks")],
        [State("tab1-volsTable", "data"), State("tab1-volProduct", "value")],
    )
    def update_trades(clicks, data, portfolio):
        if clicks != None:
            for row in data:
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
                sumbitVolas(product.lower(), cleaned_df, user, dev_keys=USE_DEV_KEYS)

            return portfolio
        else:
            return no_update

    # Load greeks for active cell
    @app.callback(
        Output("Vol_surface", "figure"),
        [Input("tab1-volsTable", "active_cell"), Input("vol-update", "n_intervals")],
        [State("tab1-volsTable", "data"), State("sol_vols", "data")],
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
        [Input("tab1-volsTable", "active_cell")],
        [State("tab1-volsTable", "data")],
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
