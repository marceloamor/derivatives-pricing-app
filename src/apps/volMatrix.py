from data_connections import Session, conn, PostGresEngine
from sql import histroicParams
from parts import (
    topMenu,
    loadRedisData,
    buildParamMatrix,
    sumbitVolasLME,
    onLoadPortFolio,
    lme_option_to_georgia,
    georgiaLabel,
    calculate_time_remaining,
    convert_georgia_option_symbol_to_expiry,
    get_product_holidays,
)

import upestatic

from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
from dash import dash_table as dtable
from dash import no_update, dcc
from dash import html, ctx
from flask import request
import pandas as pd
import numpy as np

from datetime import datetime
from functools import partial
from typing import List, Union
import json
import os


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
    {"name": "Forward Vol", "id": "forward_vol", "editable": False},
]

EURcolumns = [
    {"name": "product", "id": "product", "editable": False},
    {"name": "vola", "id": "vola", "editable": True},
    {"name": "skew", "id": "skew", "editable": True},
    {"name": "puts", "id": "puts", "editable": True},
    {"name": "calls", "id": "calls", "editable": True},
    {"name": "put_x", "id": "put_x", "editable": True},
    {"name": "call_x", "id": "call_x", "editable": True},
    {"name": "Forward Vol", "id": "forward_vol", "editable": False},
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
        products = (
            session.query(upestatic.Product)
            .where(upestatic.Product.exchange_symbol == "xext")
            .all()
        )
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
        optionsList = [option for option in product.options]
        return optionsList


def loadEURParams(surface_id):
    with Session() as session:
        surface = (
            session.query(upestatic.VolSurface)
            .where(upestatic.VolSurface.vol_surface_id == surface_id)
            .first()
        )
        params = surface.params
        return params


def pulVols(portfolio):
    # pull matrix inputs
    dff, sol_curve = buildParamMatrix(portfolio.capitalize(), dev_keys=USE_DEV_KEYS)
    # create product column
    dff["product"] = dff.index
    dff["prompt"] = pd.to_datetime(dff["prompt"], format="%d/%m/%Y")
    dff["expiration"] = dff["product"].apply(convert_georgia_option_symbol_to_expiry)
    dff = dff.sort_values(["prompt"], na_position="first")

    holiday_dates = get_product_holidays(portfolio.lower())
    t_to_expiration_calc_func_partial = partial(
        calculate_time_remaining,
        holiday_list=holiday_dates,
        holiday_weight_list=[1.0 for _ in range(len(holiday_dates))],
        weekmask=[1, 1, 1, 1, 1, 0, 0],
        _apply_time_corrections=False,
    )
    results = dff["expiration"].apply(t_to_expiration_calc_func_partial)
    t_to_expiry = [result[0] for result in results]
    dff["t_to_expiry"] = t_to_expiry
    # print(dff)
    front_month_forward_vola = dff.iloc[0, :]["vol"]
    front_month_time_to_expiration = dff.iloc[0, :]["t_to_expiry"]
    dff["forward_vol"] = dff["vol"] * 100
    dff_skip_first = dff.iloc[1:, :]
    dff_skip_first["forward_vol"] = (
        np.sqrt(
            (
                dff_skip_first["t_to_expiry"] * dff_skip_first["vol"] ** 2
                - front_month_time_to_expiration * front_month_forward_vola**2
            )
            / (dff_skip_first["t_to_expiry"] - front_month_time_to_expiration)
        )
        * 100
    ).round(2)

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


graphsLME = html.Div(
    [
        georgiaLabel("Vol Surface"),
        dcc.Graph(id="Vol_surface"),
        html.Div(
            [
                georgiaLabel("ATM Vol"),
                dcc.Loading(
                    type="circle",
                    children=[html.Div([dcc.Graph(id="atmvolGraph")])],
                    className="six columns",
                ),
                georgiaLabel("+10 Delta"),
                dcc.Loading(
                    type="circle",
                    children=[html.Div([dcc.Graph(id="plus10Graph")])],
                    className="six columns",
                ),
                georgiaLabel("+25 Delta"),
                dcc.Loading(
                    type="circle",
                    children=[html.Div([dcc.Graph(id="plus25Graph")])],
                    className="six columns",
                ),
            ],
            className="row",
        ),
        html.Div(
            [
                georgiaLabel("-10 Delta"),
                dcc.Loading(
                    type="circle",
                    children=[html.Div([dcc.Graph(id="minus10Graph")])],
                    className="six columns",
                ),
                georgiaLabel("-25 Delta"),
                dcc.Loading(
                    type="circle",
                    children=[html.Div([dcc.Graph(id="minus25Graph")])],
                    className="six columns",
                ),
            ],
            className="row",
        ),
    ]
)

graphsEUR = html.Div(
    [
        georgiaLabel("ATM Vol"),
        dcc.Graph(id="tab2-Vol_surface"),
        html.Div(
            [
                georgiaLabel("ATM Vol"),
                dcc.Loading(
                    type="circle",
                    children=[html.Div([dcc.Graph(id="tab2-volGraph")])],
                    className="six columns",
                ),
                georgiaLabel("Skew"),
                dcc.Loading(
                    type="circle",
                    children=[html.Div([dcc.Graph(id="tab2-skewGraph")])],
                    className="six columns",
                ),
            ],
            className="row",
        ),
        html.Div(
            [
                georgiaLabel("Puts"),
                dcc.Loading(
                    type="circle",
                    children=[html.Div([dcc.Graph(id="tab2-putsGraph")])],
                    className="six columns",
                ),
                georgiaLabel("Calls"),
                dcc.Loading(
                    type="circle",
                    children=[html.Div([dcc.Graph(id="tab2-callsGraph")])],
                    className="six columns",
                ),
            ],
            className="row",
        ),
        html.Div(
            [
                georgiaLabel("put_x"),
                dcc.Loading(
                    type="circle",
                    children=[html.Div([dcc.Graph(id="tab2-put_xGraph")])],
                    className="six columns",
                ),
                georgiaLabel("call_x"),
                dcc.Loading(
                    type="circle",
                    children=[html.Div([dcc.Graph(id="tab2-call_xGraph")])],
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
            html.Br(),
            html.Br(),
            graphsLME,
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
                columns=EURcolumns,
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
            html.Br(),
            html.Br(),
            graphsEUR,
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
        hidden,
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
            if button_id == "tab1-fit-val":
                # retrive settlement volas
                settlement_vols = pd.read_sql(
                    "SELECT * from public.get_settlement_vols()",
                    PostGresEngine(),
                )
                print(settlement_vols)

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
                print(settlement_vols)

                # convert data to dataframe
                data = pd.DataFrame.from_dict(data)
                print(data)
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
                print(data)

                # convert to dict
                dict = data.to_dict("records")
                # print(dict)

                return dict

            else:
                dict, sol_vol = pulVols(portfolio)
                # print(dict)
                return dict
        else:
            no_update

    # update euronext vols table
    @app.callback(
        Output("tab2-volsTable", "data"),
        [Input("tab2-volProduct", "value"), Input("tab2-fit-val", "n_clicks")],
        [State("tab2-volsTable", "data")],
    )
    def update_trades_eur(portfolio, click, data):
        # figure out which button triggered the callback
        button_id = ctx.triggered_id if not None else "No clicks yet"

        if portfolio:
            columns = [{"name": "product", "id": "product", "editable": False}]
            columns.append(
                {"name": param, "id": param, "editable": True} for param in columns
            )

            with Session() as session:
                options = (
                    session.query(upestatic.Option, upestatic.VolSurface.params)
                    .join(upestatic.VolSurface)
                    .filter(upestatic.Option.expiry >= datetime.now())
                    .filter(
                        upestatic.Option.product.has(
                            upestatic.Product.exchange_symbol == "xext"
                        )
                    )
                    .order_by(upestatic.Option.expiry.asc())
                    .all()
                )
                euronext_milling_wheat_product: Union[upestatic.Product, None] = (
                    session.query(upestatic.Product)
                    .filter(upestatic.Product.symbol == "xext-ebm-eur")
                    .one_or_none()
                )
                if not isinstance(euronext_milling_wheat_product, upestatic.Product):
                    print("Tried to retrieve EBM product in volmatrix and failed")
                    raise TypeError("Unable to retrieve EBM product, got `None`")

                holiday_list: List[
                    upestatic.Holiday
                ] = euronext_milling_wheat_product.holidays
            holiday_weights, holiday_dates = [], []
            for holiday in holiday_list:
                holiday_weights.append(holiday.holiday_weight)
                holiday_dates.append(holiday.holiday_date)

            df_in_list = []
            for p, d in options:
                df_in_list.append(
                    {
                        "product": p.symbol,
                        "vola": format(float(d["vola"]) * 100, ".2f"),
                        "skew": format(float(d["skew"]) * 100, ".2f"),
                        "puts": format(float(d["puts"]) * 100, ".2f"),
                        "calls": format(float(d["calls"]) * 100, ".2f"),
                        "put_x": d["put_x"],
                        "call_x": d["call_x"],
                        # The one problem with using this is that everything will basis the
                        # holidays in the next calendar year over NYD, be aware of discrepancies
                        # and changes this can cause
                        "t_to_expiry": calculate_time_remaining(
                            p.expiry,
                            holiday_list=holiday_dates,
                            holiday_weight_list=holiday_weights,
                            weekmask=[1, 1, 1, 1, 1, 0, 0],
                            _apply_time_corrections=False,
                        )[0],
                    }
                )

            df = pd.DataFrame(df_in_list)

            front_month_forward_vola = float(df.iloc[0, :]["vola"]) / 100
            front_month_time_to_expiration = df.iloc[0, :]["t_to_expiry"]
            df["forward_vol"] = front_month_forward_vola * 100
            df.iloc[1:, :]["forward_vol"] = (
                np.sqrt(
                    (
                        df.iloc[1:, :]["t_to_expiry"]
                        * (df.iloc[1:, :]["vola"].astype(float) / 100) ** 2
                        - front_month_time_to_expiration * front_month_forward_vola**2
                    )
                    / (df.iloc[1:, :]["t_to_expiry"] - front_month_time_to_expiration)
                )
                * 100
            ).round(2)

            dict = df.to_dict("records")

        return dict

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

    # loop over table and send all vols to redis - LME
    @app.callback(
        Output("tab1-volProduct", "value"),
        [Input("tab1-submitVol", "n_clicks")],
        [State("tab1-volsTable", "data"), State("tab1-volProduct", "value")],
    )
    def update_trades(clicks, data, portfolio):
        if clicks != None:
            index = 0
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

                if USE_DEV_KEYS:
                    stored = json.loads(conn.get(product.lower() + "Vola:dev"))
                else:
                    stored = json.loads(conn.get(product.lower() + "Vola"))

                for key, value in cleaned_df.items():
                    cleaned_df[key] = round(value, 4)
                for key, value in stored.items():
                    stored[key] = round(value, 4)
                # print((stored))
                # print((cleaned_df))
                if stored != cleaned_df:
                    sumbitVolasLME(
                        product.lower(), cleaned_df, user, index, dev_keys=USE_DEV_KEYS
                    )
                    index += 1

            return portfolio
        else:
            return no_update

    # EUR - loop over table and send all vols to database
    @app.callback(
        Output("tab2-volProduct", "value"),
        [Input("tab2-submitVol", "n_clicks")],
        [State("tab2-volsTable", "data"), State("tab2-volProduct", "value")],
    )
    def update_trades(clicks, data, portfolio):
        if clicks != None:
            index = 0
            for row in data:
                # collect data for vol submit
                product = row["product"]
                # repeated type coercion to make sure option engine is happy, and ensure a good UI
                cleaned_df = {
                    "vola": float(format((float(row["vola"]) / 100), ".7f")),
                    "skew": float(format((float(row["skew"]) / 100), ".7f")),
                    "puts": float(format((float(row["puts"]) / 100), ".7f")),
                    "calls": float(format((float(row["calls"]) / 100), ".7f")),
                    "put_x": float(row["put_x"]),
                    "call_x": float(row["call_x"]),
                }
                # submit vol and DB
                # Get the VolSurfaceID
                with Session() as session:
                    vol_surface_id = (
                        session.query(upestatic.Option.vol_surface_id)
                        .filter(upestatic.Option.symbol == product)
                        .scalar()
                    )
                    # check current params against stored params
                    storedParams = (
                        session.query(upestatic.VolSurface.params)
                        .filter(upestatic.VolSurface.vol_surface_id == vol_surface_id)
                        .scalar()
                    )
                    # if params have changed, update the DB
                    if storedParams != cleaned_df:
                        session.query(upestatic.VolSurface).filter(
                            upestatic.VolSurface.vol_surface_id == vol_surface_id
                        ).update({upestatic.VolSurface.params: cleaned_df})
                        session.commit()

                        # tell option engine to update vols
                        if index == 0:
                            json_data = json.dumps([product, "staticdata"])
                            conn.publish("compute_ext_new", json_data)
                        else:
                            json_data = json.dumps([product, "update"])
                            conn.publish("compute_ext_new", json_data)
                        index += 1

            return portfolio
        else:
            return no_update

    # Load greeks for active cell - LME
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

    # Load greeks for active cell - EUR
    @app.callback(
        Output("tab2-Vol_surface", "figure"),
        [Input("tab2-volsTable", "active_cell"), Input("vol-update", "n_intervals")],
        [State("tab2-volsTable", "data")],  # , State("sol_vols", "data")],
    )
    def updateData(cell, interval, data):
        if data and cell:
            product = data[cell["row"]]["product"]
            graphData = []

            # get georgia vols from redis
            raw_vols = conn.get(product)
            vols_data = pd.DataFrame.from_dict(json.loads(raw_vols), orient="index")
            vols_data = vols_data[vols_data["cop"] == "c"]

            strikes = vols_data["strike"].values
            vols = vols_data["vol"].values

            # data.append({"x": strikes, "y": vols, "type": "line", "name": "Vola"})

            # get settlement vols from postgres
            with Session() as session:
                most_recent_date = (
                    session.query(upestatic.SettlementVol.settlement_date)
                    .filter(upestatic.SettlementVol.option_symbol == product)
                    .order_by(upestatic.SettlementVol.settlement_date.desc())
                    .first()[0]
                )

                # Query the 'strike' and 'volatility' values for the most recent date and specific product
                results = (
                    session.query(
                        upestatic.SettlementVol.strike,
                        upestatic.SettlementVol.volatility,
                    )
                    .filter(
                        upestatic.SettlementVol.option_symbol == product,
                        upestatic.SettlementVol.settlement_date == most_recent_date,
                    )
                    .all()
                )

                # Extract 'strike' and 'volatility' values into separate lists
                settle_strikes = [row.strike for row in results]
                settle_vols = [(row.volatility / 100) for row in results]

            # Find the indices of the range of settle_strikes within strikes
            min_idx = np.argmax(strikes >= settle_strikes[0])
            max_idx = np.argmin(strikes <= settle_strikes[-1])

            # Trim strikes to the range of settle_strikes
            trimmed_strikes = strikes[min_idx : max_idx + 1]

            # Trim vols to the same length as trimmed_strikes
            trimmed_vols = vols[min_idx : max_idx + 1]

            # Plot trimmed_vols and settle_vols using the same x-axis (settle strikes has a smaller range)
            data.append(
                {
                    "x": trimmed_strikes,
                    "y": trimmed_vols,
                    "type": "line",
                    "name": "Vola",
                }
            )
            data.append(
                {
                    "x": settle_strikes,
                    "y": settle_vols,
                    "type": "line",
                    "name": "Settlement Vols",
                }
            )

            # data.append(
            #     {
            #         "x": settle_strikes,
            #         "y": settle_vols,
            #         "type": "line",
            #         "name": "Settlement Vols",
            #     }
            # )

            return {"data": data}

        else:
            return no_update

    ##update graphs on data update - LME
    @app.callback(
        [
            Output("atmvolGraph", "figure"),
            Output("plus10Graph", "figure"),
            Output("plus25Graph", "figure"),
            Output("minus10Graph", "figure"),
            Output("minus25Graph", "figure"),
        ],
        [Input("tab1-volsTable", "active_cell")],
        [State("tab1-volsTable", "data")],
    )
    def load_param_graph(cell, data):
        if cell == None:
            return no_update, no_update, no_update, no_update, no_update
        else:
            if data[0] and cell:
                product = data[cell["row"]]["product"]
                if product:
                    df = histroicParams(product)
                    dates = df["datetime"].values

                    # figure out which is -10,-25,+10,+25 to label properly
                    var1 = df["var1"] * 100
                    var2 = (df["var2"] - df["var1"]) * 100  # +10
                    var3 = (df["var3"] - df["var1"]) * 100  # +25
                    var4 = (df["var4"] - df["var1"]) * 100  # -25
                    var5 = (df["var5"] - df["var1"]) * 100  # -10

                    atmVol = {
                        "data": [
                            {
                                "x": dates,
                                "y": var1,
                                "type": "line",
                                "name": "ATM Vol",
                            }
                        ]
                    }
                    plus10 = {
                        "data": [
                            {
                                "x": dates,
                                "y": var2,
                                "type": "line",
                                "name": "Vola",
                            }
                        ]
                    }
                    plus25 = {
                        "data": [
                            {
                                "x": dates,
                                "y": var3,
                                "type": "line",
                                "name": "Skew",
                            }
                        ]
                    }
                    minus10 = {
                        "data": [
                            {
                                "x": dates,
                                "y": var4,
                                "type": "line",
                                "name": "Call",
                            }
                        ]
                    }
                    minus25 = {
                        "data": [
                            {
                                "x": dates,
                                "y": var5,
                                "type": "line",
                                "name": "Put",
                            }
                        ]
                    }

                    return atmVol, plus10, plus25, minus10, minus25
            else:
                return no_update, no_update, no_update, no_update, no_update

    ##update graphs on data update - EUR
    @app.callback(
        [
            Output("tab2-volGraph", "figure"),
            Output("tab2-skewGraph", "figure"),
            Output("tab2-putsGraph", "figure"),
            Output("tab2-callsGraph", "figure"),
            Output("tab2-put_xGraph", "figure"),
            Output("tab2-call_xGraph", "figure"),
        ],
        [Input("tab2-volsTable", "active_cell")],
        [State("tab2-volsTable", "data")],
    )
    def load_param_graph(cell, data):
        if cell == None:
            return no_update, no_update, no_update, no_update, no_update, no_update
        else:
            if data[0] and cell:
                product = data[cell["row"]]["product"]
                if product:
                    # pull all historic params for product
                    with Session() as session:
                        volSurfaceID = (
                            session.query(upestatic.Option.vol_surface_id).filter(
                                upestatic.Option.symbol == product
                            )
                            # .order_by(upestatic.SettlementVol.settlement_date.desc())
                            .scalar()
                        )

                        # pull dates and params for product
                        results = (
                            session.query(
                                upestatic.HistoricalVolSurface.update_datetime,
                                upestatic.HistoricalVolSurface.params,
                            )
                            .filter(
                                upestatic.HistoricalVolSurface.vol_surface_id
                                == volSurfaceID
                            )
                            .order_by(
                                upestatic.HistoricalVolSurface.update_datetime.desc()
                            )
                            .all()
                        )

                        # extract dates and params from results for plotting
                        dates = [row.update_datetime for row in results]
                        params = [row.params for row in results]

                    # vola = skew = puts = calls = put_x = call_x = []
                    vola, skew, puts, calls, put_x, call_x = [], [], [], [], [], []

                    for dictionary in params:
                        # extract the values for each key and append to the corresponding array
                        vola.append(dictionary["vola"])
                        skew.append(dictionary["skew"])
                        puts.append(dictionary["puts"])
                        calls.append(dictionary["calls"])
                        put_x.append(dictionary["put_x"])
                        call_x.append(dictionary["call_x"])

                    volFig = {
                        "data": [
                            {
                                "x": dates,
                                "y": vola,
                                "type": "line",
                                "name": "Vola",
                            }
                        ]
                    }
                    skewFig = {
                        "data": [
                            {
                                "x": dates,
                                "y": skew,
                                "type": "line",
                                "name": "Skew",
                            }
                        ]
                    }
                    putsFig = {
                        "data": [
                            {
                                "x": dates,
                                "y": puts,
                                "type": "line",
                                "name": "Call",
                            }
                        ]
                    }
                    callsFig = {
                        "data": [
                            {
                                "x": dates,
                                "y": calls,
                                "type": "line",
                                "name": "Put",
                            }
                        ]
                    }
                    put_xFig = {
                        "data": [
                            {
                                "x": dates,
                                "y": put_x,
                                "type": "line",
                                "name": "Call",
                            }
                        ]
                    }
                    call_xFig = {
                        "data": [
                            {
                                "x": dates,
                                "y": call_x,
                                "type": "line",
                                "name": "Put",
                            }
                        ]
                    }

                    return volFig, skewFig, putsFig, callsFig, put_xFig, call_xFig
            else:
                return no_update, no_update, no_update, no_update, no_update, no_update
