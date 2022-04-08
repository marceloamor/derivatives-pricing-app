from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_table as dtable
from flask import request

from sql import sendTrade
from parts import (
    topMenu,
    sendPosQueueUpdate,
    expiryProcess,
    timeStamp,
    updateRedisDelta,
    updateRedisPos,
    updateRedisTrade,
    updatePos,
    onLoadProduct,
)
from TradeClass import TradeClass

# column options for trade table
columns = [
    {"name": "Instrument", "id": "instrument"},
    {"name": "Action", "id": "action"},
    {"name": "Price", "id": "price"},
    {"name": "Quanitity", "id": "quanitity"},
    {"name": "Venue", "id": "tradingVenue"},
]

options = dbc.Row(
    [
        dbc.Col([dcc.Input("ref", placeholder="Enter SP")], width=3),
        dbc.Col([dcc.Dropdown("product", options=onLoadProduct())], width=3),
        dbc.Col(
            [html.Button("Run", id="run", style={"background": "#F1C40F"})], width=3
        ),
        dbc.Col(
            [html.Button("Expiry", id="expiry", style={"background": "#F1C40F"})],
            width=3,
        ),
        dbc.Col(
            [
                html.Button(
                    "Select All", id="all-button", style={"background": "#F1C40F"}
                )
            ],
            width=3,
        ),
    ]
)

table = dbc.Col(html.Div(id="tableHolder"))

layout = html.Div(
    [
        topMenu("Expiry"),
        html.Div(id="trade-div", style={"display": "none"}),
        options,
        dbc.Row([table]),
    ]
)


def initialise_callbacks(app):
    # pulltrades use hiddien inputs to trigger update on new trade
    @app.callback(
        Output("tableHolder", "children"),
        [Input("run", "n_clicks")],
        [State("ref", "value"), State("product", "value")],
    )
    def update_expiry(click, ref, product):

        if click:
            # pull data via expiry process
            dff = expiryProcess(product, float(ref))

            # turn to dict and send to the table
            dict = dff.to_dict("records")
            return dtable.DataTable(
                id="expiryTable",
                columns=columns,
                data=dict,
                row_selectable="multi",
                editable=True,
            )

    # send trade to system
    @app.callback(
        Output("trade-div", "children"),
        [Input("expiry", "n_clicks")],
        [State("expiryTable", "selected_rows"), State("expiryTable", "data")],
    )
    def sendTrades(clicks, indices, rows):
        timestamp = timeStamp()
        # pull username from site header
        user = request.headers.get("X-MS-CLIENT-PRINCIPAL-NAME")
        if not user:
            user = "Test"

        if indices:
            for i in indices:
                # create st to record which products to update in redis
                redisUpdate = set([])
                # check that this is not the total line.
                if rows[i]["instrument"] != "Total":

                    if rows[i]["instrument"][3] == "O":
                        # is option
                        product = rows[i]["instrument"][:6]
                        redisUpdate.add(product)
                        productName = (rows[i]["instrument"]).split(" ")
                        strike = productName[1]
                        CoP = productName[2]

                        prompt = rows[i]["prompt"]
                        price = rows[i]["price"]
                        qty = rows[i]["quanitity"]
                        counterparty = "EXPIRY"

                        trade = TradeClass(
                            0,
                            timestamp,
                            product,
                            strike,
                            CoP,
                            prompt,
                            price,
                            qty,
                            counterparty,
                            "",
                            user,
                            "Georgia",
                        )
                        # send trade to DB and record ID returened

                        trade.id = sendTrade(trade)
                        updatePos(trade)

                    elif rows[i]["instrument"][3] == " ":
                        # is futures
                        product = rows[i]["instrument"][:3]
                        redisUpdate.add(product)
                        prompt = rows[i]["prompt"]
                        price = rows[i]["price"]
                        qty = rows[i]["quanitity"]
                        counterparty = "EXPIRY FUTURE"

                        trade = TradeClass(
                            0,
                            timestamp,
                            product,
                            None,
                            None,
                            prompt,
                            price,
                            qty,
                            counterparty,
                            "",
                            user,
                            "Georgia",
                        )
                        # send trade to DB and record ID returened
                        trade.id = sendTrade(trade)
                        updatePos(trade)

                    # update redis for each product requirng it
                    for update in redisUpdate:
                        updateRedisDelta(update)
                        updateRedisPos(update)
                        updateRedisTrade(update)
                        sendPosQueueUpdate(update)
            return True

    # use callback to select all rows in expiry table
    @app.callback(
        [
            Output("expiryTable", "selected_rows"),
        ],
        [
            Input("all-button", "n_clicks"),
        ],
        [
            State("expiryTable", "derived_virtual_data"),
        ],
    )
    def select_all(n_clicks, selected_rows):
        if selected_rows is None:

            return [[]]
        else:
            return [[i for i in range(len(selected_rows))]]
