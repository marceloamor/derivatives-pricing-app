from dash.dependencies import Input, Output, State
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash import dcc
from dash import dash_table as dtable
from flask import request

from sql import sendTrade
import sql_utils
from parts import (
    topMenu,
    sendPosQueueUpdate,
    expiryProcess,
    expiryProcessEUR,
    timeStamp,
    updateRedisDelta,
    updateRedisPos,
    updateRedisTrade,
    updatePos,
    onLoadProduct,
)
from TradeClass import TradeClass
import time
from datetime import datetime
from data_connections import engine, Session, PostGresEngine, conn
import sqlalchemy, traceback, os, pickle
import pandas as pd
import upestatic

legacyEngine = PostGresEngine()

USE_DEV_KEYS = os.getenv("USE_DEV_KEYS", "false").lower() in [
    "true",
    "t",
    "1",
    "y",
    "yes",
]
dev_key_redis_append = "" if not USE_DEV_KEYS else ":dev"

# column options for trade table
columns = [
    {"name": "Instrument", "id": "instrument"},
    {"name": "Action", "id": "action"},
    {"name": "Price", "id": "price"},
    {"name": "Quanitity", "id": "quanitity"},
    {"name": "Venue", "id": "tradingVenue"},
]


# append euronext  from both static datas
def onLoadProductsPlusEuronext():
    lme_options = onLoadProduct()
    eur_options = []
    with Session() as session:
        eur_products = (
            session.query(upestatic.Product.symbol)
            .filter(upestatic.Product.exchange_symbol == "xext")
            .all()
        )
        # double loop ready for when more euronext products are added to static data
        for product in eur_products:
            options = (
                session.query(upestatic.Option.symbol)
                .filter(upestatic.Option.product_symbol == product[0])
                .filter(upestatic.Option.expiry >= datetime.now())
                .all()
            )
            for option in options:
                lme_options.append(
                    {"label": option[0].upper(), "value": option[0].upper()}
                )
    return lme_options


options = dbc.Row(
    [
        dbc.Col([dcc.Input(id="ref", placeholder="Enter SP")], width=3),
        dbc.Col(
            [dcc.Dropdown(id="product", options=onLoadProductsPlusEuronext())], width=3
        ),
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
            if product[:1].upper() == "X":
                dff = expiryProcessEUR(product, float(ref))
            else:
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
        if clicks is None:
            return True
        timestamp = timeStamp()
        # pull username from site header
        user = request.headers.get("X-MS-CLIENT-PRINCIPAL-NAME")
        if not user:
            user = "Test"

        if indices:
            # set variables shared by all trades
            packaged_trades_to_send_legacy = []
            packaged_trades_to_send_new = []
            trader_id = 0
            upsert_pos_params = []
            trade_time_ns = time.time_ns()
            booking_dt = datetime.utcnow()
            processed_user = user.replace(" ", "").split("@")[0]

            exchange = (
                "xext" if rows[indices[0]]["instrument"][:1].upper() == "X" else "lme"
            )

            with engine.connect() as pg_db2_connection:
                stmt = sqlalchemy.text(
                    "SELECT trader_id FROM traders WHERE email = :user_email"
                )
                result = pg_db2_connection.execute(
                    stmt, {"user_email": user.lower()}
                ).scalar_one_or_none()
                if result is None:
                    trader_id = -101
                else:
                    trader_id = result

            for i in indices:
                # create st to record which products to update in redis
                redisUpdate = set([])
                # check that this is not the total line.
                if rows[i]["instrument"] != "Total":
                    if rows[i]["instrument"][3] == "O":
                        # is option
                        product = rows[i]["instrument"][:6]
                        instrument = rows[i]["instrument"]
                        redisUpdate.add(product)
                        productName = (rows[i]["instrument"]).split(" ")
                        strike = productName[1]
                        CoP = productName[2]

                        prompt = rows[i]["prompt"]
                        price = rows[i]["price"]
                        qty = rows[i]["quanitity"]
                        counterparty = "EXPIRY"

                        georgia_trade_id = (
                            f"expiry{exchange}.{processed_user}.{trade_time_ns}:{i}"
                        )

                        packaged_trades_to_send_legacy.append(
                            sql_utils.LegacyTradesTable(
                                dateTime=booking_dt,
                                instrument=instrument,
                                price=price,
                                quanitity=qty,
                                theo=0.0,
                                user=user,
                                counterPart=counterparty,
                                Comment=f"{exchange.upper()} EXPIRY",
                                prompt=prompt,
                                venue="Georgia",
                                deleted=0,
                                venue_trade_id=georgia_trade_id,
                            )
                        )
                        packaged_trades_to_send_new.append(
                            sql_utils.TradesTable(
                                trade_datetime_utc=booking_dt,
                                instrument_symbol=instrument,
                                quantity=qty,
                                price=price,
                                portfolio_id=1 if exchange == "lme" else 3,
                                trader_id=trader_id,
                                notes="LME EXPIRY",
                                venue_name="Georgia",
                                venue_trade_id=georgia_trade_id,
                                counterparty=counterparty,
                            )
                        )
                        upsert_pos_params.append(
                            {
                                "qty": qty,
                                "instrument": instrument,
                                "tstamp": booking_dt,
                            }
                        )
                    elif rows[i]["instrument"][3] == " ":
                        # is futures
                        product = rows[i]["instrument"][:3]  # format= PBD PR-OM-PT
                        instrument = rows[i]["instrument"]
                        # redisUpdate.add(product)
                        prompt = rows[i]["prompt"]
                        price = rows[i]["price"]
                        qty = rows[i]["quanitity"]
                        counterparty = "EXPIRY FUTURE"

                        georgia_trade_id = (
                            f"expiry{exchange}.{processed_user}.{trade_time_ns}:{i}"
                        )

                        packaged_trades_to_send_legacy.append(
                            sql_utils.LegacyTradesTable(
                                dateTime=booking_dt,
                                instrument=instrument,
                                price=price,
                                quanitity=qty,
                                theo=0.0,
                                user=user,
                                counterPart=counterparty,
                                Comment=f"{exchange.upper()} EXPIRY",
                                prompt=prompt,
                                venue="Georgia",
                                deleted=0,
                                venue_trade_id=georgia_trade_id,
                            )
                        )
                        packaged_trades_to_send_new.append(
                            sql_utils.TradesTable(
                                trade_datetime_utc=booking_dt,
                                instrument_symbol=instrument,
                                quantity=qty,
                                price=price,
                                portfolio_id=1 if exchange == "lme" else 3,
                                trader_id=trader_id,
                                notes="LME EXPIRY",
                                venue_name="Georgia",
                                venue_trade_id=georgia_trade_id,
                                counterparty=counterparty,
                            )
                        )
                        upsert_pos_params.append(
                            {
                                "qty": qty,
                                "instrument": instrument,
                                "tstamp": booking_dt,
                            }
                        )
            # options and futures built, double booking trades
            # new table
            try:
                with sqlalchemy.orm.Session(engine, expire_on_commit=False) as session:
                    session.add_all(packaged_trades_to_send_new)
                    session.commit()
            except Exception as e:
                print("Exception while attempting to book trade in new standard table")
                print(traceback.format_exc())
                return True
            # legacy table
            try:
                with sqlalchemy.orm.Session(legacyEngine) as session:
                    session.add_all(packaged_trades_to_send_legacy)
                    pos_upsert_statement = sqlalchemy.text(
                        "SELECT upsert_position(:qty, :instrument, :tstamp)"
                    )
                    _ = session.execute(pos_upsert_statement, params=upsert_pos_params)
                    session.commit()
            except Exception as e:
                print("Exception while attempting to book trade in legacy table")
                print(traceback.format_exc())
                for trade in packaged_trades_to_send_new:
                    trade.deleted = True
                # to clear up new trades table assuming they were booked correctly
                with sqlalchemy.orm.Session(engine) as session:
                    session.add_all(packaged_trades_to_send_new)
                    session.commit()
                    return True

            # send trades to redis
            try:
                with legacyEngine.connect() as pg_connection:
                    trades = pd.read_sql("trades", pg_connection)
                    positions = pd.read_sql("positions", pg_connection)

                trades.columns = trades.columns.str.lower()
                positions.columns = positions.columns.str.lower()

                pipeline = conn.pipeline()
                pipeline.set("trades" + dev_key_redis_append, pickle.dumps(trades))
                pipeline.set(
                    "positions" + dev_key_redis_append, pickle.dumps(positions)
                )
                pipeline.execute()
            except Exception as e:
                print(
                    "Exception encountered while trying to update expiry redis trades/posi"
                )
                print(traceback.format_exc())
                return True

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
