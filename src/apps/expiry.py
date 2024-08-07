import logging
import os
import pickle
import time
import warnings
from datetime import datetime, timedelta

import dash_bootstrap_components as dbc
import display_names
import orjson
import pandas as pd
import sql_utils
import sqlalchemy
from dash import callback_context, dcc, html
from dash import dash_table as dtable
from dash.dependencies import Input, Output, State
from data_connections import PostGresEngine, conn, shared_engine, shared_session
from flask import request
from parts import (
    onLoadProduct,
    timeStamp,
    topMenu,
)
from upedata import dynamic_data as upe_dynamic
from upedata import static_data as upe_static

legacyEngine = PostGresEngine()
logger = logging.getLogger("frontend")

USE_DEV_KEYS = os.getenv("USE_DEV_KEYS", "false").lower() in [
    "true",
    "t",
    "1",
    "y",
    "yes",
]
if USE_DEV_KEYS:
    pass
dev_key_redis_append = "" if not USE_DEV_KEYS else ":dev"

# column options for trade table
columns = [
    # {"name": "Instrument", "id": "instrument_symbol"},
    {"name": "Display Name", "id": "display_name"},
    {"name": "Portfolio", "id": "portfolio_id"},
    {"name": "Action", "id": "action"},
    {"name": "Price", "id": "price"},
    {"name": "Quanitity", "id": "net_quantity"},
    {"name": "Venue", "id": "tradingVenue"},
]


def build_old_lme_symbol_from_new(symbol: str) -> str:
    # new format: XLME-LAD-USD O 23-04-17 A-254-C
    # new format: XLME-LAD-USD F 23-05-10
    # old format: LADOK4 2100 P
    # old format: LAD 2024-03-20

    monthCode = {
        "01": "f",
        "02": "g",
        "03": "h",
        "04": "j",
        "05": "k",
        "06": "m",
        "07": "n",
        "08": "q",
        "09": "u",
        "10": "v",
        "11": "x",
        "12": "z",
    }

    # split symbol into parts
    parts = symbol.split(" ")
    product = parts[0].split("-")[1]
    # convert prompt from YY-MM-DD to YYYY-MM-DD
    prompt = parts[2]
    month = prompt.split("-")[1]
    year = prompt.split("-")[0][-1]
    lme_code = monthCode[month]
    expiry = datetime.strptime(prompt, "%y-%m-%d").strftime("%Y-%m-%d")

    # build new symbol
    if parts[1].upper() == "F":
        new_symbol = f"{product} {expiry}"
    elif parts[1].upper() == "O":
        info = parts[3]
        strike = info.split("-")[1]
        option_type = info.split("-")[2]
        new_symbol = f"{product}O{lme_code}{year} {strike} {option_type}"
    return new_symbol.upper()


# new expiry process function, dynamic to all exchanges
def pull_expiry_data(product, ref):
    ##inputs to be entered from the page
    now = datetime.now().strftime("%Y-%m-%d")

    # load positions for product
    positions = conn.get("positions")
    positions = pickle.loads(positions)
    pos = pd.DataFrame.from_dict(positions)

    # filter for euronext
    pos = pos[pos["instrument"].str[:1] == "X"]

    # set option and future names
    option_name = product[:25].lower()
    with shared_session() as session:
        future_name = (
            session.query(upe_static.Option.underlying_future_symbol)
            .filter(upe_static.Option.symbol == option_name)
            .first()
        )[0].upper()

    # filter for just the month we are looking at
    pos = pos[pos["instrument"].str[:25].isin([product.upper()])]
    pos = pos[pos["quanitity"] != 0]

    # new data frame with split value columns
    pos["info"] = pos["instrument"].str.split(" ", n=3, expand=True)[3]
    pos[["_", "strike", "optionTypeId"]] = pos["info"].str.split("-", expand=True)

    # drop futures - no need as filtering for month already filtered out futures
    # pos = pos[pos["optionTypeId"].isin(["C", "P"])]

    # convert strike to float
    pos["strike"] = pos["strike"].astype(float)

    # remove partials
    posPartial = pos[pos["strike"] == ref]
    posPartial["action"] = "Partial"

    # reverse qty so it takes position out
    posPartial["quanitity"] = posPartial["quanitity"] * -1
    posPartial["price"] = 0

    # seperate into calls and puts
    posC = pos[pos["optionTypeId"] == "C"]
    posP = pos[pos["optionTypeId"] == "P"]

    # seperate into ITM and OTM
    posIC = posC[posC["strike"] < ref]
    posOC = posC[posC["strike"] > ref]
    posIP = posP[posP["strike"] > ref]
    posOP = posP[posP["strike"] < ref]

    # Create Df for out only
    out = pd.concat([posOC, posOP])
    out["action"] = "Abandon"

    # reverse qty so it takes position out
    out["quanitity"] = out["quanitity"] * -1

    # set price to Zero
    out["price"] = 0

    # build expiry futures trade df
    futC = posIC.reset_index(drop=True)
    futC["instrument"] = future_name
    # futC["prompt"] = thirdWed
    futC["action"] = "Exercise Future"
    futC["price"] = futC["strike"]
    futC["strike"] = None
    futC["optionTypeId"] = None

    futP = posIP.reset_index(drop=True)
    futP["instrument"] = future_name
    # futP["prompt"] = thirdWed
    futP["quanitity"] = futP["quanitity"] * -1
    futP["action"] = "Exercise Future"
    futP["price"] = futP["strike"]
    futP["strike"] = None
    futP["optionTypeId"] = None

    # build conteracting options position df
    posIP["quanitity"] = posIP["quanitity"].values * -1
    posIC["quanitity"] = posIC["quanitity"].values * -1
    posIP["action"] = "Exercised"
    posIC["action"] = "Exercised"
    posIP["price"] = 0
    posIC["price"] = 0

    # pull it all together
    all = out.append([futC, futP, posIP, posIC, posPartial])

    # add trading venue
    all["tradingVenue"] = "Exercise Process"

    # add trading time
    all["tradeDate"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    # drop the columns we dont need
    all.drop(
        ["delta", "index", "settlePrice", "index", "ID", "dateTime"],
        axis=1,
        inplace=True,
        errors="ignore",
    )

    return all


# append euronext  from both static datas
def onLoadProductsPlusEuronext():
    lme_options = onLoadProduct()
    eur_options = []
    with shared_session() as session:
        eur_products = (
            session.query(upe_static.Product.symbol)
            .filter(upe_static.Product.exchange_symbol == "xext")
            .all()
        )
        # double loop ready for when more euronext products are added to static data
        for product in eur_products:
            options = (
                session.query(upe_static.Option.symbol)
                .filter(upe_static.Option.product_symbol == product[0])
                .filter(upe_static.Option.expiry >= datetime.now())
                .all()
            )
            for option in options:
                lme_options.append(
                    {"label": option[0].upper(), "value": option[0].upper()}
                )
    return lme_options


# pull exchanges from db
def loadExchanges():
    with shared_session() as session:
        exchanges = session.query(upe_static.Exchange).all()
        exchangeList = []
        for exchange in exchanges:
            if exchange.symbol != "xtest":
                exchangeList.append({"label": exchange.name, "value": exchange.symbol})
    return exchangeList


exchangeList = loadExchanges()


exchangeDropdown = dcc.Dropdown(
    id="exchange-dropdown",
    options=exchangeList,
    value=exchangeList[0]["value"],
    clearable=False,
)
exchangeLabel = html.Label(
    ["Exchange:"], style={"font-weight": "bold", "text-align": "left"}
)

# instrument dropdown
productDropdown = dcc.Dropdown(
    id="product-dropdown",
    options=[],
    value=[],
    clearable=False,
)
productLabel = html.Label(
    ["Product:"], style={"font-weight": "bold", "text-align": "left"}
)

strikeInput = dcc.Input(
    id="strike-input",
    type="number",
    style={"width": "100%"},
)
strikeLabel = html.Label(["SP:"], style={"font-weight": "bold", "text-align": "left"})

runButton = dbc.Button(
    "Run",
    id="run-button",
)

selectAllButton = dbc.Button(
    "Select All",
    id="select-all-expiry",
)

expiryButton = dbc.Button(
    "Expiry",
    id="expiry-button",
    color="warning",
)

expiry_button_group = dbc.ButtonGroup(
    [runButton, selectAllButton],
    # vertical=True,
)


options = dbc.Row(
    [
        dbc.Col(html.Div(children=[exchangeLabel, exchangeDropdown]), width=3),
        dbc.Col(html.Div(children=[productLabel, productDropdown]), width=3),
        dbc.Col(html.Div(children=[strikeLabel, strikeInput]), width=1),
        dbc.Col(html.Div(children=[html.Br(), expiry_button_group]), width=2),
        # dbc.Col(html.Div(children=[html.Br(), runButton]), width=1),
        # dbc.Col(html.Div(children=[html.Br(), selectAllButton]), width=2),
        dbc.Col(html.Div(children=""), width=2),
        dbc.Col(html.Div(children=[html.Br(), expiryButton])),
    ]
)

# set up the table
expiryTable = dtable.DataTable(
    id="expiryTable",
    columns=columns,
    data=[],
    row_selectable="multi",
    editable=True,
)

alerts = html.Div(
    [
        dbc.Alert(
            "Expiry Trades Sent Successfully",
            id="tradeSent-expiry",
            dismissable=True,
            is_open=False,
            duration=5000,
            color="success",
        ),
        dbc.Alert(
            "Expiry Trades Sending Failed",
            id="tradeSentFail-expiry",
            dismissable=True,
            is_open=False,
            duration=5000,
            color="danger",
        ),
    ]
)

table = dbc.Col(html.Div(id="tableHolder"))

layout = html.Div(
    [
        topMenu("Expiry"),
        html.Div(
            [
                alerts,
                html.Div(id="trade-div", style={"display": "none"}),
                options,
                html.Div([expiryTable], className="mt-2"),
                dbc.Row([table]),
                dcc.Store(id="front-month-op"),
                dcc.Store(id="front-month-fut"),
            ],
            className="mx-3",
        ),
    ]
)


def initialise_callbacks(app):
    @app.callback(
        Output("product-dropdown", "options"),
        Output("product-dropdown", "value"),
        [Input("exchange-dropdown", "value")],
    )
    def update_expiry(exchange):
        product_list = []
        if exchange:
            with shared_session() as session:
                products = (
                    session.query(upe_static.Product)
                    .filter(upe_static.Product.exchange_symbol == exchange)
                    .all()
                )
                for product in products:
                    product_list.append(
                        {"label": product.long_name.title(), "value": product.symbol}
                    )
        return product_list, product_list[0]["value"]

    @app.callback(
        Output("strike-input", "placeholder"),
        Output("front-month-op", "value"),
        Output("front-month-fut", "value"),
        [Input("product-dropdown", "value")],
    )
    def update_expiry(product):
        # get front month instrument symbol from db
        with shared_session() as session:
            option = (
                session.query(upe_static.Option)
                .filter(upe_static.Option.product_symbol == product)
                .filter(
                    upe_static.Option.expiry >= datetime.now() - timedelta(hours=48)
                )
                .order_by(upe_static.Option.expiry)
                .first()
            )
            front_month_op = option.symbol
            front_month_fut = option.underlying_future_symbol

        # get underlying price from op eng output
        productInfo = conn.get(front_month_op + dev_key_redis_append)

        productInfo = orjson.loads(productInfo)
        basis = round(productInfo["underlying_prices"][0], 2)

        return basis, front_month_op, front_month_fut

    # pulltrades use hiddien inputs to trigger update on new trade
    @app.callback(
        Output("expiryTable", "data"),
        [Input("run-button", "n_clicks"), Input("product-dropdown", "value")],
        [
            State("strike-input", "placeholder"),
            State("strike-input", "value"),
            State("front-month-op", "value"),
            State("front-month-fut", "value"),
            # State("product-dropdown", "value"),
        ],
    )
    def update_expiry(click, product, strikeP, strike, front_month_op, front_month_fut):
        # clear table on product change
        context = callback_context.triggered[0]["prop_id"].split(".")[0]
        if context == "product-dropdown":
            return []
        if not strike:
            strike = strikeP

        # pull list of option positions
        if click:
            with shared_session() as session:
                positions = (
                    session.query(upe_dynamic.Position)
                    .filter(
                        upe_dynamic.Position.instrument_symbol.startswith(
                            front_month_op
                        )
                    )
                    .all()
                )
            # turn into a df
            df = pd.DataFrame([vars(position) for position in positions])
            # filter for non zero positions
            if not df.empty:
                df = df[df["net_quantity"] != 0]
            if not df.empty:
                # create strike and CoP column from instrument symbol
                df["info"] = df["instrument_symbol"].str.split(" ").str[-1]
                df[["a", "strike", "optionTypeId"]] = df["info"].str.split(
                    "-", expand=True
                )
                df["strike"] = df["strike"].astype(float)

                # handle partials
                dfPartial = df[df["strike"] == strike]
                dfPartial["action"] = "Partial"
                dfPartial["net_quantity"] = dfPartial["net_quantity"] * -1
                dfPartial["price"] = 0

                # seperate into calls and puts
                dfC = df[df["optionTypeId"].isin(["C", "c"])].copy()
                dfP = df[df["optionTypeId"].isin(["P", "p"])].copy()

                # seperate into ITM and OTM
                dfIC = dfC[dfC["strike"] < strike].copy()
                dfOC = dfC[dfC["strike"] > strike].copy()
                dfIP = dfP[dfP["strike"] > strike].copy()
                dfOP = dfP[dfP["strike"] < strike].copy()

                # Create Df for out only
                try:
                    out_list = [dfOC, dfOP]
                    out = pd.concat([df for df in out_list])
                    out["action"] = "Abandon"
                    out["net_quantity"] = out["net_quantity"] * -1
                    out["price"] = 0
                except Exception as e:
                    out = pd.DataFrame()

                # build expiry futures trade df
                futC = dfIC.copy()
                futC["instrument_symbol"] = front_month_fut
                futC["action"] = "Exercise Future"
                futC["price"] = futC["strike"]
                futC["strike"] = None
                futC["optionTypeId"] = None

                futP = dfIP.copy()
                futP["instrument_symbol"] = front_month_fut
                futP["net_quantity"] = futP["net_quantity"] * -1
                futP["action"] = "Exercise Future"
                futP["price"] = futP["strike"]
                futP["strike"] = None
                futP["optionTypeId"] = None

                # build conteracting options position df
                dfIP["net_quantity"] = dfIP["net_quantity"].values * -1
                dfIC["net_quantity"] = dfIC["net_quantity"].values * -1
                dfIP["action"] = "Exercised"
                dfIC["action"] = "Exercised"
                dfIP["price"] = 0
                dfIC["price"] = 0

                # pull it all together - done this way to avoid pandas warning concat w NAs
                df_list = [out, futC, futP, dfIP, dfIC, dfPartial]
                with warnings.catch_warnings(action="ignore", category=FutureWarning):
                    full_expiry_df = pd.concat([df for df in df_list if not df.empty])

                # add trading venue
                full_expiry_df["tradingVenue"] = "Exercise Process"

                # add trading time
                full_expiry_df["tradeDate"] = datetime.now().strftime(
                    "%d/%m/%Y %H:%M:%S"
                )
                if len(full_expiry_df) > 0:
                    try:
                        full_expiry_df["display_name"] = (
                            display_names.map_symbols_to_display_names(
                                full_expiry_df["instrument_symbol"].to_list()
                            )
                        )
                    except KeyError:
                        full_expiry_df["display_name"] = full_expiry_df[
                            "instrument_symbol"
                        ]
                else:
                    full_expiry_df["display_name"] = full_expiry_df["instrument_symbol"]
                full_expiry_df["display_name"] = full_expiry_df[
                    "display_name"
                ].str.upper()

                # specify columns to display
                full_expiry_df = full_expiry_df[
                    [
                        "instrument_symbol",
                        "display_name",
                        "portfolio_id",
                        "action",
                        "price",
                        "net_quantity",
                        "tradingVenue",
                        "tradeDate",
                    ]
                ]

                return full_expiry_df.to_dict("records")
            else:
                return []

    # send trade to system
    @app.callback(
        Output("tradeSent-expiry", "is_open"),
        Output("tradeSentFail-expiry", "is_open"),
        Output("tradeSentFail-expiry", "children"),
        [Input("expiry-button", "n_clicks")],
        [
            State("expiryTable", "selected_rows"),
            State("expiryTable", "data"),
            State("exchange-dropdown", "value"),
        ],
        prevent_initial_call=True,
    )
    def sendTrades(clicks, indices, rows, exchange):
        if clicks is None:
            return False, True

        timestamp = timeStamp()
        # pull username from site header
        user = request.headers.get("X-MS-CLIENT-PRINCIPAL-NAME")
        if not user:
            user = "TEST"

        if indices:
            # set variables shared by all trades
            # start of borrowed logic from carry page
            packaged_trades_to_send_legacy = []
            packaged_trades_to_send_new = []
            trader_id = 0
            upsert_pos_params = []
            trade_time_ns = time.time_ns()
            booking_dt = datetime.utcnow()

            with shared_engine.connect() as cnxn:
                stmt = sqlalchemy.text(
                    "SELECT trader_id FROM traders WHERE email = :user_email"
                )
                result = cnxn.execute(
                    stmt, {"user_email": user.lower()}
                ).scalar_one_or_none()
                if result is None:
                    trader_id = -101
                else:
                    trader_id = result
            ############################################################
            for i in indices:
                # check that this is not the total line.
                if rows[i]["instrument_symbol"] != "Total":
                    try:
                        portfolio_id = rows[i]["portfolio_id"]
                        if portfolio_id is None:
                            error_msg = (
                                f"No account selected for row {i+1} of trades table"
                            )
                            logger.error(error_msg)
                            return False, True, [error_msg]
                    except KeyError:
                        error_msg = f"No account selected for row {i+1} of trades table"
                        logger.error(error_msg)
                        return False, True, [error_msg]
                    # OPTIONS
                    if rows[i]["instrument_symbol"][-1] in ["C", "P", "c", "p"]:
                        # is option in format: "XEXT-EBM-EUR O 23-04-17 A-254-C"
                        product = " ".join(rows[i]["instrument_symbol"].split(" ")[:3])
                        product = (
                            product
                            + " "
                            + rows[i]["instrument_symbol"].split(" ")[-1][0]
                        )
                        instrument = rows[i]["instrument_symbol"]

                        prompt = datetime.strptime(
                            instrument.split(" ")[2], "%y-%m-%d"
                        ).strftime("%Y-%m-%d")
                        price = float(rows[i]["price"])
                        qty = int(rows[i]["net_quantity"])
                        counterparty = "EXPIRY"
                        if counterparty is None or counterparty == "":
                            error_msg = f"No counterparty selected for row {i+1} of trades table"
                            logger.error(error_msg)
                            return False, True, [error_msg]

                        # variables saved, now build class to send to DB twice
                        # trade_row = trade_table_data[trade_row_index]
                        processed_user = user.replace(" ", "").split("@")[0]
                        georgia_trade_id = (
                            f"expiry{exchange}.{processed_user}.{trade_time_ns}:{i}"
                        )

                        packaged_trades_to_send_legacy.append(
                            sql_utils.LegacyTradesTable(
                                dateTime=booking_dt,
                                instrument=(
                                    instrument.upper()
                                    if exchange != "xlme"
                                    else build_old_lme_symbol_from_new(instrument)
                                ),
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
                                instrument_symbol=instrument.lower(),
                                quantity=qty,
                                price=price,
                                portfolio_id=portfolio_id,
                                trader_id=trader_id,
                                notes=f"{exchange.upper()} EXPIRY",
                                venue_name="Georgia",
                                venue_trade_id=georgia_trade_id,
                                counterparty=counterparty,
                            )
                        )
                        upsert_pos_params.append(
                            {
                                "qty": qty,
                                "instrument": (
                                    instrument.upper()
                                    if exchange != "xlme"
                                    else build_old_lme_symbol_from_new(instrument)
                                ),
                                "tstamp": booking_dt,
                            }
                        )
                    # FUTURES
                    elif rows[i]["instrument_symbol"].split(" ")[1].upper() == "F":
                        # is futures in format: "XEXT-EBM-EUR F 23-05-10"
                        instrument = rows[i]["instrument_symbol"]
                        prompt = datetime.strptime(
                            instrument.split(" ")[-1], "%y-%m-%d"
                        ).strftime("%Y-%m-%d")
                        price = float(rows[i]["price"])
                        qty = int(rows[i]["net_quantity"])
                        counterparty = "EXPIRY FUTURE"

                        processed_user = user.replace(" ", "").split("@")[0]
                        georgia_trade_id = (
                            f"expiry{exchange}.{processed_user}.{trade_time_ns}:{i}"
                        )

                        packaged_trades_to_send_legacy.append(
                            sql_utils.LegacyTradesTable(
                                dateTime=booking_dt,
                                instrument=(
                                    instrument.upper()
                                    if exchange != "xlme"
                                    else build_old_lme_symbol_from_new(instrument)
                                ),
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
                                instrument_symbol=instrument.lower(),
                                quantity=qty,
                                price=price,
                                portfolio_id=portfolio_id,
                                trader_id=trader_id,
                                notes=f"{exchange.upper()} EXPIRY",
                                venue_name="Georgia",
                                venue_trade_id=georgia_trade_id,
                                counterparty=counterparty,
                            )
                        )
                        upsert_pos_params.append(
                            {
                                "qty": qty,
                                "instrument": (
                                    instrument.upper()
                                    if exchange != "xlme"
                                    else build_old_lme_symbol_from_new(instrument)
                                ),
                                "tstamp": booking_dt,
                            }
                        )
                        # END OF FUTURES

            # send trades to db
            try:
                with sqlalchemy.orm.Session(
                    shared_engine, expire_on_commit=False
                ) as session:
                    session.add_all(packaged_trades_to_send_new)
                    session.commit()
            except Exception:
                error_msg = (
                    "Exception while attempting to book trade in new standard table"
                )
                logger.exception(error_msg)
                return False, True, [error_msg]
            try:
                with sqlalchemy.orm.Session(legacyEngine) as session:
                    session.add_all(packaged_trades_to_send_legacy)
                    pos_upsert_statement = sqlalchemy.text(
                        "SELECT upsert_position(:qty, :instrument, :tstamp)"
                    )
                    _ = session.execute(pos_upsert_statement, params=upsert_pos_params)
                    session.commit()
            except Exception:
                error_msg = "Exception while attempting to book trade in legacy table"
                logger.exception(error_msg)
                for trade in packaged_trades_to_send_new:
                    trade.deleted = True
                # to clear up new trades table assuming they were booked correctly
                with sqlalchemy.orm.Session(shared_engine) as session:
                    session.add_all(packaged_trades_to_send_new)
                    session.commit()
                return False, True, [error_msg]

            return True, False, ["Trade failed to save ????"]

    # use callback to select all rows in expiry table
    @app.callback(
        [
            Output("expiryTable", "selected_rows"),
        ],
        [
            Input("select-all-expiry", "n_clicks"),
            Input("product-dropdown", "value"),
        ],
        [
            State("expiryTable", "derived_virtual_data"),
        ],
    )
    def select_all(n_clicks, product, selected_rows):
        # check callback context
        ctx = callback_context
        if not ctx.triggered:
            return [[]]
        else:
            trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]
            if trigger_id == "select-all-expiry":
                return [[i for i in range(len(selected_rows))]]
            else:
                return [[]]
