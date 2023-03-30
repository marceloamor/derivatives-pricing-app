from dash.dependencies import Input, Output, State
from dash import dcc, html
from dash import dcc
from dash import no_update
from datetime import datetime as dt
import dash_bootstrap_components as dbc
from numpy import True_
import pandas as pd
import datetime as dt
import os, pickle

from parts import topMenu, productOptions
from data_connections import PostGresEngine, conn

sdLocation = os.getenv("SD_LOCAITON", default="staticdata")


def BBfromName(product):
    product = product.upper()[0:3]

    if product == "LAD":
        return "LMAHDS03 COMDTY"
    elif product == "PBD":
        return "LMPBDS03 COMDTY"
    elif product == "LCU":
        return "LMCADS03 COMDTY"
    elif product == "LND":
        return "LMNIDS03 COMDTY"
    elif product == "LZH":
        return "LMZSDS03 COMDTY"
    else:
        return "UNKNOWN"


def BBFullName(product):
    product = product.upper()[0:3]

    if product == "LAD":
        return "LONDON (LME) HG PRIMARY ALUMINIUM (USD)"
    elif product == "PBD":
        return "LONDON(LME) LEAD (US DOLLAR) OPTION"
    elif product == "LCU":
        return "LONDON (LME) COPPER (US DOLLAR) OPTION"
    elif product == "LND":
        return "LONDON (LME) NICKEL (US DOLLAR) OPTION"
    elif product == "LZH":
        return "LME HIGH GRADE ZINC OPTIONS"
    else:
        return "UNKNOWN"


def findDay(date):
    weekday = dt.datetime.strptime(date, "%Y-%m-%d").weekday()
    day = dt.datetime.strptime(date, "%Y-%m-%d").day
    return weekday, day


form = dbc.Row(
    [
        dbc.Col(
            [
                dbc.Form(
                    [
                        dbc.FormGroup(
                            [
                                dbc.Label("Product", className="mr-2"),
                                dbc.Input(
                                    type="text",
                                    id="product",
                                    placeholder="Georgia product",
                                ),
                                dbc.FormFeedback("Valid Name", valid=True),
                                dbc.FormFeedback(
                                    "This is not a valid product name. Product names are F2 code followed by 'O' and month code",
                                    valid=False,
                                    id="product_vailidator",
                                ),
                            ],
                            className="mr-3",
                        ),
                        dbc.FormGroup(
                            [
                                dbc.Label("Name", className="mr-2"),
                                dcc.Dropdown(id="name", options=productOptions()),
                            ],
                            className="mr-3",
                        ),
                        dbc.FormGroup(
                            [
                                dbc.Label("Max Strike", className="mr-2"),
                                dbc.Input(type="numeric", id="strike_max"),
                                dbc.Label("Min Strike", className="mr-2"),
                                dbc.Input(type="numeric", id="strike_min"),
                                dbc.Label("Strike Interval", className="mr-2"),
                                dbc.Input(type="numeric", id="strike_step"),
                            ],
                            inline=True,
                            className="mr-3",
                        ),
                        dbc.FormGroup(
                            [
                                dbc.Label("Expiry", className="mr-2"),
                                dbc.Input(id="expiry", type="date"),
                                dbc.FormFeedback(
                                    "That looks like a a first Wednesday", valid=True
                                ),
                                dbc.FormFeedback(
                                    "This does not look like a first Wednesday",
                                    valid=False,
                                ),
                                dbc.Label("third_wed", className="mr-2"),
                                dbc.Input(id="third_wed", type="date"),
                                dbc.FormFeedback(
                                    "That looks like a a third Wednesday", valid=True
                                ),
                                dbc.FormFeedback(
                                    "This does not look like a third Wednesday",
                                    valid=False,
                                ),
                                dbc.Label("Multiplier", className="mr-2"),
                                dbc.Input(type="numeric", id="multiplier"),
                                dbc.Label("Currency", className="mr-2"),
                                dbc.Input(type="numeric", id="currency"),
                                dbc.Label("Market Open", className="mr-2"),
                                dbc.Input(type="numeric", id="market_open"),
                                dbc.Label("Market Close", className="mr-2"),
                                dbc.Input(type="numeric", id="market_close"),
                                dbc.Label("Portfolio", className="mr-2"),
                                dbc.Input(type="text", id="portfolio"),
                            ],
                            className="mr-3",
                        ),
                    ],
                    # inline=True,
                )
            ],
            width=6,
        ),
        dbc.Col(
            [
                dbc.Label("Underlying", className="mr-2"),
                dbc.Input(id="underlying", type="text"),
                dbc.Label("3m_bb_code", className="mr-2"),
                dbc.Input(id="3m_bb_code", type="text"),
                dbc.Label("Full Name", className="mr-2"),
                dbc.Input(id="full_name", type="text"),
                dbc.Label("F2 Name", className="mr-2"),
                dbc.Input(type="text", id="f2_name"),
                dbc.Label("LME Short Name", className="mr-2"),
                dbc.Input(type="text", id="lme_short_name"),
                dbc.Label("Option Type", className="mr-2"),
                dbc.Input(type="text", id="option_type", value="vanilla"),
                dbc.Label("volatility underlying", className="mr-2"),
                dbc.Input(type="text", id="vol_underlying"),
                dbc.Label("ffd", className="mr-2"),
                dbc.Input(type="text", id="ffd"),
                dbc.Label("Volatility Model Type", className="mr-2"),
                dbc.Input(type="text", id="vol_model_type", value="paramertised"),
                dbc.Label("Day Count Type", className="mr-2"),
                dbc.Input(type="text", id="day_count", value="actual/365"),
                dbc.Label("Exchange", className="mr-2"),
                dbc.Input(type="text", id="exchange", value="LME"),
            ]
        ),
    ]
)

layout = html.Div(
    [
        topMenu("Static Data"),
        form,
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.Button("Add", id="button"),
                        dbc.Label("Input fields", id="update"),
                    ],
                    width=6,
                ),
                dbc.Col(
                    [
                        dbc.Button(
                            "Delete",
                            id="delButton",
                            color="danger",
                            active=False,
                            className="mr-1",
                        ),
                        dbc.Label("Input fields", id="delUpdate"),
                    ],
                    width=6,
                ),
            ]
        ),
    ]
)


def initialise_callbacks(app):
    # pull outputs from staticdata for set product
    @app.callback(
        [
            Output("button", "children"),
            Output("name", "value"),
            Output("strike_max", "value"),
            Output("strike_min", "value"),
            Output("strike_step", "value"),
            Output("multiplier", "value"),
            Output("currency", "value"),
            Output("market_open", "value"),
            Output("market_close", "value"),
            Output("portfolio", "value"),
            Output("expiry", "value"),
            Output("third_wed", "value"),
            Output("vol_underlying", "value"),
            Output("delButton", "active"),
            Output("f2_name", "value"),
            Output("3m_bb_code", "value"),
        ],
        [Input("product", "value")],
    )
    def changeValues(product):
        if product:
            product = product.upper()
            # get staticdata
            static = conn.get(sdLocation)
            df = pd.read_json(static)

            if product in df["product"].values:
                df = df[df["product"] == product]
                # convert dates to datetime
                expriy = df["expiry"].values[0]
                third_wed = df["third_wed"].values[0]

                expiry = pd.to_datetime(str(expriy), format="%d/%m/%Y")
                third_wed = pd.to_datetime(str(third_wed), format="%d/%m/%Y")

                expiry = dt.datetime.strftime(expiry, "%Y-%m-%d")
                third_wed = dt.datetime.strftime(third_wed, "%Y-%m-%d")

                return (
                    "Update",
                    df.name.values[0],
                    df.strike_max.values[0],
                    df.strike_min.values[0],
                    df.strike_step.values[0],
                    df.multiplier.values[0],
                    df.currency.values[0],
                    df.market_open.values[0],
                    df.market_close.values[0],
                    df.portfolio.values[0],
                    expiry,
                    third_wed,
                    product,
                    True,
                    df.f2_name.values[0],
                    df["3m_bb_code"].values[0],
                )
            else:
                return (
                    "Add",
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    False,
                    no_update,
                    no_update,
                )
        else:
            return (
                "Error",
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
                False,
                no_update,
                no_update,
            )

    @app.callback(
        [
            Output("underlying", "value"),
            Output("full_name", "value"),
            Output("lme_short_name", "value"),
        ],
        [Input("product", "value")],
    )
    def changeValues(product):
        if product:
            product = product.upper()
            underlying = product[0:3] + product[4:6]
            underlyingBB = BBfromName(product)
            fullName = BBFullName(product)
            lmeName = underlyingBB[2:4]

            return underlying, fullName, lmeName
        else:
            return no_update, no_update, no_update

    # check dates look right
    @app.callback(
        [
            Output("expiry", "valid"),
            Output("expiry", "invalid"),
            Output("third_wed", "valid"),
            Output("third_wed", "invalid"),
        ],
        [Input("expiry", "value"), Input("third_wed", "value")],
    )
    def check_validity(expiry, thirdWed):
        if expiry:
            date_1 = dt.datetime.strptime(expiry, "%Y-%m-%d")
            third = date_1 + dt.timedelta(days=14)
            weekday, day = findDay(expiry)

            if weekday == 2 and day <= 7:
                expiry = [True, False]
            else:
                expiry = [False, True]

            if thirdWed:
                thirdWed = dt.datetime.strptime(thirdWed, "%Y-%m-%d")
                if thirdWed == third:
                    thirdWed = [True, False]
                else:
                    thirdWed = [False, True]
            else:
                thirdWed = [no_update, no_update]

            return expiry + thirdWed

        else:
            return [no_update, no_update, no_update, no_update]

    # check product name looks right
    @app.callback(
        [Output("product", "valid"), Output("product", "invalid")],
        [Input("product", "value")],
    )
    def check_validity(text):
        if text:
            text = text.upper()
            name = text[:3]
            if len(text) == 6 and text[3] == "O":
                return True, False
            else:
                return False, True
        else:
            return False, True

    inputs = [
        "product",
        "name",
        "underlying",
        "strike_max",
        "strike_min",
        "strike_step",
        "expiry",
        "multiplier",
        "currency",
        "market_open",
        "market_close",
        "portfolio",
        "expiry",
        "full_name",
        "third_wed",
        "underlying",
        "3m_bb_code",
        "f2_name",
        "option_type",
        "vol_underlying",
        "ffd",
        "day_count",
        "day_count",
        "lme_short_name",
        "vol_model_type",
        "exchange",
    ]

    def sendSD(static, product):
        # send to redis
        # static_json = static.to_json()
        # conn.set(sdLocation, static_json)

        # send to postgres
        static.to_sql("staticdata", con=PostGresEngine(), if_exists="replace")

        data = pd.read_sql("staticdata", PostGresEngine())
        data.columns = data.columns.str.lower()
        conn.set(sdLocation, data.to_json())

        # tell options engine about new prodcut or change
        pic_data = pickle.dumps([product, "staticdata"])
        conn.publish("compute", pic_data)

    # action button press update
    @app.callback(
        Output("update", "children"),
        Input("button", "n_clicks"),
        [State("button", "children")]
        + [State("{}".format(i), "value") for i in inputs],
    )
    def sendUpdate(click, value, *args):
        if click:
            # turn inputs into dataframe
            staticadata_inputs = dict(zip(inputs, args))
            df = pd.DataFrame(staticadata_inputs, index=[0])

            df["expiry"] = pd.to_datetime(df["expiry"], format="%Y-%m-%d")
            df["third_wed"] = pd.to_datetime(df["third_wed"], format="%Y-%m-%d")

            df["expiry"] = df["expiry"].dt.strftime("%d/%m/%Y")
            df["third_wed"] = df["third_wed"].dt.strftime("%d/%m/%Y")

            df["product"] = df["product"].str.upper()

            # get staticdata
            static = conn.get(sdLocation)
            static = pd.read_json(static)
            if value == "Add":
                product = df["product"].values[0]
                static = static.append(df, ignore_index=True)
                sendSD(static, product)
                return "Added {}".format(product)

            elif value == "Update":
                product = df["product"].values[0]
                # remove current data
                idx = static.index[static["product"] == product.upper()]
                static.drop(idx[0], inplace=True)
                # add new data to the end
                static = static.append(df, ignore_index=True)
                sendSD(static, product)
                return "Updated {}".format(product)
            else:
                return "Error check inputs"
        else:
            return "Error check inputs"

    # action button press delete
    @app.callback(
        Output("delUpdate", "children"),
        [Input("delButton", "n_clicks")],
        [State("product", "value"), State("delButton", "active")],
    )
    def sendUpdate(click, product, state):
        if click and state:
            # get staticdata
            static = conn.get(sdLocation)
            static = pd.read_json(static)
            static.set_index("product", inplace=True)
            static.drop([product.upper()], inplace=True)
            static.reset_index(inplace=True)

            sendSD(static, product)
            return "Deleted {}".format(product)
        else:
            """"""
