from dash.dependencies import Input, Output, State
from dash import dcc, html
from dash import dcc
import dash_bootstrap_components as dbc
from datetime import timedelta
from datetime import datetime
import json
from dash import dash_table as dtable
import pandas as pd
import datetime as dt
import time, math, os, io
from dash.exceptions import PreventUpdate
from flask import request

from TradeClass import TradeClass, Option
from sql import pulltrades, sendTrade, storeTradeSend, pullCodeNames, updateRedisCurve
from parts import (
    loadRedisData,
    buildTableData,
    buildTradesTableData,
    retriveParams,
    retriveTickData,
    loadStaticData,
    get_theo,
    updateRedisDelta,
    updateRedisPos,
    updateRedisTrade,
    updatePos,
    sendFIXML,
    tradeID,
    loadVolaData,
    buildSurfaceParams,
    codeToName,
    codeToMonth,
    monthSymbol,
    loadStaticData,
)
from app import app, topMenu


def fetechstrikes(product):
    if product != None:
        strikes = []
        data = loadRedisData(product.lower())
        data = json.loads(data)
        for strike in data["strikes"]:
            strikes.append({"label": strike, "value": strike})
        return strikes
    else:
        return {"label": 0, "value": 0}


def timeStamp():
    now = dt.datetime.now()
    now.strftime("%Y-%m-%d %H:%M:%S")
    return now


def convertTimestampToSQLDateTime(value):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(value))


def convertToSQLDate(date):
    value = date.strftime(f)
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(value))


def onLoadProduct():
    staticData = loadStaticData()
    staticData = pd.read_json(staticData)
    staticData.sort_values("product")
    products = []
    for product in staticData["product"]:
        products.append({"label": product, "value": product})
    return products, products[0]["value"]


def onLoadProductProducts():
    staticData = loadStaticData()
    staticData = pd.read_json(staticData)
    products = []
    staticData["product"] = [x[:3] for x in staticData["product"]]
    productNames = staticData["product"].unique()
    staticData.sort_values("product")
    for product in productNames:
        products.append({"label": product, "value": product})
    return products, products[0]["value"]


def onLoadProductMonths(product):
    # load staticdata
    staticData = loadStaticData()
    staticData = pd.read_json(staticData)
    # convert to shortname
    staticData = staticData.loc[staticData["F2_name"] == product.upper()]
    # sort data
    staticData["expiry"] = pd.to_datetime(staticData["expiry"], dayfirst=True)
    staticData = staticData.sort_values(by=["expiry"])
    # create month code from product code
    staticData["product"] = [x[4:] for x in staticData["product"]]
    productNames = staticData["product"].unique()

    products = []
    for product in productNames:
        products.append({"label": product, "value": product})
    # add 3M to months list
    products.append({"label": "3M", "value": "3M"})
    return products, products[0]["value"]


def buildProductName(product, strike, Cop):
    if strike == None and Cop == None:
        return product
    else:
        return product + " " + str(strike) + " " + Cop


def buildCounterparties():
    # load couterparties from DB
    df = pullCodeNames()
    nestedOptions = df["Code name"].values
    options = [{"label": opt, "value": opt} for opt in nestedOptions]
    options.append({"label": "ERROR", "value": "ERROR"})
    options.append({"label": "BACKBOOK", "value": "09600"})
    options.append({"label": "93492", "value": "93492"})

    return options


def excelNameConversion(name):
    if name == "cu":
        return "LCUO"
    elif name == "zn":
        return "LZHO"
    elif name == "ni":
        return "LNDO"
    elif name == "pb":
        return "PBDO"
    elif name == "al":
        return "LADO"


stratOptions = [
    {"label": "Outright", "value": "outright"},
    {"label": "Spread", "value": "spread"},
    {"label": "Straddle/Strangle", "value": "straddle"},
    {"label": "Fly", "value": "fly"},
    {"label": "Condor", "value": "condor"},
    {"label": "Ladder", "value": "ladder"},
    {"label": "1*2", "value": "ratio"},
]

stratConverstion = {
    "outright": [1, 0, 0, 0],
    "spread": [1, -1, 0, 0],
    "straddle": [1, 1, 0, 0],
    "fly": [1, -2, 1, 0],
    "condor": [1, -1, -1, 1],
    "ladder": [1, -1, -1, 0],
    "ratio": [1, -2, 0, 0],
}


# trades table layout
calculator = dbc.Col(
    [
        # top row lables
        dbc.Row(
            [
                dbc.Col(["Basis"], width=4),
                dbc.Col(["Forward"], width=4),
                dbc.Col(["Interest"], width=4),
            ]
        ),
        # top row values
        dbc.Row(
            [
                dbc.Col([dcc.Input(id="calculatorBasis", type="text")], width=4),
                dbc.Col([dcc.Input(id="calculatorForward", type="text")], width=4),
                dbc.Col([dcc.Input(id="interestRate", type="text")], width=4),
            ]
        ),
        # second row labels
        dbc.Row(
            [
                dbc.Col([html.Div("Spread")], width=4),
                dbc.Col([html.Div("Strategy")], width=4),
                dbc.Col([html.Div("Days Convention")], width=4),
            ]
        ),
        # second row values
        dbc.Row(
            [
                dbc.Col(
                    [html.Div([dcc.Input(type="text", id="calculatorSpread1")])],
                    width=4,
                ),
                dbc.Col(
                    [
                        html.Div(
                            [
                                dcc.Dropdown(
                                    id="strategy",
                                    value="outright",
                                    options=stratOptions,
                                )
                            ]
                        )
                    ],
                    width=4,
                ),
                dbc.Col(
                    [
                        html.Div(
                            [
                                dcc.Dropdown(
                                    "dayConvention",
                                    value="",
                                    options=[
                                        {"label": "Bis/Bis", "value": "b/b"},
                                        {"label": "Calendar/365", "value": ""},
                                    ],
                                )
                            ]
                        )
                    ],
                    width=4,
                ),
            ]
        ),
        # model settings
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.Div(
                            [
                                dcc.RadioItems(
                                    id="calculatorVol_price",
                                    options=[
                                        {"label": "Vol", "value": "vol"},
                                        {"label": "Price", "value": "price"},
                                    ],
                                    value="vol",
                                )
                            ]
                        )
                    ],
                    width=3,
                ),
                dbc.Col(
                    [
                        html.Div(
                            [
                                dcc.RadioItems(
                                    id="nowOpen",
                                    options=[
                                        {"label": "Now", "value": "now"},
                                        {"label": "Open", "value": "open"},
                                    ],
                                    value="open",
                                )
                            ]
                        )
                    ],
                    width=3,
                ),
                dbc.Col([html.Div("Counterparty:")], width=3),
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="counterparty", value="", options=buildCounterparties()
                        )
                    ],
                    width=3,
                ),
            ]
        ),
        # leg inputs and outputs
        # leg inputs
        # labels
        dbc.Row(
            [
                dbc.Col(["Strike: "], width=2),
                dbc.Col([dcc.Input(id="oneStrike")], width=2),
                dbc.Col([dcc.Input(id="twoStrike")], width=2),
                dbc.Col([dcc.Input(id="threeStrike")], width=2),
                dbc.Col([dcc.Input(id="fourStrike")], width=2),
                dbc.Col([dcc.Input(id="qty", type="number", value=10, min=0)], width=2),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Price/Vol: "], width=2),
                dbc.Col([dcc.Input(id="oneVol_price")], width=2),
                dbc.Col([dcc.Input(id="twoVol_price")], width=2),
                dbc.Col([dcc.Input(id="threeVol_price")], width=2),
                dbc.Col([dcc.Input(id="fourVol_price")], width=2),
                dbc.Col(
                    [dbc.Button("Buy", id="buy", n_clicks_timestamp="0", active=True)],
                    width=1,
                ),
                dbc.Col(
                    [
                        dbc.Button(
                            "Sell", id="sell", n_clicks_timestamp="0", active=True
                        )
                    ],
                    width=1,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["C/P: "], width=2),
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="oneCoP",
                            value="c",
                            options=[
                                {"label": "C", "value": "c"},
                                {"label": "P", "value": "p"},
                                {"label": "F", "value": "f"},
                            ],
                            style={"height": "50%", "verticalAlign": "middle"},
                        )
                    ],
                    width=2,
                ),
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="twoCoP",
                            value="c",
                            options=[
                                {"label": "C", "value": "c"},
                                {"label": "P", "value": "p"},
                                {"label": "F", "value": "f"},
                            ],
                            style={"height": "50%", "verticalAlign": "middle"},
                        )
                    ],
                    width=2,
                ),
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="threeCoP",
                            value="c",
                            options=[
                                {"label": "C", "value": "c"},
                                {"label": "P", "value": "p"},
                                {"label": "F", "value": "f"},
                            ],
                            style={"height": "50%", "verticalAlign": "middle"},
                        )
                    ],
                    width=2,
                ),
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="fourCoP",
                            value="c",
                            options=[
                                {"label": "C", "value": "c"},
                                {"label": "P", "value": "p"},
                                {"label": "F", "value": "f"},
                            ],
                            style={"height": "50%", "verticalAlign": "middle"},
                        )
                    ],
                    width=2,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Theo: "], width=2),
                dbc.Col([html.Div(id="oneTheo")], width=2),
                dbc.Col([html.Div(id="twoTheo")], width=2),
                dbc.Col([html.Div(id="threeTheo")], width=2),
                dbc.Col([html.Div(id="fourTheo")], width=2),
                dbc.Col([html.Div(id="stratTheo")], width=2),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["IV: "], width=2),
                dbc.Col([html.Div(id="oneIV")], width=2),
                dbc.Col([html.Div(id="twoIV")], width=2),
                dbc.Col([html.Div(id="threeIV")], width=2),
                dbc.Col([html.Div(id="fourIV")], width=2),
                dbc.Col([html.Div(id="stratIV")], width=2),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Delta: "], width=2),
                dbc.Col([html.Div(id="oneDelta")], width=2),
                dbc.Col([html.Div(id="twoDelta")], width=2),
                dbc.Col([html.Div(id="threeDelta")], width=2),
                dbc.Col([html.Div(id="fourDelta")], width=2),
                dbc.Col([html.Div(id="stratDelta")], width=2),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(html.Div("Full Delta: ", id="fullDeltaLabel"), width=2),
                dbc.Col([html.Div(id="oneFullDelta")], width=2),
                dbc.Col([html.Div(id="twoFullDelta")], width=2),
                dbc.Col([html.Div(id="threeFullDelta")], width=2),
                dbc.Col([html.Div(id="fourFullDelta")], width=2),
                dbc.Col([html.Div(id="stratFullDelta")], width=2),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Gamma: "], width=2),
                dbc.Col([html.Div(id="oneGamma")], width=2),
                dbc.Col([html.Div(id="twoGamma")], width=2),
                dbc.Col([html.Div(id="threeGamma")], width=2),
                dbc.Col([html.Div(id="fourGamma")], width=2),
                dbc.Col([html.Div(id="stratGamma")], width=2),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Vega: "], width=2),
                dbc.Col([html.Div(id="oneVega")], width=2),
                dbc.Col([html.Div(id="twoVega")], width=2),
                dbc.Col([html.Div(id="threeVega")], width=2),
                dbc.Col([html.Div(id="fourVega")], width=2),
                dbc.Col([html.Div(id="stratVega")], width=2),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(["Theta: "], width=2),
                dbc.Col([html.Div(id="oneTheta")], width=2),
                dbc.Col([html.Div(id="twoTheta")], width=2),
                dbc.Col([html.Div(id="threeTheta")], width=2),
                dbc.Col([html.Div(id="fourTheta")], width=2),
                dbc.Col([html.Div(id="stratTheta")], width=2),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(html.Div("Vol Theta: ", id="volThetaLabel"), width=2),
                dbc.Col([html.Div(id="onevolTheta")], width=2),
                dbc.Col([html.Div(id="twovolTheta")], width=2),
                dbc.Col([html.Div(id="threevolTheta")], width=2),
                dbc.Col([html.Div(id="fourvolTheta")], width=2),
                dbc.Col([html.Div(id="stratvolTheta")], width=2),
            ]
        ),
    ],
    width=9,
)

hidden = (
    # hidden to store greeks from the 4 legs
    html.Div(id="oneCalculatorCalculatorData", style={"display": "none"}),
    html.Div(id="twoCalculatorCalculatorData", style={"display": "none"}),
    html.Div(id="threeCalculatorCalculatorData", style={"display": "none"}),
    html.Div(id="fourCalculatorCalculatorData", style={"display": "none"}),
    dcc.Store(id="tradesStore"),
    dcc.Store(id="paramsStore"),
    dcc.Store(id="productInfo"),
    html.Div(id="trades_div", style={"display": "none"}),
    html.Div(id="trade_div", style={"display": "none"}),
    html.Div(id="trade_div2", style={"display": "none"}),
    html.Div(id="productData", style={"display": "none"}),
)

actions = dbc.Row(
    [
        dbc.Col([html.Button("Delete", id="delete", n_clicks_timestamp="0")], width=3),
        dbc.Col([html.Button("Trade", id="trade", n_clicks_timestamp="0")], width=3),
        dbc.Col(
            [html.Button("Client Recap", id="clientRecap", n_clicks_timestamp="0")],
            width=3,
        ),
        dbc.Col([html.Button("Report", id="report", n_clicks_timestamp="0")], width=3),
    ]
)

columns = [
    {"id": "Instrument", "name": "Instrument"},
    {
        "id": "Qty",
        "name": "Qty",
    },
    {
        "id": "Theo",
        "name": "Theo",
    },
    {"id": "Prompt", "name": "Prompt"},
    {"id": "Forward", "name": "Forward"},
    {"id": "IV", "name": "IV"},
    {"id": "Delta", "name": "Delta"},
    {"id": "Gamma", "name": "Gamma"},
    {"id": "Vega", "name": "Vega"},
    {"id": "Theta", "name": "Theta"},
    {"id": "Counterparty", "name": "Counterparty", "type": "dropdown"},
]

tables = dbc.Col(
    dtable.DataTable(
        id="tradesTable",
        data=[{}],
        columns=columns,
        row_selectable=True,
        editable=True,
        #              column_static_dropdown=[
        # {
        #    'id': 'Counterparty',
        #    'dropdown': countrparties
        # }]
    )
)

toolTips = html.Div(
    [
        dbc.Tooltip("Theta in terms of vol change equivalent", target="volThetaLabel"),
        dbc.Tooltip(
            "Full price change of the option with underlying"
            "including both BS dleta and the option moving on the Vol surface",
            target="fullDeltaLabel",
        ),
    ]
)

sideMenu = dbc.Col(
    [
        dbc.Row(
            dbc.Col(
                [
                    dcc.Dropdown(
                        id="productCalc-selector",
                        value=onLoadProductProducts()[1],
                        options=onLoadProductProducts()[0],
                    )
                ],
                width=12,
            )
        ),
        dbc.Row(dbc.Col([dcc.Dropdown(id="monthCalc-selector")], width=12)),
        # dbc.Row(
        #   dbc.Col([ 'Product:'], width = 12)
        #    ),
        dbc.Row(dbc.Col(["Expiry:"], width=12)),
        dbc.Row(dbc.Col([html.Div("expiry", id="calculatorExpiry")], width=12)),
        dbc.Row(dbc.Col(["Third Wednesday:"], width=12)),
        dbc.Row(dbc.Col([html.Div("3wed", id="3wed")])),
        dbc.Row(
            dbc.Col(
                [
                    dcc.Input(
                        placeholder="Enter a value...",
                        type="text",
                        id="excelInput",
                        value="",
                    )
                ],
                width=12,
            )
        ),
        dbc.Row(
            dbc.Col(
                [html.Button("Load Excel", id="loadExcel", n_clicks_timestamp="0")],
                width=12,
            )
        ),
    ],
    width=3,
)

output = dcc.Markdown(id="reponseOutput")

alert = html.Div(
    [
        dbc.Alert(
            "Trade sent",
            id="tradeSent",
            dismissable=True,
            is_open=False,
            duration=3000,
            color="success",
        ),
        dbc.Alert(
            "Trade Routed",
            id="tradeRouted",
            dismissable=True,
            is_open=False,
            duration=3000,
            color="success",
        ),
        dbc.Alert(
            "Trade Routing Failed",
            id="tradeRouteFail",
            dismissable=True,
            is_open=False,
            duration=3000,
            color="danger",
        ),
    ]
)

layout = html.Div(
    [
        topMenu("Calculator"),
        dbc.Row(alert),
        dbc.Row(hidden),
        dbc.Row([sideMenu, calculator]),
        dbc.Row(tables),
        actions,
        dbc.Row(output),
        toolTips,
    ]
)


@app.callback(
    Output("productData", "children"), [Input("productCalc-selector", "value")]
)
def updateSpread1(product):
    params = retriveParams(product.lower())
    if params:
        spread = params["spread"]
        return spread


@app.callback(
    Output("calculatorSpread1", "placeholder"),
    [Input("productCalc-selector", "value"), Input("monthCalc-selector", "value")],
)
def updateSpread1(product, month):
    if month == "3M":
        return 0
    else:
        product = product + "O" + month
        params = retriveParams(product.lower())
        if params:
            spread = params["spread"]
            return spread


# load vola params for us fulldelta calc later
@app.callback(
    Output("paramsStore", "data"),
    [
        Input("productCalc-selector", "value"),
        Input("monthCalc-selector", "value"),
        Input("calculatorForward", "value"),
        Input("calculatorForward", "placeholder"),
        Input("calculatorExpiry", "children"),
    ],
)
def updateSpread1(product, month, spot, spotP, expiry):
    print("Load vola data")
    if month == "3M":
        return 0
    else:
        # build product from month and product
        product = product + "O" + month
        params = loadVolaData(product.lower())
        if params:
            print(params)
            return params


# update months options on product change


@app.callback(
    Output("monthCalc-selector", "options"), [Input("productCalc-selector", "value")]
)
def updateOptions(product):
    if product:
        return onLoadProductMonths(product)[0]


# update months value on product change


@app.callback(
    Output("monthCalc-selector", "value"), [Input("monthCalc-selector", "options")]
)
def updatevalue(options):
    if options:
        return options[0]["value"]


# update expiry date


@app.callback(
    Output(component_id="calculatorExpiry", component_property="children"),
    [Input("productInfo", "data")],
)
def findExpiryDate(params):
    if params:
        return params["m_expiry"]


# update 3wed


@app.callback(
    Output(component_id="3wed", component_property="children"),
    [Input("productInfo", "data")],
)
def find3Wed(params):
    if params:
        return params["third_wed"]


@app.callback(Output("tradesTable", "data"), [Input("tradesStore", "data")])
def loadTradeTable(data):
    if data != None:
        trades = buildTradesTableData(data)
        return trades.to_dict("records")

    else:
        return [{}]


@app.callback(
    [Output("tradesStore", "data"), Output("tradesTable", "selected_rows")],
    [
        Input("buy", "n_clicks_timestamp"),
        Input("sell", "n_clicks_timestamp"),
        Input("delete", "n_clicks_timestamp"),
        Input("loadExcel", "n_clicks_timestamp"),
    ],
    # standard trade inputs
    [
        State("tradesTable", "selected_rows"),
        State("tradesTable", "data"),
        State("calculatorVol_price", "value"),
        State("tradesStore", "data"),
        State("counterparty", "value"),
        # State('trades_div' , 'children'),
        State("productCalc-selector", "value"),
        State("monthCalc-selector", "value"),
        State("qty", "value"),
        State("strategy", "value"),
        # trade value inputs
        # one vlaues
        State("oneStrike", "value"),
        State("oneStrike", "placeholder"),
        State("oneCoP", "value"),
        State("oneTheo", "children"),
        State("oneIV", "children"),
        State("oneDelta", "children"),
        State("oneGamma", "children"),
        State("oneVega", "children"),
        State("oneTheta", "children"),
        # two values
        State("twoStrike", "value"),
        State("twoStrike", "placeholder"),
        State("twoCoP", "value"),
        State("twoTheo", "children"),
        State("twoIV", "children"),
        State("twoDelta", "children"),
        State("twoGamma", "children"),
        State("twoVega", "children"),
        State("twoTheta", "children"),
        # three values
        State("threeStrike", "value"),
        State("threeStrike", "placeholder"),
        State("threeCoP", "value"),
        State("threeTheo", "children"),
        State("threeIV", "children"),
        State("threeDelta", "children"),
        State("threeGamma", "children"),
        State("threeVega", "children"),
        State("threeTheta", "children"),
        # four values
        State("fourStrike", "value"),
        State("fourStrike", "placeholder"),
        State("fourCoP", "value"),
        State("fourTheo", "children"),
        State("fourIV", "children"),
        State("fourDelta", "children"),
        State("fourGamma", "children"),
        State("fourVega", "children"),
        State("fourTheta", "children"),
        State("calculatorExpiry", "children"),
        State("3wed", "children"),
        State("calculatorForward", "value"),
        State("calculatorForward", "placeholder"),
        State("excelInput", "value"),
    ],
)
def stratTrade(
    buy,
    sell,
    delete,
    loadExcel,
    clickdata,
    rows,
    pricevola,
    data,
    counterparty,
    product,
    month,
    qty,
    strat,
    onestrike,
    ponestrike,
    onecop,
    onetheo,
    oneiv,
    onedelta,
    onegamma,
    onevega,
    onetheta,
    twostrike,
    ptwostrike,
    twocop,
    twotheo,
    twoiv,
    twodelta,
    twogamma,
    twovega,
    twotheta,
    threestrike,
    pthreestrike,
    threecop,
    threetheo,
    threeiv,
    threedelta,
    threegamma,
    threevega,
    threetheta,
    fourstrike,
    pfourstrike,
    fourcop,
    fourtheo,
    fouriv,
    fourdelta,
    fourgamma,
    fourvega,
    fourtheta,
    expiry,
    wed,
    forward,
    pforward,
    excelInput,
):

    # prevent error from empty inputs on page load
    if (int(buy) + int(sell) + int(delete) + int(loadExcel)) == 0:
        raise PreventUpdate

    # replace rows if nonetype
    if not clickdata:
        clickdata = []

    # reset buy sell signal
    bs = 0
    # convert qty to float to save multiple tims later
    qty = float(qty)

    # build product from month and product dropdown
    if product and month:
        product = product + "O" + month

        if data:
            trades = data
        else:
            trades = {}

        # if load excel pressed last then load excel data
        if (
            int(loadExcel) > int(buy)
            and int(loadExcel) > int(sell)
            and int(loadExcel) > int(delete)
        ):
            input = excelInput.splitlines()
            input = input[0].split(" ")
            df = pd.DataFrame([x.split("\t") for x in input])

            # filter for coulmns we want and rename
            df = df[
                [
                    0,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10,
                    11,
                    12,
                    13,
                    14,
                    15,
                    16,
                    17,
                    18,
                    19,
                    21,
                    22,
                    23,
                    24,
                ]
            ]
            df.columns = [
                "qty",
                "cop",
                "metal",
                "strike",
                "expiry",
                "tm",
                "carry",
                "undBid",
                "undAsk",
                "adjVol",
                "volMid",
                "volBid",
                "volAsk",
                "priceBid",
                "priceAsk",
                "deltaBid",
                "deltaAsk",
                "gammaBid",
                "gammaAsk",
                "vegaBid",
                "vegaAsk",
                "thetaBid",
                "thetaAsk",
            ]

            # load staticdata for fidning prompt
            staticData = loadStaticData()
            staticData = pd.read_json(staticData)

            # load from excel into trade file
            for index, row in df.iterrows():
                # build product string
                product = excelNameConversion(row["metal"]) + monthSymbol(
                    dt.datetime.strptime(row["expiry"], "%b-%y")
                )

                # retrive prompt from staticdata
                prompt = staticData[staticData["product"] == product]["expiry"].values[
                    0
                ]
                prompt = dt.datetime.strptime(prompt, "%d/%m/%Y").strftime("%Y-%m-%d")

                name = product + " " + row["strike"] + " " + row["cop"].upper()

                # build trades list to send to table
                if float(row["qty"]) > 0:
                    price = float(row["priceBid"])
                    vol = float(row["volBid"])
                    delta = float(row["deltaBid"])
                    gamma = float(row["gammaBid"])
                    vega = float(row["vegaBid"])
                    theta = float(row["thetaBid"])
                elif float(row["qty"]) < 0:
                    price = float(row["priceAsk"])
                    vol = float(row["volAsk"])
                    delta = float(row["deltaAsk"])
                    gamma = float(row["gammaAsk"])
                    vega = float(row["vegaAsk"])
                    theta = float(row["thetaAsk"])

                qty = float(row["qty"])
                forward = (float(row["undBid"]) + float(row["undAsk"])) / 2

                trades[name] = {
                    "qty": qty,
                    "theo": float(price),
                    "prompt": prompt,
                    "forward": forward,
                    "iv": float(vol),
                    "delta": float(delta),
                    "gamma": float(gamma) * qty,
                    "vega": float(vega),
                    "theta": float(theta),
                    "counterparty": counterparty,
                }

            return trades, clickdata

        else:

            # on delete work over indices and delete rows then update trades dict
            if int(delete) > int(buy) and int(delete) > int(sell):
                if clickdata:
                    for i in clickdata:
                        instrument = rows[i]["Instrument"]
                        trades.pop(instrument, None)
                        clickdata = []
            else:
                # not delete so see ifs its a buy/sell button click
                # create name then go over buy/sell and action

                # find the stat mults
                statWeights = stratConverstion[strat]
                # clac forward and prompt
                Bforward, Aforward = placholderCheck(forward, pforward)
                prompt = str(
                    dt.datetime.strptime(expiry[:10], "%d/%m/%Y") + timedelta(days=14)
                )[:10]
                futureName = str(product)[:3] + " " + str(prompt)

                # calc strat for buy
                if int(buy) > int(sell) and int(buy) > int(delete):
                    bs = 1

                elif int(buy) < int(sell) and int(sell) > int(delete):
                    bs = -1

                if bs != 0:
                    deltaBucket = 0
                    # calc one leg weight
                    if statWeights[0] != 0:
                        # get strike from value and placeholder
                        onestrike = strikePlaceholderCheck(onestrike, ponestrike)

                        weight = statWeights[0] * bs
                        name = (
                            str(product)
                            + " "
                            + str(onestrike)
                            + " "
                            + str(onecop).upper()
                        )
                        trades[name] = {
                            "qty": qty * weight,
                            "theo": round(float(onetheo), 2),
                            "prompt": prompt,
                            "forward": Bforward,
                            "iv": float(oneiv) * weight,
                            "delta": float(onedelta) * weight * qty,
                            "gamma": float(onegamma) * weight * qty,
                            "vega": float(onevega) * weight * qty,
                            "theta": float(onetheta) * weight * qty,
                            "counterparty": counterparty,
                        }
                        # add delta to delta bucket for hedge
                        deltaBucket += float(onedelta) * weight * qty

                    # calc two leg weight
                    if statWeights[1] != 0:
                        # get strike from value and placeholder
                        twostrike = strikePlaceholderCheck(twostrike, ptwostrike)

                        weight = statWeights[1] * bs
                        name = (
                            str(product)
                            + " "
                            + str(twostrike)
                            + " "
                            + str(twocop).upper()
                        )
                        trades[name] = {
                            "qty": float(qty) * weight,
                            "theo": round(float(twotheo), 2),
                            "prompt": prompt,
                            "forward": Bforward,
                            "iv": float(twoiv) * weight,
                            "delta": float(twodelta) * weight * qty,
                            "gamma": float(twogamma) * weight * qty,
                            "vega": float(twovega) * weight * qty,
                            "theta": float(twotheta) * weight * qty,
                            "counterparty": counterparty,
                        }
                        # add delta to delta bucket for hedge
                        deltaBucket += float(twodelta) * weight * qty

                    # calc three leg weight
                    if statWeights[2] != 0:
                        # get strike from value and placeholder
                        threestrike = strikePlaceholderCheck(threestrike, pthreestrike)

                        weight = statWeights[2] * bs
                        name = (
                            str(product)
                            + " "
                            + str(threestrike)
                            + " "
                            + str(threecop).upper()
                        )
                        trades[name] = {
                            "qty": float(qty) * weight,
                            "theo": round(float(threetheo), 2),
                            "prompt": prompt,
                            "forward": Bforward,
                            "iv": float(threeiv) * weight,
                            "delta": float(threedelta) * weight * qty,
                            "gamma": float(threegamma) * weight * qty,
                            "vega": float(threevega) * weight * qty,
                            "theta": float(threetheta) * weight * qty,
                            "counterparty": counterparty,
                        }
                        # add delta to delta bucket for hedge
                        deltaBucket += float(threedelta) * weight * qty

                    # calc four leg weight
                    if statWeights[3] != 0:
                        # get strike from value and placeholder
                        fourstrike = strikePlaceholderCheck(fourstrike, pfourstrike)

                        weight = statWeights[3] * bs
                        name = (
                            str(product)
                            + " "
                            + str(fourstrike)
                            + " "
                            + str(fourcop).upper()
                        )
                        trades[name] = {
                            "qty": float(qty) * weight,
                            "theo": round(float(fourtheo), 2),
                            "prompt": prompt,
                            "forward": Bforward,
                            "iv": float(fouriv) * weight,
                            "delta": float(fourdelta) * weight * qty,
                            "gamma": float(fourgamma) * weight * qty,
                            "vega": float(fourvega) * weight * qty,
                            "theta": float(fourtheta) * weight * qty,
                            "counterparty": counterparty,
                        }
                        # add delta to delta bucket for hedge
                        deltaBucket += float(fourdelta) * weight * qty
                    # if vol trade then add hedge along side
                    if pricevola == "vol":
                        delta = round(float(deltaBucket), 0) * -1

                        hedge = {
                            "qty": delta,
                            "theo": Bforward,
                            "prompt": prompt,
                            "forward": Bforward,
                            "iv": 0,
                            "delta": delta,
                            "gamma": 0,
                            "vega": 0,
                            "theta": 0,
                            "counterparty": counterparty,
                        }
                        if futureName in trades:
                            trades[futureName]["qty"] = (
                                trades[futureName]["qty"] + hedge["qty"]
                            )
                        else:
                            trades[futureName] = hedge
            return trades, clickdata


# delete all input values on product changes
@app.callback(
    [
        Output("oneStrike", "value"),
        Output("oneVol_price", "value"),
        Output("twoStrike", "value"),
        Output("twoVol_price", "value"),
        Output("threeStrike", "value"),
        Output("threeVol_price", "value"),
        Output("fourStrike", "value"),
        Output("fourVol_price", "value"),
    ],
    [Input("productCalc-selector", "value"), Input("monthCalc-selector", "value")],
)
def clearSelectedRows(product, month):
    return "", "", "", "", "", "", "", ""


# send trade to system
@app.callback(
    Output("tradeSent", "is_open"),
    [Input("trade", "n_clicks")],
    [State("tradesTable", "selected_rows"), State("tradesTable", "data")],
)
def sendTrades(clicks, indices, rows):
    timestamp = timeStamp()
    user = request.authorization["username"]
    if indices:
        for i in indices:
            # create st to record which products to update in redis
            redisUpdate = set([])
            # check that this is not the total line.
            if rows[i]["Instrument"] != "Total":

                if rows[i]["Instrument"][3] == "O":
                    # is option
                    product = rows[i]["Instrument"][:6]
                    redisUpdate.add(product)
                    productName = (rows[i]["Instrument"]).split(" ")
                    strike = productName[1]
                    CoP = productName[2]

                    prompt = rows[i]["Prompt"]
                    price = rows[i]["Theo"]
                    qty = rows[i]["Qty"]
                    counterparty = rows[i]["Counterparty"]

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
                elif rows[i]["Instrument"][3] == " ":
                    # is futures
                    product = rows[i]["Instrument"][:3]
                    redisUpdate.add(product)
                    prompt = rows[i]["Prompt"]
                    price = rows[i]["Theo"]
                    qty = rows[i]["Qty"]
                    counterparty = rows[i]["Counterparty"]

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
                    updateRedisCurve(update)
        return True


# send trade to F2 and exchange


@app.callback(
    [
        Output("reponseOutput", "children"),
        Output("tradeRouted", "is_open"),
        Output("tradeRoutedFail", "is_open"),
    ],
    [Input("report", "n_clicks_timestamp"), Input("clientRecap", "n_clicks_timestamp")],
    [State("tradesTable", "selected_rows"), State("tradesTable", "data")],
)
def sendTrades(report, recap, indices, rows):

    # string to hold router respose
    tradeResponse = "## Response"
    if (int(report) + int(recap)) == 0:
        raise PreventUpdate

    # enact trade recap logic
    if int(recap) > int(report):
        response = "Recap: \r\n"
        if indices:
            for i in indices:
                if rows[i]["Instrument"][3] == "O":
                    # is option
                    instrument = rows[i]["Instrument"].split()
                    product = codeToName(instrument[0])
                    strike = instrument[1]
                    CoP = instrument[2]
                    month = codeToMonth(instrument[0])

                    if CoP == "C":
                        CoP = "calls"
                    elif CoP == "P":
                        CoP = "puts"

                    price = round(abs(float(rows[i]["Theo"])), 2)
                    qty = float(rows[i]["Qty"])
                    vol = round(abs(float(rows[i]["IV"])), 2)
                    if qty > 0:
                        bs = "Sell"
                    elif qty < 0:
                        bs = "Buy"
                    else:
                        continue

                    response += "You {} {} {} {} {} {} at {} ({}%) \r\n".format(
                        bs,
                        abs(int(qty)),
                        month,
                        product,
                        strike,
                        CoP,
                        price,
                        round(vol, 2),
                    )
                elif rows[i]["Instrument"][3] == " ":
                    # is futures
                    instrument = rows[i]["Instrument"].split()
                    date = datetime.strptime(instrument[1], "%Y-%m-%d")
                    month = date.strftime("%b")[:3]
                    product = rows[i]["Instrument"][:3]
                    prompt = rows[i]["Prompt"]
                    price = rows[i]["Theo"]
                    qty = rows[i]["Qty"]
                    if qty > 0:
                        bs = "Sell"
                    elif qty < 0:
                        bs = "Buy"

                    response += "You {} {} {} {} at {} \r\n".format(
                        bs, abs(int(qty)), month, product, price
                    )
            print(response)
            return response, False, False
        else:
            return "No rows selected", False, False

    # find user related trade details
    timestamp = timeStamp()
    user = request.authorization["username"]
    if int(recap) < int(report):
        if indices:
            for i in indices:
                if rows[i]["Instrument"][3] == "O":
                    # is option
                    instrument = rows[i]["Instrument"].split()
                    product = instrument[0]
                    strike = instrument[1]
                    CoP = instrument[2]
                    prompt = rows[i]["Prompt"]
                    price = round(float(rows[i]["Theo"]), 2)
                    qty = rows[i]["Qty"]
                    vol = rows[i]["IV"]
                    counterparty = rows[i]["Counterparty"]
                    underlying = rows[i]["Forward"]

                    # build trade object
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
                        underlying=underlying,
                    )

                    # assign unique id to trade
                    trade.id = tradeID()
                    # assign vol
                    trade.vol = vol

                    # take trade fixml
                    fixml = trade.fixml()

                    # send it to the soap server
                    response = sendFIXML(fixml)

                    # set pop alert booleans
                    if response["Status"] == "Routed":
                        good, bad = True, False
                    else:
                        good, bad = False, True

                    # store action for auditing
                    storeTradeSend(trade, response)

                    response = responseParser(response)

                    # attached reposne to print out
                    tradeResponse = tradeResponse + os.linesep + response

                elif rows[i]["Instrument"][3] == " ":
                    # is futures
                    product = rows[i]["Instrument"][:3]
                    prompt = rows[i]["Prompt"]
                    price = round(float(rows[i]["Theo"]), 2)
                    qty = rows[i]["Qty"]
                    counterparty = rows[i]["Counterparty"]
                    underlying = rows[i]["Forward"]

                    # build trade object
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
                        underlying=underlying,
                    )

                    # assign unique id to trade
                    trade.id = tradeID()

                    # take trade fixml
                    fixml = trade.fixml()

                    # send it to the soap server
                    response = sendFIXML(fixml)

                    # set pop alert booleans
                    if response["Status"] == "Routed":
                        good, bad = True, False
                    else:
                        good, bad = False, True

                    # store action for auditing
                    storeTradeSend(trade, response)

                    response = responseParser(response)

                    # attached reposne to print out
                    tradeResponse = tradeResponse + os.linesep + response

            return tradeResponse, good, bad


def responseParser(response):

    return "Status: {} Error: {}".format(response["Status"], response["ErrorMessage"])


# pull 3m from product data
@app.callback(Output("calculatorBasis", "placeholder"), [Input("productInfo", "data")])
def loadBasis(data):
    if data != None:
        return data["3m_und"]
    else:
        return str(0)


# clear inputs on product change
@app.callback(
    Output("calculatorBasis", "value"),
    [Input("productCalc-selector", "value"), Input("monthCalc-selector", "value")],
)
def loadBasis(product, month):
    return ""


@app.callback(
    Output("calculatorForward", "value"),
    [Input("productCalc-selector", "value"), Input("monthCalc-selector", "value")],
)
def loadBasis(product, month):
    return ""


@app.callback(
    Output("interestRate", "value"),
    [Input("productCalc-selector", "value"), Input("monthCalc-selector", "value")],
)
def loadBasis(product, month):
    return ""


@app.callback(
    Output("calculatorSpread1", "value"),
    [Input("productCalc-selector", "value"), Input("monthCalc-selector", "value")],
)
def loadBasis(product, month):
    return ""


@app.callback(
    Output("calculatorPrice/Vola", "value"),
    [Input("productCalc-selector", "value"), Input("monthCalc-selector", "value")],
)
def loadBasis(product, month):
    return ""


@app.callback(Output("interestRate", "placeholder"), [Input("3wed", "children")])
def loadBasis(expiry):
    data = loadRedisData("USDRate")
    expiry = expiry.split("/")
    expiry = expiry[2] + expiry[1] + expiry[0]
    if data != None:
        data = json.loads(data)
        data = float(data[expiry]["Interest Rate"]) * 100
        return str(data)
    else:
        return str(0)


# update product info on product change
@app.callback(
    Output("productInfo", "data"),
    [Input("productCalc-selector", "value"), Input("monthCalc-selector", "value")],
)
def updateProduct(product, month):
    if month == "3M":
        return 0
    else:
        product = product + "O" + month
        params = loadRedisData(product.lower())
        params = json.loads(params)
        return params


@app.callback(
    Output("calculatorForward", "placeholder"),
    [
        Input("calculatorBasis", "value"),
        Input("calculatorBasis", "placeholder"),
        Input("calculatorSpread1", "value"),
        Input("calculatorSpread1", "placeholder"),
    ],
)
def loadBasis(basis, pbasis, spread, pspread):
    if not basis:
        basis = pbasis
    if not spread:
        spread = pspread

    if basis and spread:
        basis = str(basis).split("/")
        spread = str(spread).split("/")

        if len(basis) > 1 and len(spread) > 1:
            bid = float(basis[0]) + float(spread[0])
            ask = float(basis[1]) + float(spread[1])
            return str(bid) + "/" + str(ask)
        elif len(spread) > 1:
            bid = float(basis[0]) + float(spread[0])
            ask = float(basis[0]) + float(spread[1])
            return str(bid) + "/" + str(ask)
        elif len(basis) > 1:
            bid = float(basis[0]) + float(spread[0])
            ask = float(basis[1]) + float(spread[0])
            return str(bid) + "/" + str(ask)
        else:
            spread2 = float(basis[0]) + float(spread[0])
            return str(spread2)
    elif spread == 0:
        return basis

    else:
        return 0


def placholderCheck(value, placeholder):
    if value and value != None and value != " ":
        value = value.split("/")
        if len(value) > 1:
            if value[1] != "":
                return float(value[0]), float(value[1])
            else:
                return float(value[0]), float(value[0])
        else:
            return float(value[0]), float(value[0])

    elif placeholder and placeholder != " ":
        placeholder = placeholder.split("/")
        if len(placeholder) > 1 and placeholder[1] != " ":
            return float(placeholder[0]), float(placeholder[1])
        else:
            return float(placeholder[0]), float(placeholder[0])
    else:
        return 0, 0


def strikePlaceholderCheck(value, placeholder):
    if value:
        return value
    elif placeholder:
        value = placeholder.split(".")
        return value[0]
    else:
        return 0


legOptions = ["one", "two", "three", "four"]

# create fecth strikes function
def buildFetchStrikes():
    def updateDropdown(product, month):
        if month == "3M":
            return 0
        else:
            product = product + "O" + month
            strikes = fetechstrikes(product)
            length = int(len(strikes) / 2)
            value = strikes[length]["value"]
            return value

    return updateDropdown


# create vola function
def buildUpdateVola(leg):
    def updateVola(params, strike, pStrike, cop, priceVol):
        # get strike from strike vs pstrike
        strike = str(int(placholderCheck(strike, pStrike)[0]))

        if strike:

            if params:
                if strike in params["strikes"]:
                    if priceVol == "vol":
                        vola = float(params["strikes"][strike][cop.upper()]["vola"])
                        if type(vola) == float:
                            return str(round(vola * 100, 2))
                        else:
                            return " "

                    elif priceVol == "price":
                        return params["strikes"][strike][cop.upper()]["theo"]
        else:
            return 0

    return updateVola


def buildvolaCalc(leg):
    def volaCalc(
        expiry,
        nowOpen,
        rate,
        prate,
        forward,
        pforward,
        strike,
        pStrike,
        cop,
        priceVola,
        ppriceVola,
        volprice,
        days,
        params,
    ):
        print("testing for params")
        if params != None:
            print("Had params")
            # get inputs placeholders vs values
            strike = str(int(placholderCheck(strike, pStrike)[0]))
            Brate, Arate = placholderCheck(rate, prate)
            Bforward, Aforward = placholderCheck(forward, pforward)
            BpriceVola, ApriceVola = placholderCheck(priceVola, ppriceVola)

            # set eval date
            eval_date = dt.datetime.now()
            # build params object
            params = buildSurfaceParams(params, Bforward, expiry, eval_date)

            if None not in (
                expiry,
                Bforward,
                Aforward,
                BpriceVola,
                ApriceVola,
                strike,
                cop,
            ):
                if nowOpen == "now":
                    now = True
                else:
                    now = False
                today = dt.datetime.today()
                if volprice == "vol":

                    option = Option(
                        cop,
                        Bforward,
                        strike,
                        today,
                        expiry,
                        Brate / 100,
                        BpriceVola / 100,
                        days=days,
                        now=now,
                        params=params,
                    )
                    Bgreeks = option.get_all()

                    return {"bid": Bgreeks, "Bvol": BpriceVola}

                elif volprice == "price":
                    option = Option(
                        cop,
                        Bforward,
                        strike,
                        today,
                        expiry,
                        Brate / 100,
                        0,
                        price=BpriceVola,
                        days=days,
                        now=now,
                        params=params,
                    )
                    option.get_impl_vol()
                    Bvol = option.vol
                    Bgreeks = list(option.get_all())
                    Bgreeks[0] = BpriceVola

                    return {"bid": Bgreeks, "Bvol": Bvol * 100}

            return volaCalc


def createLoadParam(param):
    def loadtheo(params):
        # pull greeks from stored hidden
        if params != None:
            print("return greek")
            return str("%.4f" % params["bid"][param[1]])

    return loadtheo


def buildVoltheta():
    def loadtheo(vega, theta):
        if vega != None and theta != None:
            vega = float(vega)
            if vega > 0:
                return "%.2f" % (float(theta) / vega)
            else:
                return 0
        else:
            return "n/a"

    return loadtheo


def buildTheoIV():
    def loadIV(params):
        if params != None:
            # params = json.loads(params)
            return str("%.4f" % params["Bvol"])
        else:
            return "n/a"

    return loadIV


# create placeholder function for each {leg}Strike
for leg in legOptions:
    app.callback(
        Output("{}Strike".format(leg), "placeholder"),
        [Input("productCalc-selector", "value"), Input("monthCalc-selector", "value")],
    )(buildFetchStrikes())

    app.callback(
        Output("{}Vol_price".format(leg), "placeholder"),
        [
            Input("productInfo", "data"),
            Input("{}Strike".format(leg), "value"),
            Input("{}Strike".format(leg), "placeholder"),
            Input("{}CoP".format(leg), "value"),
            Input("calculatorVol_price", "value"),
        ],
    )(buildUpdateVola(leg))

    app.callback(
        Output("{}CalculatorCalculatorData".format(leg), "children"),
        [
            Input(component_id="calculatorExpiry", component_property="children"),
            Input(component_id="nowOpen", component_property="value"),
            Input(component_id="interestRate", component_property="value"),
            Input(component_id="interestRate", component_property="placeholder"),
            Input(component_id="calculatorForward", component_property="value"),
            Input(component_id="calculatorForward", component_property="placeholder"),
            Input(component_id="{}Strike".format(leg), component_property="value"),
            Input(
                component_id="{}Strike".format(leg), component_property="placeholder"
            ),
            Input(component_id="{}CoP".format(leg), component_property="value"),
            Input(component_id="{}Vol_price".format(leg), component_property="value"),
            Input(
                component_id="{}Vol_price".format(leg), component_property="placeholder"
            ),
            Input(component_id="calculatorVol_price", component_property="value"),
            Input(component_id="dayConvention", component_property="value"),
            Input(component_id="paramsStore", component_property="data"),
        ],
    )(buildvolaCalc(leg))

    # calculate the vol thata from vega and theta
    app.callback(
        Output("{}volTheta".format(leg), "children"),
        [
            Input("{}Vega".format(leg), "children"),
            Input("{}Theta".format(leg), "children"),
        ],
    )(buildVoltheta())

    # add callbacks that fill in the IV
    app.callback(
        Output("{}IV".format(leg), "children"),
        [Input("{}CalculatorCalculatorData".format(leg), "children")],
    )(buildTheoIV())

    # add different greeks to leg and calc
    for param in [
        ["Theo", 0],
        ["Delta", 1],
        ["Gamma", 3],
        ["Vega", 4],
        ["Theta", 2],
        ["FullDelta", 11],
    ]:

        app.callback(
            Output("{}{}".format(leg, param[0]), "children"),
            [Input("{}CalculatorCalculatorData".format(leg), "children")],
        )(createLoadParam(param))


def buildStratGreeks():
    def stratGreeks(strat, one, two, three, four):
        strat = stratConverstion[strat]
        greek = (
            (strat[0] * float(one))
            + (strat[1] * float(two))
            + (strat[2] * float(three))
            + (strat[3] * float(four))
        )
        greek = round(greek, 2)
        return str(greek)

    return stratGreeks


# add different greeks to leg and calc
for param in ["Theo", "FullDelta", "Delta", "Gamma", "Vega", "Theta", "IV", "volTheta"]:

    app.callback(
        Output("strat{}".format(param), "children"),
        [
            Input("strategy", "value"),
            Input("one{}".format(param), "children"),
            Input("two{}".format(param), "children"),
            Input("three{}".format(param), "children"),
            Input("four{}".format(param), "children"),
        ],
    )(buildStratGreeks())
