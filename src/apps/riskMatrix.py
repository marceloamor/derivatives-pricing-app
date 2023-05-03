from dash.dependencies import Input, Output, State
from dash import dcc, html
from dash import dcc
import plotly.figure_factory as ff
import dash_bootstrap_components as dbc
from dash import dash_table as dtable
import pandas as pd
from pandas.plotting import table
import datetime as dt
from datetime import datetime
import requests, math, ast, os, json, colorlover
import plotly.graph_objs as go
from dash import no_update
import numpy as np

from data_connections import riskAPi
from parts import (
    topMenu,
    onLoadPortFolio,
    heatunpackRisk,
    heampMapColourScale,
    curren3mPortfolio,
    unpackPriceRisk,
)

# production port
baseURL = "http://{}:8050/RiskApi/V1/risk".format(riskAPi)
# baseURL = "http://{}/RiskApi/V1/risk".format(riskAPi)

undSteps = {
    "aluminium": "10",
    "copper": "40",
    "nickel": "100",
    "zinc": "10",
    "lead": "10",
}


def buildURL(base, portfolio, und, vol, level, eval, rels):
    und = "und=" + str(und)[1:-1]
    vol = "vol=" + str(vol)[1:-1]
    level = "level=" + level
    portfolio = "portfolio=" + portfolio
    rels = "rel=" + rels
    eval = "eval=" + eval

    url = (
        base
        + "?"
        + portfolio
        + "&"
        + vol
        + "&"
        + und
        + "&"
        + level
        + "&"
        + eval
        + "&"
        + rels
    )
    print(url)
    return url


def discrete_background_color_bins(df, n_bins=4, columns="all"):
    bounds = [i * (1.0 / n_bins) for i in range(n_bins + 1)]

    if columns == "all":
        if "id" in df:
            df_numeric_columns = df.select_dtypes("number").drop(["id"], axis=1)
        else:
            df_numeric_columns = df.select_dtypes("number")
    else:
        df_numeric_columns = df[columns]

    df_max = df_numeric_columns.max().max()
    df_min = df_numeric_columns.min().min()

    styles = []

    # build ranges
    ranges = [(df_max * i) for i in bounds]

    for i in range(1, len(ranges)):
        min_bound = ranges[i - 1]
        max_bound = ranges[i]
        half_bins = str(len(bounds))
        backgroundColor = colorlover.scales[half_bins]["seq"]["Greens"][i - 1]
        color = "black"
        for column in df_numeric_columns:
            styles.append(
                {
                    "if": {
                        "filter_query": (
                            "{{{column}}} >= {min_bound}"
                            + (
                                " && {{{column}}} < {max_bound}"
                                if (i < len(ranges) - 1)
                                else ""
                            )
                        ).format(
                            column=column, min_bound=min_bound, max_bound=max_bound
                        ),
                        "column_id": str(column),
                    },
                    "backgroundColor": backgroundColor,
                    "color": color,
                }
            )

    # build ranges
    ranges = [(df_min * i) for i in bounds]
    for i in range(1, len(ranges)):
        min_bound = ranges[i - 1]
        max_bound = ranges[i]
        half_bins = str(len(ranges))
        backgroundColor = colorlover.scales[half_bins]["seq"]["Reds"][i - 1]
        color = "black"
        for column in df_numeric_columns:
            styles.append(
                {
                    "if": {
                        "filter_query": (
                            "{{{column}}} <= {min_bound}"
                            + (
                                " && {{{column}}} > {max_bound}"
                                if (i < len(ranges) - 1)
                                else ""
                            )
                        ).format(
                            column=column, min_bound=min_bound, max_bound=max_bound
                        ),
                        "column_id": str(column),
                    },
                    "backgroundColor": backgroundColor,
                    "color": color,
                }
            )

    # add zero color
    for column in df_numeric_columns:
        styles.append(
            {
                "if": {
                    "filter_query": ("{{{column}}} = 0").format(column=column),
                    "column_id": str(column),
                },
                "backgroundColor": "rgb(255,255,255)",
                "color": color,
            }
        )
    return styles


options = dbc.Row(
    [
        dbc.Col(
            [
                html.Label(
                    ["Portfolio:"], style={"font-weight": "bold", "text-align": "left"}
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                dcc.Dropdown(
                                    id="riskPortfolio",
                                    options=onLoadPortFolio(),
                                    value="copper",
                                )
                            ]
                        )
                    ]
                ),
                html.Label(
                    ["Basis Price:"],
                    style={"font-weight": "bold", "text-align": "left"},
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.Div(
                                    [
                                        dcc.Input(
                                            id="basisPrice",
                                            placeholder=8500,
                                            type="number",
                                        )
                                    ]
                                ),
                            ]
                        )
                    ]
                ),
            ],
            width=2,
        ),
        dbc.Col(
            [
                html.Label(
                    ["Price Shock Step Size:"],
                    style={"font-weight": "bold", "text-align": "left"},
                ),
                html.Div([dcc.Input(id="shockSize", placeholder=1, type="number")]),
                html.Label(
                    ["Price Shock Max:"],
                    style={"font-weight": "bold", "text-align": "left"},
                ),
                html.Div([dcc.Input(id="shockMax", placeholder=10, type="number")]),
            ],
            width=2,
        ),
        dbc.Col(
            [
                html.Label(
                    ["Time Step Size:"],
                    style={"font-weight": "bold", "text-align": "left"},
                ),
                html.Div([dcc.Input(id="timeStepSize", placeholder=1, type="number")]),
                html.Label(
                    ["Time Max:"], style={"font-weight": "bold", "text-align": "left"}
                ),
                html.Div([dcc.Input(id="timeMax", placeholder=10, type="number")]),
            ],
            width=2,
        ),
        dbc.Col(
            [
                html.Label(
                    ["Evaluation Date:"],
                    style={"font-weight": "bold", "text-align": "left"},
                ),
                html.Div(
                    [
                        dcc.DatePickerSingle(
                            id="evalDate",
                            month_format="MMMM Y",
                            placeholder="MMMM Y",
                            date=dt.datetime.today(),
                        )
                    ],
                ),
                html.Br(),
                html.Div(dbc.Button("generate!", id="riskMatrix-button", n_clicks=0)),
            ],
            width=2,
        ),
        dbc.Col(
            [
                html.Br(),
                html.Label(
                    ["Greek:"], style={"font-weight": "bold", "text-align": "left"}
                ),
                dcc.Dropdown(
                    id="greeks",
                    options=[
                        {
                            "label": "Full Delta",
                            "value": "full_delta",
                        },
                        {"label": "Delta", "value": "delta"},
                        {"label": "Vega", "value": "vega"},
                        {"label": "Gamma", "value": "gamma"},
                        {"label": "Theta", "value": "theta"},
                    ],
                    value="full_delta",
                ),
            ],
            width=2,
        ),
    ]
)

# priceMatrix = dbc.Row(
#     [
#         dbc.Col(
#             [
#                 dcc.Loading(
#                     id="loading-2",
#                     type="circle",
#                     children=[dtable.DataTable(id="priceMatrix", data=[{}])],
#                 )
#             ]
#         )
#     ]
# )

heatMap = dbc.Row(
    [
        dbc.Col(
            [
                dcc.Loading(
                    id="loading-2",
                    type="circle",
                    children=[
                        dtable.DataTable(
                            id="riskMatrix",
                            data=[{}],
                            # fixed_columns={'headers': True, 'data': 1},
                            style_table={"overflowX": "scroll", "minWidth": "100%"},
                        )
                    ],
                )
            ]
        )
    ]
)

greeksTable = dbc.Row(
    [
        dbc.Col(
            [
                dcc.Loading(
                    id="loading-2",
                    type="circle",
                    children=[
                        dtable.DataTable(
                            id="greeksTable",
                            data=[{}],
                            # fixed_columns={'headers': True, 'data': 1},
                            style_table={"overflowX": "scroll", "minWidth": "100%"},
                        )
                    ],
                )
            ]
        )
    ]
)

# heatMap = dbc.Row(
#     [
#         dbc.Col(
#             [
#                 dcc.Loading(
#                     id="loading-1", type="circle", children=[dcc.Graph(id="heatMap")]
#                 )
#             ]
#         )
#     ]
# )

hidden = dbc.Row([dcc.Store(id="riskData")])

layout = html.Div([topMenu("Risk Matrix"), options, heatMap, html.Br(), greeksTable, hidden])


def placholderCheck(value, placeholder):
    if value and value != None:
        return float(value)

    elif placeholder and placeholder != None:
        return float(placeholder)


def initialise_callbacks(app):
    # populate data
    @app.callback(
        Output("riskData", "data"),
        Input("riskMatrix-button", "n_clicks"),
        [
            State("riskPortfolio", "value"),
            # State("riskType", "value"),
            State("basisPrice", "placeholder"),
            State("basisPrice", "value"),
            State("shockSize", "placeholder"),
            State("shockSize", "value"),
            State("shockMax", "placeholder"),
            State("shockMax", "value"),
            State("timeStepSize", "placeholder"),
            State("timeStepSize", "value"),
            State("timeMax", "placeholder"),
            State("timeMax", "value"),
            State("evalDate", "date"),
            # State("abs/rel", "value"),
        ],
    )
    def load_data(
        n_clicks,
        portfolio,
        basisPriceP,
        basisPrice,
        shockSizeP,
        shockSize,
        shockMaxP,
        shockMax,
        timeStepSizeP,
        timeStepSize,
        timeMaxP,
        timeMax,
        evalDate,
    ):

        # placeholder check
        if not shockSize:
            shockSize = shockSizeP
        if not shockMax:
            shockMax = shockMaxP
        if not timeStepSize:
            timeStepSize = timeStepSizeP
        if not timeMax:
            timeMax = timeMaxP
        if not basisPrice:
            basisPrice = basisPriceP

        evalDate = evalDate.split("T")[0]
        days_offset = (
            dt.datetime.today() - dt.datetime.strptime(evalDate, "%Y-%m-%d")
        ).days

        if portfolio and n_clicks > 0:
            try:
                r = requests.get(
                    "http://0.0.0.0:8008/generate/{}".format(portfolio),
                    params={
                        "basis_price": str(basisPrice),
                        "shock_max": str(shockMax),
                        "shock_step": str(shockSize),
                        "from_today_offset_days": str(days_offset),
                        "time_max": str(timeMax),
                        "time_step": str(timeStepSize),
                    },
                )
                data = json.loads(r.text)
                return data
            except:
                print("error loading data")
                return no_update

    # risk matrix heat map
    @app.callback(
        Output("riskMatrix", "data"),
        Output("riskMatrix", "style_data_conditional"),
        Input("riskData", "data"),
        Input("greeks", "value"),
        prevent_initial_call=True,
    )
    def load_data(data, greek):
        if data and greek:

            df = pd.DataFrame(data)
            df = df.applymap(lambda x: x.get(greek))

            df = df.swapaxes("index", "columns")
            df = df.reset_index()

            # round figures for display
            if greek == "gamma":
                df = df.round(4)
            else:
                df = df.round(1)

            styles = discrete_background_color_bins(df)
            data = df.to_dict("records")

            return data, styles
        else:
            return no_update, no_update

    # second figure
    @app.callback(
        Output("greeksTable", "data"),
        Input("riskData", "data"),
        prevent_initial_call=True,
    )
    def load_data(data):
        if data:

            df = pd.DataFrame(data)

            # new_format = lambda x: '\n'.join([f"{k}: {v}" for k, v in x.items()])
            # df = df.applymap(new_format)

            # select today's greeks and transpose
            today = df.iloc[:, 0]
            df = df.swapaxes("index", "columns")

            df = pd.DataFrame(today.to_dict())
            df = df.reset_index()

            df.columns = ["greeks"] + list(df.columns[1:])

            print(df)

            df = df.round(3)

            data = df.to_dict('records')

            return data
        else:
            return no_update

            # df = pd.DataFrame(data)
            # new_format = lambda x: '\n'.join([f"{k}: {v}" for k, v in x.items()])

            # # apply lambda function to each element of DataFrame
            # df = df.applymap(new_format)

            # df = df.T

            # df["date"] = df.index

            # print(df)

            # print("data\nloaded")

        ####### datatable!!!!!!!!!!!!
        # pull data from data store
        # filter if needed
        # display in table with following format to wrap lines in columns
        # dash_table.DataTable(
        #     id="table",
        #     columns=[{"name": i, "id": i} for i in df.columns],
        #     data=df.to_dict("records"),
        #     style_cell={"whiteSpace": "pre-line"},
        # )

        # list to default moves
        # list = [-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5]

        # # create und/vol steps from default
        # step = placholderCheck(stepV, stepP)
        # vstep = placholderCheck(vstepV, vstepP) / 100

        # # convert eval data to datetime
        # eval = datetime.strptime(eval[:10], "%Y-%m-%d")
        # eval = datetime.strftime(eval, "%d/%m/%Y")

        # build url and inputs and url and send to API
        # if step:
        #     und = [x * step for x in list]
        #     vol = [x * vstep for x in list]
        #     url = buildURL(baseURL, portfolio, und, vol, "high", eval, rels)
        #     myResponse = requests.get(url)

        #     # parse response and return output
        #     if myResponse.ok:
        #         messageContent = myResponse.content
        #         print(ast.literal_eval(messageContent.decode("utf-8")))
        #         return ast.literal_eval(messageContent.decode("utf-8"))
        #     else:
        #         # If response code is not ok (200), print the resulting http error code with description
        #         print(myResponse.raise_for_status())
        #         return no_update

    # populate data
    # @app.callback(
    #     Output("riskData", "data"),
    #     [
    #         Input("riskData", "data"),
    #     ],
    # )
    # def load_data(portfolio):
    #     return None

    # send APIinputs to risk API and display results
    # @app.callback(
    #     Output("heatMap", "figure"),
    #     [Input("riskType", "value"), Input("riskData", "data")],
    #     [
    #         State("stepSize", "placeholder"),
    #         State("stepSize", "value"),
    #         State("VstepSize", "placeholder"),
    #         State("VstepSize", "value"),
    #         State("riskPortfolio", "value"),
    #     ],
    # )
    # def load_data(greek, data, stepP, stepV, vstepP, vstepV, portfolio):
    #     # find und/vol step from placeholder/value
    #     step = placholderCheck(stepV, stepP)
    #     vstep = placholderCheck(vstepV, vstepP)

    #     if data:
    #         # uun pack then re pack data into the required frames
    #         jdata, underlying, volaility = heatunpackRisk(data, greek)

    #         # convert underlying in to absolute from relataive
    #         tM = curren3mPortfolio(portfolio.lower())
    #         underlying = [float(x) + tM for x in underlying]

    #         # build anotaions
    #         annotations = []
    #         z = jdata
    #         y = underlying
    #         x = volaility
    #         for n, row in enumerate(z):
    #             for m, val in enumerate(row):
    #                 annotations.append(
    #                     go.layout.Annotation(
    #                         text=str(z[n][m]),
    #                         x=x[m],
    #                         y=y[n],
    #                         xref="x1",
    #                         yref="y1",
    #                         showarrow=False,
    #                         font=dict(color="white"),
    #                     )
    #                 )

    #         # build traces to pass to heatmap
    #         trace = go.Heatmap(
    #             x=x, y=y, z=z, colorscale=heampMapColourScale, showscale=False
    #         )
    #         fig = go.Figure(data=([trace]))

    #         # add annotaions and labels to figure
    #         fig.layout.annotations = annotations
    #         fig.layout.yaxis.title = "Underlying ($)"
    #         fig.layout.xaxis.title = "Volatility (%)"
    #         fig.layout.xaxis.tickmode = "linear"
    #         fig.layout.xaxis.dtick = vstep
    #         fig.layout.yaxis.dtick = step

    #         # reutrn complete figure
    #         return fig

    # @app.callback(
    #     [
    #         Output("priceMatrix", "data"),
    #         Output("priceMatrix", "columns"),
    #         Output("priceMatrix", "style_data_conditional"),
    #     ],
    #     [Input("riskData", "data")],
    #     [State("riskPortfolio", "value")],
    # )
    # def load_data(data, portfolio):
    #     if data:
    #         tm = curren3mPortfolio(portfolio.lower())
    #         data = unpackPriceRisk(data, tm)
    #         columns = [{"name": str(i), "id": str(i)} for i in data[0]]

    #         # find middle column to highlight later
    #         middleColumn = columns[6]["id"]
    #         style_data_conditional = [
    #             {
    #                 "if": {"column_id": middleColumn},
    #                 "backgroundColor": "#3D9970",
    #                 "color": "white",
    #             }
    #         ]

    #         return data, columns, style_data_conditional

    # rounding function for stepSize
    def roundup(x):
        return int(math.ceil(x / 5.0)) * 5

    # filled in breakeven on product change
    @app.callback(Output("stepSize", "placeholder"), [Input("riskPortfolio", "value")])
    def pullStepSize(portfolio):
        return undSteps[portfolio.lower()]

    # clear inputs on product change
    @app.callback(Output("stepSize", "value"), [Input("riskPortfolio", "value")])
    def loadBasis(product):
        return ""

    # clear inputs on product change
    @app.callback(Output("VstepSize", "value"), [Input("riskPortfolio", "value")])
    def loadBasis(product):
        return ""
