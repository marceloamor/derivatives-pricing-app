from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_core_components as dcc
from datetime import datetime as dt
import dash_bootstrap_components as dbc
import dash_table as dtable
import datetime as dt
from dash import no_update
from pandas.tseries.offsets import BDay
from flask import request

from sql import (
    pullPosition,
    pullF2Position,
    deletePositions,
    deleteAllPositions,
    pullAllF2Position,
)
from parts import (
    topMenu,
    loadStaticData,
    saveF2Pos,
    loadLiveF2Trades,
    ringTime,
    deletePosRedis,
    deleteRedisPos,
    loadSelectTrades,
    onLoadPortFolio,
)

interval = 1250


def LastBisDay():
    today = dt.datetime.today()
    date = today - BDay(1)
    return date


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
        return "UNKNOWN"


def optionPrompt(product):
    staticData = loadStaticData()
    # staticData = pd.read_json(staticData)
    staticdata = staticData.loc[staticData["product"] == product.upper()]
    staticdata = staticdata["third_wed"].values[0]
    date = staticdata.split("/")
    prompt = date[2] + "-" + date[1] + "-" + date[0]

    return prompt


posColumns = [
    {"name": "Date", "id": "dateTime"},
    {"name": "Instrument", "id": "instrument"},
    {"name": "Quantitiy", "id": "quanitity"},
    {"name": "Prompt", "id": "prompt"},
]

f2Columns = [
    {"name": "Product", "id": "productId"},
    {"name": "Strike", "id": "strike"},
    {"name": "CoP", "id": "optionTypeId"},
    {"name": "Prompt", "id": "prompt"},
    {"name": "Quantitiy", "id": "quanitity"},
    {"name": "Price", "id": "price"},
]

position_table = dbc.Col(
    [
        dtable.DataTable(
            id="position",
            columns=posColumns,
            data=[{}],
            # page_size=10,
            # fixed_rows=[{ 'headers': True, 'data': 0 }],
            style_table={"overflowY": "auto"},
            style_data_conditional=[
                {"if": {"row_index": "odd"}, "backgroundColor": "rgb(248, 248, 248)"}
            ],
        )
    ]
)

f2_table = dbc.Col(
    [
        dtable.DataTable(
            id="f2",
            columns=f2Columns,
            data=[{}],
            page_size=10,
            # fixed_rows=[{ 'headers': True, 'data': 0 }],
            style_data_conditional=[
                {"if": {"row_index": "odd"}, "backgroundColor": "rgb(248, 248, 248)"}
            ],
        )
    ]
)

hidden = html.Div(
    [
        html.Div(id="hidden1-div", style={"display": "none"}),
        html.Div(id="hidden2-div", style={"display": "none"}),
        html.Div(id="hidden3-div", style={"display": "none"}),
        html.Div(id="hidden4-div", style={"display": "none"}),
        html.Div(id="hidden5-div", style={"display": "none"}),
    ],
    className="row",
)

selectors = dbc.Row(
    [
        dbc.Col(
            [dcc.DatePickerSingle(id="position_date", date=dt.date.today())], width=2
        ),
        dbc.Col(
            [dcc.Dropdown(id="product", value="copper", options=onLoadPortFolio())],
            width=2,
        ),
        dbc.Col(
            [html.Button("Copy Select", id="select", style={"background": "#F1C40F"})],
            width=2,
        ),
        dbc.Col(id="modTime", className="four columns"),
        dbc.Col(
            [html.Button("Copy All F2", id="liveF2", style={"background": "#CCD1D1"})],
            width=2,
        ),
        dcc.ConfirmDialog(
            id="confirmLiveF2",
            message="F2 closing positon copied and all todays trades re entered.",
        ),
    ]
)

F2selectors = dbc.Row(
    [
        dbc.Col(
            [dcc.DatePickerSingle(id="F2position_date", date=LastBisDay())], width=2
        ),
        dbc.Col(
            [dcc.Dropdown(id="F2product", value="copper", options=onLoadPortFolio())],
            width=2,
        ),
        dbc.Col(
            [
                html.Button(
                    "Copy F2 Positions", id="copyF2", style={"background": "#fff"}
                )
            ],
            width=2,
        ),
        dcc.ConfirmDialog(
            id="confirm",
            message="This will delete all position and replace them with F2, this action can not be reversed. Do you wish to continue?",
        ),
        dcc.ConfirmDialog(
            id="confirmMove",
            message="F2 position copied",
        ),
    ]
)

layout = html.Div(
    [
        topMenu("Positions"),
        dcc.Interval(id="live-update-portfolio", interval=interval),
        dbc.Row([dbc.Col(["Position"])]),
        selectors,
        position_table,
        dbc.Row([dbc.Col(["F2 Positions"])]),
        F2selectors,
        f2_table,
        # hidden
    ]
)


def initialise_callbacks(app):
    # pulltrades
    @app.callback(
        Output("position", "data"),
        [
            Input("live-update-portfolio", "n_intervals"),
            Input("position_date", "date"),
            Input("product", "value"),
        ],
    )
    def update_trades(interval, date, product):
        product = shortName(str(product))
        dff = pullPosition(product, date)

        return dff.to_dict("records")

    # pull F2 trades
    @app.callback(
        Output("f2", "data"),
        [Input("F2position_date", "date"), Input("F2product", "value")],
    )
    def update_f2Position(date, product):
        product = shortName(product)

        dff = pullF2Position(date, product)
        if not dff.empty:
            dff["price"] = 0
        return dff.to_dict("records")

    # send copy to confrim dialogue
    @app.callback(Output("confirm", "displayed"), [Input("copyF2", "n_clicks")])
    def display_confirm(clicks):
        if clicks:
            return True
        else:
            return no_update

    # copy F2
    @app.callback(
        Output("confirmMove", "displayed"),
        [Input("confirm", "submit_n_clicks")],
        [State("F2product", "value"), State("F2position_date", "date")],
    )
    def update_f2Position(clicks, product, date):
        if clicks:
            deletePosRedis(product.lower())
            product = shortName(product)

            deletePositions(date, product)
            # pull F2 data and fill in price column
            dff = pullF2Position(date, product)
            dff["price"] = 0
            # assign user and send all positons as trades readjusting redis accordingly
            user = request.authorization["username"]
            saveF2Pos(dff, user)

            print("F2 position copied")

            # copy F2 Live pos

    @app.callback(
        Output("confirmLiveF2", "displayed"),
        [Input("liveF2", "n_clicks")],
        [State("F2position_date", "date")],
    )
    def update_Allf2Position(clicks, date):
        if clicks:
            # delte all positon in SQL
            deleteAllPositions()

            # delete all redis pos so that we remove closed up positions
            deleteRedisPos()

            # pull all F2 positons for today
            dff = pullAllF2Position(date)
            # reassign price column
            dff["price"] = 0

            # assign user and send all positons as trades readjusting redis accordingly
            user = request.authorization["username"]
            saveF2Pos(dff, user)

            # load F2 from .csv and send all lines as trades.
            loadLiveF2Trades()

            print("F2 live position copied")

    @app.callback(Output("hidden5-div", "value"), [Input("select", "n_clicks")])
    def update_select(clicks):

        loadSelectTrades()

        print("Select trades copied")

    @app.callback(
        Output("ringPosition", "children"),
        [Input("live-update-portfolio", "n_intervals")],
    )
    def updareRing(interval):
        return ringTime()
