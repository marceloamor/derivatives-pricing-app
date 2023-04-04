from dash.dependencies import Input, Output, State
from dash import dcc, html
from dash import dcc
from datetime import datetime as dt
import dash_bootstrap_components as dbc
from dash import dash_table as dtable
import datetime as dt
from dash import no_update
from pandas.tseries.offsets import BDay
from flask import request

from sql import (
    pullPosition,
)
from parts import (
    topMenu,
    loadStaticData,
    ringTime,
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
    elif product.lower() == "xext-ebm-eur": # change this when more euronext products are added 
        return "XEX"
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
]

position_table = dbc.Col(
    [
        dtable.DataTable(
            id="position",
            columns=posColumns,
            data=[{}],
            filter_action="native",
            sort_action="native",
            sort_mode="multi",
            # page_size=10,
            # fixed_rows=[{ 'headers': True, 'data': 0 }],
            style_table={"overflowY": "auto"},
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

options = onLoadPortFolio()
options.append({"label": "Milling Wheat", "value": "xext-ebm-eur"})

# dropdown and label
productDropdown = dcc.Dropdown(id="product", value="copper", options=options)
productLabel = html.Label(
    ["Product:"], style={"font-weight": "bold", "text-align": "left"}
)


selectors = dbc.Row(
    [
        dbc.Col(
            [productLabel, productDropdown],
            width=3,
        ),
    ]
)


layout = html.Div(
    [
        topMenu("Positions"),
        dcc.Interval(id="live-update-portfolio", interval=interval),
        # dbc.Row([dbc.Col(["Position"])]),
        selectors,
        position_table,
    ]
)


def initialise_callbacks(app):
    # pulltrades
    @app.callback(
        Output("position", "data"),
        [
            Input("live-update-portfolio", "n_intervals"),
            Input("product", "value"),
        ],
    )
    def update_trades(interval, product):
        product = shortName(str(product))
        dff = pullPosition(product, dt.datetime.today())

        return dff.to_dict("records")

    # send copy to confrim dialogue
    @app.callback(Output("confirm", "displayed"), [Input("copyF2", "n_clicks")])
    def display_confirm(clicks):
        if clicks:
            return True
        else:
            return no_update

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
