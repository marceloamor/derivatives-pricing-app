from dash.dependencies import Input, Output, State
import dash
from dash import dcc, html
from dash import dcc
from datetime import datetime as dt
import dash_bootstrap_components as dbc
from dash import dash_table as dtable
import pandas as pd
import datetime as dt
import json
import time

from parts import (
    topMenu,
    strikePnlTable,
    productPnlTable,
    pullPnl,
    PortfolioPnlTable,
    loadStaticData,
)

interval = str(3 * 1000)


def onLoadPortFolio():
    staticData = loadStaticData()
    staticData = pd.read_json(staticData)
    portfolios = []
    for portfolio in staticData.portfolio.unique():
        portfolios.append({"label": portfolio, "value": portfolio})
    return portfolios


columns = [
    {"name": "Portfolio", "id": "Portfolio"},
    {"name": "Trade", "id": "Trade Pnl"},
    {"name": "Position", "id": "Position PNL"},
    {"name": "Total", "id": "Total PNL"},
]

productColumns = [
    {"name": "Product", "id": "Product"},
    {"name": "Trade", "id": "Trade Pnl"},
    {"name": "Position", "id": "Position PNL"},
    {"name": "Total", "id": "Total PNL"},
]

pnl_tables = html.Div(
    [
        dbc.Row(
            [
                dbc.Col(
                    [
                        dtable.DataTable(
                            id="portfolioPnl",
                            data=[{}],
                            columns=columns,
                            row_selectable="single",
                        )
                    ],
                    width=6,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        dtable.DataTable(
                            id="productPnl",
                            data=[{}],
                            columns=productColumns,
                            row_selectable="single",
                        )
                    ],
                    width=6,
                )
            ]
        ),
        dbc.Row([dtable.DataTable(id="strikePnl", data=[{}], columns=productColumns)]),
    ]
)

layout = html.Div(
    [
        topMenu("PNL"),
        dcc.Interval(id="live-updatePNL", interval=interval),
        pnl_tables,
        dcc.Store(id="pnlData"),
    ]
)


def initialise_callbacks(app):
    # pull pnl
    @app.callback(Output("pnlData", "data"), [Input("live-updatePNL", "n_intervals")])
    def pullPnlData(interval):
        data = pullPnl()
        data = json.loads(data)
        return data

    # portfolio pnl
    @app.callback(Output("portfolioPnl", "data"), [Input("pnlData", "data")])
    def portfolioPnlData(data):
        if data:
            data = PortfolioPnlTable(data)
            return data

    # product pnl
    @app.callback(
        Output("productPnl", "data"),
        [
            Input("pnlData", "data"),
            Input("portfolioPnl", "data"),
            Input("portfolioPnl", "selected_rows"),
        ],
    )
    def productPnlData(data, rows, selected_row_indices):
        if selected_row_indices and selected_row_indices != None:
            selected_rows = [rows[i] for i in selected_row_indices][0]
            portfolio = selected_rows["Portfolio"]

            if portfolio != "Total":
                tableData = productPnlTable(data, portfolio.lower())
                return tableData

    # strike PNL
    @app.callback(
        Output("strikePnl", "data"),
        [
            Input("pnlData", "data"),
            Input("productPnl", "data"),
            Input("productPnl", "selected_rows"),
        ],
        [State("portfolioPnl", "data"), State("portfolioPnl", "selected_rows")],
    )
    def strikePnlData(data, rows, selected_row_indices, portRows, portIndices):
        if selected_row_indices and portIndices:
            portselected_rows = [portRows[i] for i in portIndices][0]
            portfolio = portselected_rows["Portfolio"]
            selected_rows = [rows[i] for i in selected_row_indices][0]
            product = selected_rows["Product"]
            data = strikePnlTable(data, portfolio.lower(), product)

            return data
