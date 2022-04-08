from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_table as dtable
import pandas as pd
import datetime as dt
import time, json

from sql import pullRouteStatus

from parts import topMenu

# column options for trade table
columns = [
    {"name": "Date", "id": "saveddate"},
    {"name": "Product", "id": "product"},
    {"name": "Strike", "id": "strike"},
    {"name": "CoP", "id": "optionType"},
    {"name": "Price", "id": "price"},
    {"name": "Quanitity", "id": "quanitity"},
    {"name": "Counterparty", "id": "counterparty"},
    {"name": "User", "id": "user"},
    {"name": "Status", "id": "status"},
    {"name": "Message", "id": "message"},
]

optionsDropdown = [
    {"label": "All", "value": "All"},
    {"label": "Routed", "value": "Routed"},
    {"label": "Delivery Failure", "value": "DeliveryFailure"},
    {"label": "InvalidFixml", "value": "InvalidFixml"},
]

options = dbc.Col(
    [dcc.Dropdown(id="message", value="All", options=optionsDropdown)], width=3
)

table = dbc.Col(
    [
        dtable.DataTable(
            id="statusTable",
            columns=columns,
            data=[{}],
            fixed_rows=[{"headers": True, "data": 0}],
            style_data_conditional=[
                {"if": {"row_index": "odd"}, "backgroundColor": "rgb(248, 248, 248)"}
            ],
        )
    ]
)

layout = html.Div(
    [
        topMenu("Route Status"),
        dbc.Row(options),
        dbc.Row(table),
    ]
)


def initialise_callbacks(app):
    # pulltrades use hiddien inputs to trigger update on new trade
    @app.callback(Output("statusTable", "data"), [Input("message", "value")])
    def update_trades(selector):
        # pull all routed trades feedback
        dff = pullRouteStatus()
        # filter for user input
        if selector != "All":
            dff = dff[dff["status"].str.contains(selector)]

        # convert savedate to datetime
        dff["saveddate"] = pd.to_datetime(dff["saveddate"])

        dff = dff.sort_values("saveddate", ascending=False)

        dict = dff.to_dict("records")
        return dict
