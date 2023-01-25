from dash.dependencies import Input, Output, State
from dash import dcc, html
from dash import dcc
import dash_bootstrap_components as dbc
from dash import dash_table as dtable
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

# status dropdown options:
statusOptions = [
    {"label": "All", "value": "All"},
    {"label": "Processing", "value": "PROCESSING"},
    {"label": "Routed", "value": "ROUTED"},
    {"label": "Delivery Failure", "value": "FAILED"},
    {"label": "Unsent", "value": "UNSENT"},
]
statusLabel = html.Label(
    ["Status:"], style={"font-weight": "bold", "text-align": "left"}
)
statusDropdown = dcc.Dropdown(
    id="status", value="All", options=statusOptions, clearable=False
)

# sender dropdown options:
senderOptions = [
    {"label": "All", "value": "All"},
    {"label": "PME", "value": "PME"},
    {"label": "CME", "value": "CME"},
    {"label": "Sol3", "value": "Sol3"},
    {"label": "Gareth", "value": "gareth@upetrading.com"},
    {"label": "Tom", "value": "thomas.beever@upetrading.com"},
]
senderDropdown = dcc.Dropdown(
    id="sender", value="All", options=senderOptions, clearable=False
)
senderLabel = html.Label(
    ["Sender:"], style={"font-weight": "bold", "text-align": "left"}
)


options = (
    dbc.Col(html.Div(children=[statusLabel, statusDropdown])),
    dbc.Col(html.Div(children=[senderLabel, senderDropdown])),
)


table = dbc.Col(
    [
        dtable.DataTable(
            id="statusTable",
            columns=columns,
            data=[{}],
            # fixed_rows=[{"headers": True, "data": 0}],
            style_data_conditional=[
                {"if": {"row_index": "odd"}, "backgroundColor": "rgb(248, 248, 248)"},
                {
                    "if": {
                        "filter_query": "{{state}} = {}".format("FAILED"),
                    },
                    "backgroundColor": "#FF4136",
                    "color": "white",
                },
            ],
        )
    ]
)

layout = html.Div(
    [
        topMenu("Route Status"),
        dcc.Interval(
            id="live-update", interval=10 * 1000, n_intervals=0  # 10 second refresh
        ),
        dbc.Row(options),
        dbc.Row(table),
    ]
)


def initialise_callbacks(app):
    # pulltrades use hidden inputs to trigger update on new trade
    @app.callback(
        [Output("statusTable", "data"), Output("statusTable", "columns")],
        [Input("live-update", "n_intervals")],
        [Input("status", "value")],
        [Input("sender", "value")],
    )
    def update_trades(interval, status, sender):
        # pull all routed trades feedback
        dff = pullRouteStatus()
        # filter for user input
        if status != "All":
            dff = dff[dff["state"].str.contains(status)]

        if sender != "All":
            dff = dff[dff["sender"].str.contains(sender)]

        # convert savedate to datetime
        dff["datetime"] = pd.to_datetime(dff["datetime"])

        dff = dff.sort_values("datetime", ascending=False)

        columns = [{"name": i.capitalize(), "id": i} for i in dff.columns]

        dict = dff.to_dict("records")
        return dict, columns
