from sql import pullRouteStatus
from parts import topMenu

from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
from dash import dash_table as dtable
from dash import dcc, html
import pandas as pd


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


# load options for dropdowns dynamioally
def onLoadStatusOptions():
    try:
        routeStatus = pullRouteStatus()
    except:
        statusOptions = [{"label": "error", "value": "error"}]
        return statusOptions
    statusOptions = [{"label": "All", "value": "All"}]
    for status in routeStatus.state.unique():
        statusOptions.append({"label": status, "value": status})
    return statusOptions


def onLoadSenderOptions():
    try:
        routeStatus = pullRouteStatus()
    except:
        senderOptions = [{"label": "error", "value": "error"}]
        return senderOptions
    senderOptions = [{"label": "All", "value": "All"}]
    for status in routeStatus.sender.unique():
        senderOptions.append({"label": status, "value": status})
    return senderOptions


def onLoadBrokerOptions():
    try:
        routeStatus = pullRouteStatus()
    except:
        brokerOptions = [{"label": "error", "value": "error"}]
        return brokerOptions
    brokerOptions = [{"label": "All", "value": "All"}]
    for broker in routeStatus.broker.unique():
        brokerOptions.append({"label": broker, "value": broker})
    return brokerOptions


# status dropdown and label
statusLabel = html.Label(
    ["Status:"], style={"font-weight": "bold", "text-align": "left"}
)
statusDropdown = dcc.Dropdown(
    id="status", value="All", options=onLoadStatusOptions(), clearable=False
)

# sender dropdown and label
senderDropdown = dcc.Dropdown(
    id="sender", value="All", options=onLoadSenderOptions(), clearable=False
)
senderLabel = html.Label(
    ["Sender:"], style={"font-weight": "bold", "text-align": "left"}
)

# broker dropdown and label
brokerDropdown = dcc.Dropdown(
    id="broker", value="All", options=onLoadBrokerOptions(), clearable=False
)
brokerLabel = html.Label(
    ["Broker:"], style={"font-weight": "bold", "text-align": "left"}
)


options = (
    dbc.Col(html.Div(children=[statusLabel, statusDropdown]), width=4),
    dbc.Col(html.Div(children=[senderLabel, senderDropdown]), width=4),
    dbc.Col(html.Div(children=[brokerLabel, brokerDropdown]), width=4),
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
        [Input("broker", "value")],
    )
    def update_trades(interval, status, sender, broker):
        # pull all routed trades feedback
        dff = pullRouteStatus()
        # filter for user input
        if status != "All":
            dff = dff[dff["state"].str.contains(status)]

        if sender != "All":
            dff = dff[dff["sender"].str.contains(sender)]

        if broker != "All":
            dff = dff[dff["broker"].str.contains(broker)]

        # convert savedate to datetime
        dff["datetime"] = pd.to_datetime(dff["datetime"])

        dff = dff.sort_values("datetime", ascending=False)

        columns = [{"name": i.capitalize(), "id": i} for i in dff.columns]

        dict = dff.to_dict("records")
        return dict, columns
