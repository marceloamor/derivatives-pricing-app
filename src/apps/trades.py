from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
from dash import dash_table as dtable
import datetime as dt
from dash import no_update, dcc, html
from dash.exceptions import PreventUpdate
import dash_daq as daq
import time, pickle, json
import pandas as pd
import sqlalchemy


# from sql import pulltrades
from parts import topMenu, onLoadPortFolioAll
from data_connections import conn, Session, get_new_postgres_db_engine
from sql import delete_trade

import upestatic

georgia_db2_engine = get_new_postgres_db_engine()

# Inteval time for trades table refresh
interval = 1000 * 3
# column options for trade table
columns = [
    {"name": "Date", "id": "dateTime"},
    {"name": "Instrument", "id": "instrument"},
    {"name": "Price", "id": "price"},
    {"name": "Quantitiy", "id": "quanitity"},
    {"name": "Theo", "id": "theo"},
    {"name": "User", "id": "user"},
    {"name": "Counterparty", "id": "counterPart"},
    {"name": "Prompt", "id": "prompt"},
    {"name": "Venue", "id": "venue"},
    {"name": "Deleted", "id": "deleted"},
]


def timeStamp():
    now = dt.datetime.now()
    now.strftime("%Y-%m-%d %H:%M:%S")
    return now


def convertTimestampToSQLDateTime(value):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(value))


def shortName(product):
    if product == None:
        return "all"
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
    elif product.lower() == "xext-ebm-eur":
        return "XEX"
    else:
        return "all"


# date picker
dateLabel = html.Label(["Date:"], style={"font-weight": "bold", "text-align": "left"})
datePicker = dcc.DatePickerSingle(
    id="date-picker",
    date=dt.date.today(),
    display_format="DD/MM/YYYY",
)

# if you're here to make product dropdown dynamic again, you're in the right place
# change the below lines
options = onLoadPortFolioAll()
options.append({"label": "Milling Wheat", "value": "xext-ebm-eur"})

# product dropdown
productLabel = html.Label(
    ["Product:"], style={"font-weight": "bold", "text-align": "left"}
)
productDropdown = dcc.Dropdown(id="product", value="all", options=options)


# venue dropdown
def onLoadVenueOptions():
    data = conn.get("trades")
    venueOptions = [{"label": "All", "value": "all"}]
    if data:
        dff = pickle.loads(data)
        for venue in dff.venue.unique():
            venueOptions.append({"label": venue, "value": venue})
    return venueOptions


venueDropdown = dcc.Dropdown(
    id="venue", value="all", options=onLoadVenueOptions(), clearable=False
)
venueLabel = html.Label(["Venue:"], style={"font-weight": "bold", "text-align": "left"})


def onLoadCounterpartOptions():
    data = conn.get("trades")
    counterpartOptions = [
        {"label": " All", "value": "all"}
    ]  # space in front of All to make it first in list
    if data:
        dff: pd.DataFrame = pickle.loads(data)
        dff.columns = dff.columns.str.lower()
        for counterpart in dff.loc[:, "counterpart"].unique():
            counterpartOptions.append({"label": counterpart, "value": counterpart})
        sorted_counterpartOptions = sorted(
            counterpartOptions, key=lambda k: k["label"]
        )  # sort alphabetically
    return sorted_counterpartOptions


counterpartDropdown = dcc.Dropdown(
    id="counterpart", value="all", options=onLoadCounterpartOptions(), clearable=False
)
counterpartLabel = html.Label(
    ["Counterpart:"], style={"font-weight": "bold", "text-align": "left"}
)

# deleted trades boolean switch
deletedLabel = html.Label(
    ["Deleted:"], style={"font-weight": "bold", "text-align": "center"}
)
deletedSwitch = daq.BooleanSwitch(id="deleted", on=False)

options = (
    dbc.Col(html.Div(children=[dateLabel, datePicker])),
    dbc.Col(html.Div(children=[productLabel, productDropdown])),
    dbc.Col(html.Div(children=[venueLabel, venueDropdown])),
    dbc.Col(html.Div(children=[counterpartLabel, counterpartDropdown])),
    dbc.Col(html.Div(children=[deletedLabel, deletedSwitch])),
)


tables = dbc.Row(
    [
        dbc.Col(
            [
                dtable.DataTable(
                    id="tradesTable1",
                    columns=columns,
                    data=[{}],
                    row_deletable=True,
                    # fixed_rows=[{'headers': True, 'data': 0 }],
                    style_data_conditional=[
                        {
                            "if": {"row_index": "odd"},
                            "backgroundColor": "rgb(137, 186, 240)",
                        }
                    ],
                )
            ]
        )
    ]
)

layout = html.Div(
    [
        topMenu("Trades"),
        # interval HTML
        dcc.Interval(id="trades-update", interval=interval),
        dbc.Row(options),
        tables,
        html.Div(id="output"),
    ]
)


def initialise_callbacks(app):
    # pulltrades use hidden inputs to trigger update on new trade
    @app.callback(
        [
            Output("tradesTable1", "data"),
            Output("tradesTable1", "columns"),
            Output("tradesTable1", "row_deletable"),
        ],
        [
            Input("date-picker", "date"),
            Input("trades-update", "n_intervals"),
            Input("product", "value"),
            Input("venue", "value"),
            Input("counterpart", "value"),
            Input("deleted", "on"),
        ],
    )
    def update_trades(date, interval, product, venue, counterpart, deleted):
        if product:
            # convert date into datetime
            date = dt.datetime.strptime(date, "%Y-%m-%d")

            # pull trades on data
            data = conn.get("trades")

            if data:
                dff = pickle.loads(data)

                # convert columsn to lower case
                dff.columns = dff.columns.str.lower()

                dff.deleted = dff.deleted.astype(bool)

                # filter for date and deleted
                dff = dff[dff["datetime"] >= date]
                dff = dff[dff["deleted"] == bool(deleted)]

                # create columns for end table
                columns = [{"name": i.capitalize(), "id": i} for i in dff.columns]

                product = shortName(product)
                # filter for product
                if product != "all":
                    dff = dff[dff["instrument"].str.contains(product)]

                # filter for venue
                if venue != "all":
                    dff = dff[dff["venue"] == venue]

                # filter for counterpart
                if counterpart != "all":
                    dff = dff[dff["counterpart"] == counterpart]

                # sort data, swapped from index to datetime. sort by id also an option
                # dff.sort_index(inplace=True, ascending=True)
                dff.sort_values(by=["datetime"], inplace=True, ascending=False)

                dict = dff.to_dict("records")

                if deleted:
                    delete_rows = False
                else:
                    delete_rows = True

                return dict, columns, delete_rows
            else:
                no_update, no_update, no_update
        else:
            no_update, no_update, no_update

    @app.callback(
        Output("trades-update", "n_intervals"),
        [Input("tradesTable1", "data_previous")],
        [State("tradesTable1", "data")],
    )
    def show_removed_rows(previous, current):
        if previous is None:
            PreventUpdate()
        else:
            diff = [row for row in previous if row not in current]
            id = diff[0]["id"]

            # delete trade in legacy trade table
            delete_trade(id)

            # delete trade in new trade table as well by venue and venue id
            venue = diff[0]["venue"]
            venue_trade_id = diff[0]["venue_trade_id"]

            update_params = [{"venue": venue, "venue_trade_id": venue_trade_id}]

            # update when db-prod becomes ORM compatible
            with georgia_db2_engine.connect() as db_conn:
                stmt = sqlalchemy.text(
                    "UPDATE trades SET deleted = true WHERE venue = :venue AND venue_trade_id = :venue_trade_id"
                )
                db_conn.execute(stmt, params=update_params)
            return 1
