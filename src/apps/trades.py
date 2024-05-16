import datetime as dt
import pickle
import time

import dash_bootstrap_components as dbc
import dash_daq as daq
import display_names
import pandas as pd
import sqlalchemy
from dash import dash_table as dtable
from dash import dcc, html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from data_connections import PostGresEngine, conn, shared_engine, shared_session
from parts import loadPortfolios, loadProducts, topMenu
from upedata import dynamic_data as upe_dynamic
from upedata import static_data as upe_static

# Inteval time for trades table refresh
interval = 1000 * 5
# column options for trade table
columns = [
    {"name": "ID", "id": "trade_pk"},
    {"name": "Datetime", "id": "trade_datetime_utc"},
    # {"name": "Instrument", "id": "instrument_symbol"},
    {"name": "Display Name", "id": "instrument_display_name"},
    {"name": "Price", "id": "price"},
    {"name": "Quantity", "id": "quantity"},
    {"name": "Trader", "id": "full_name"},
    {"name": "Counterparty", "id": "counterparty"},
    {"name": "Portfolio", "id": "display_name"},  # joined on portfolio_id
    {"name": "Notes", "id": "notes"},
    # {"name": "Deleted", "id": "deleted"},
    {"name": "Venue", "id": "venue_name"},
    # {"name": "Venue Trade ID", "id": "venue_trade_id"},
    # {"name": "Routing Status ID", "id": "routing_status_id"},
]


def timeStamp():
    now = dt.datetime.now()
    now.strftime("%Y-%m-%d %H:%M:%S")
    return now


def convertTimestampToSQLDateTime(value):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(value))


# date picker
dateLabel = html.Label(["Date:"], style={"font-weight": "bold", "text-align": "left"})
datePicker = dcc.DatePickerSingle(
    id="date-picker",
    date=dt.date.today(),
    display_format="DD/MM/YYYY",
)

# if you're here to make product dropdown dynamic again, you're in the right place
# change the below lines
options = [{"label": " All", "value": "all"}] + loadProducts()

# product dropdown
productLabel = html.Label(
    ["Product:"], style={"font-weight": "bold", "text-align": "left"}
)
productDropdown = dcc.Dropdown(id="product", value="all", options=options)


# venue dropdown
def onLoadVenueOptions_Old():
    data = conn.get("trades")
    venueOptions = [{"label": "All", "value": "all"}]
    if data:
        dff = pickle.loads(data)
        for venue in dff.venue.unique():
            venueOptions.append({"label": venue, "value": venue})
    return venueOptions


def onLoadVenueOptions():
    dropdown_options = [{"label": "All", "value": "all"}]
    with shared_session() as session:
        stmt = sqlalchemy.select(upe_dynamic.Trade.venue_name).distinct()
        result = session.execute(stmt).fetchall()

    for row in result:
        venue = row[0]
        if venue != "TEST":
            dropdown_options.append({"label": venue, "value": venue})

    return dropdown_options


venueDropdown = dcc.Dropdown(
    id="venue", value="all", options=onLoadVenueOptions(), clearable=False
)
venueLabel = html.Label(["Venue:"], style={"font-weight": "bold", "text-align": "left"})


def onLoadCounterpartOptions_Old():
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


def onLoadCounterpartOptions():
    dropdown_options = [
        {"label": " All", "value": "all"},
        {"label": "CQG", "value": "CQG"},
        {"label": "SELECT", "value": "SELECT"},
    ]
    with shared_session() as session:
        stmt = sqlalchemy.select(upe_static.CounterpartyClearer.counterparty).distinct()
        result = session.execute(stmt).fetchall()

    for row in result:
        counterparty = row[0]
        if counterparty != "TEST":
            dropdown_options.append({"label": counterparty, "value": counterparty})
    dropdown_options = sorted(
        dropdown_options, key=lambda k: k["label"]
    )  # sort alphabetically

    return dropdown_options


counterpartDropdown = dcc.Dropdown(
    id="counterpart",
    value="all",
    options=onLoadCounterpartOptions(),
    clearable=False,
)
counterpartLabel = html.Label(
    ["Counterpart:"], style={"font-weight": "bold", "text-align": "left"}
)

# portfolio
portfolioDropdown = dcc.Dropdown(
    id="portfolio-trades",
    value="all",
    options=loadPortfolios(),
    clearable=False,
)
portfolioLabel = html.Label(
    ["Portfolio:"], style={"font-weight": "bold", "text-align": "left"}
)

# deleted trades boolean switch
deletedLabel = html.Label(
    ["Deleted:"], style={"font-weight": "bold", "text-align": "center"}
)
deletedSwitch = daq.BooleanSwitch(id="deleted", on=False)

options = (
    dbc.Col(html.Div(children=[dateLabel, datePicker]), width=2),
    dbc.Col(html.Div(children=[productLabel, productDropdown]), width=2),
    dbc.Col(html.Div(children=[venueLabel, venueDropdown]), width=2),
    dbc.Col(html.Div(children=[counterpartLabel, counterpartDropdown]), width=2),
    dbc.Col(html.Div(children=[portfolioLabel, portfolioDropdown]), width=2),
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
            # Output("tradesTable1", "columns"),
            Output("tradesTable1", "row_deletable"),
        ],
        [
            Input("date-picker", "date"),
            Input("trades-update", "n_intervals"),
            Input("product", "value"),
            Input("venue", "value"),
            Input("counterpart", "value"),
            Input("portfolio-trades", "value"),
            Input("deleted", "on"),
        ],
    )
    def update_trades(date, interval, product, venue, counterpart, portfolio, deleted):
        if product:
            # convert date into datetime
            date = dt.datetime.strptime(date, "%Y-%m-%d")

            # this new database call is a crude temporary solution!
            # either switch back to redis -- or split callback so you only pull from postgres on date change
            # all other inputs filter the data in a separate callback
            stmt = (
                sqlalchemy.select(
                    upe_dynamic.Trade,
                    upe_static.Trader.full_name,
                    upe_static.Portfolio.display_name,
                )
                .join(
                    upe_static.Trader,
                    upe_dynamic.Trade.trader_id == upe_static.Trader.trader_id,
                )
                .join(
                    upe_static.Portfolio,
                    upe_dynamic.Trade.portfolio_id == upe_static.Portfolio.portfolio_id,
                )
                .filter(upe_dynamic.Trade.trade_datetime_utc >= date)
            )
            df = pd.read_sql(stmt, shared_engine)

            if df is not None:
                # filter for deleted
                df = df[df["deleted"] == bool(deleted)]

                if product != "all":
                    df = df[df["instrument_symbol"].str.contains(product)]

                # filter for venue
                if venue != "all":
                    df = df[df["venue_name"] == venue]

                # filter for counterpart
                if counterpart != "all":
                    df = df[df["counterparty"] == counterpart]

                # filter for portfolio
                if portfolio != "all":
                    df = df[df["portfolio_id"] == portfolio]

                df.sort_index(inplace=True, ascending=True)
                df.sort_values(by=["trade_datetime_utc"], inplace=True, ascending=False)
                if len(df) > 0:
                    df["instrument_display_name"] = (
                        display_names.map_symbols_to_display_names(
                            df["instrument_symbol"].to_list()
                        )
                    )
                    df["instrument_display_name"] = df[
                        "instrument_display_name"
                    ].str.upper()
                else:
                    df["instrument_display_name"] = pd.Series()
                dict = df.to_dict("records")

                if deleted:
                    delete_rows = False
                else:
                    delete_rows = True

                return dict, delete_rows

        ##############################################################################
        # uncomment if going back to the redis standard instead of the db calls

    # # pulltrades use hidden inputs to trigger update on new trade
    # @app.callback(
    #     [
    #         Output("tradesTable1", "data"),
    #         Output("tradesTable1", "columns"),
    #         Output("tradesTable1", "row_deletable"),
    #     ],
    #     [
    #         Input("date-picker", "date"),
    #         Input("trades-update", "n_intervals"),
    #         Input("product", "value"),
    #         Input("venue", "value"),
    #         Input("counterpart", "value"),
    #         Input("deleted", "on"),
    #     ],
    # )
    # def update_trades(date, interval, product, venue, counterpart, deleted):
    #     if product:
    #         # convert date into datetime
    #         date = dt.datetime.strptime(date, "%Y-%m-%d")

    #         # pull trades on data
    #         data = conn.get("trades")

    #         if data:
    #             dff = pickle.loads(data)

    #             # convert columsn to lower case
    #             dff.columns = dff.columns.str.lower()

    #             dff.deleted = dff.deleted.astype(bool)

    #             # filter for date and deleted
    #             dff = dff[dff["datetime"] >= date]
    #             dff = dff[dff["deleted"] == bool(deleted)]

    #             # create columns for end table
    #             columns = [{"name": i.capitalize(), "id": i} for i in dff.columns]

    #             product = shortName(product)
    #             # filter for product
    #             if product != "all":
    #                 dff = dff[dff["instrument"].str.contains(product)]

    #             # filter for venue
    #             if venue != "all":
    #                 dff = dff[dff["venue"] == venue]

    #             # filter for counterpart
    #             if counterpart != "all":
    #                 dff = dff[dff["counterpart"] == counterpart]

    #             # sort data, swapped from index to datetime. sort by id also an option
    #             # dff.sort_index(inplace=True, ascending=True)
    #             dff.sort_values(by=["datetime"], inplace=True, ascending=False)

    #             dict = dff.to_dict("records")

    #             if deleted:
    #                 delete_rows = False
    #             else:
    #                 delete_rows = True

    #             return dict, columns, delete_rows
    #         else:
    #             no_update, no_update, no_update
    #     else:
    #         no_update, no_update, no_update

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

            # delete trade in new trade table as well by venue and venue id
            venue = diff[0]["venue_name"]
            venue_trade_id = diff[0]["venue_trade_id"]

            # delete trade in legacy trade table
            delete_trade(venue, venue_trade_id)

            # update when db-prod becomes ORM compatible
            with shared_session() as session:
                stmt = sqlalchemy.text(
                    "UPDATE trades SET deleted = true WHERE venue_name = :venue AND venue_trade_id = :venue_trade_id"
                )
                session.execute(
                    stmt, params={"venue": venue, "venue_trade_id": venue_trade_id}
                )
                session.commit()
            return 1


# currently used for backwards compatibility with old trades table -- change to sqlalchemy
def delete_trade(venue, venue_trade_id):
    # connect to the database using PostGresEngine()
    with sqlalchemy.orm.Session(PostGresEngine()) as cnxn:
        # changed from calling delete_trade(id) psql function to manual update after it broke
        # change to sqlalchemy text/paramterized query after georgia update
        ##############
        sql = sqlalchemy.text(
            'SELECT quanitity FROM public.trades WHERE "venue" = :venue AND "venue_trade_id" = :venue_trade_id'
        )
        qty = cnxn.execute(
            sql, params={"venue": venue, "venue_trade_id": venue_trade_id}
        ).fetchone()[0]

        if qty is not None:
            # Update query for 'public.trades' table
            sql1 = sqlalchemy.text(
                'UPDATE public.trades SET deleted = 1 WHERE "venue" = :venue AND "venue_trade_id" = :venue_trade_id'
            )
            cnxn.execute(
                sql1, params={"venue": venue, "venue_trade_id": venue_trade_id}
            )

            # Update query for 'public.positions' table
            sql2 = sqlalchemy.text(
                """
                UPDATE public.positions
                SET quanitity = quanitity - :qty
                WHERE instrument = (SELECT instrument FROM public.trades WHERE "venue" = :venue AND "venue_trade_id" = :venue_trade_id)
            """
            )
            cnxn.execute(
                sql2,
                params={"qty": qty, "venue": venue, "venue_trade_id": venue_trade_id},
            )
            cnxn.commit()

    # # update trades in redis
    # trades = pd.read_sql("trades", PostGresEngine())
    # trades.columns = trades.columns.str.lower()
    # pick_trades = pickle.dumps(trades, protocol=-1)
    # conn.set("trades" + dev_key_redis_append, pick_trades)

    # # update pos in redis from postgres.
    # pos = pd.read_sql("positions", PostGresEngine())
    # pos.columns = pos.columns.str.lower()
    # pos = pickle.dumps(pos)
    # conn.set("positions" + dev_key_redis_append, pos)
