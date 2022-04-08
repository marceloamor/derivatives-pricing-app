from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_core_components as dcc
from dash import no_update
from datetime import datetime as dt
import dash_bootstrap_components as dbc
from numpy import True_
import pandas as pd
import datetime as dt
import os, pickle
import dash_table as dtable

from parts import topMenu
from data_connections import PostGresEngine, conn, Connection, call_function


def broker_data():
    try:
        cnxn = Connection("Sucden-sql-soft", "LME")
        sql = "SELECT * FROM public.get_brokers()"
        df = pd.read_sql(sql, cnxn)
        columns = [{"name": i.capitalize(), "id": i} for i in df.columns]

        return df.to_dict("records"), columns
    except Exception as e:
        print("Missing broker details")
        return [{}], [{"name": "Error", "id": "error"}]


form = dbc.Row(
    [
        dbc.Col(
            [
                dbc.Form(
                    [
                        # ['codename', 'firm', 'contact', 'contact_email', 'clearing_email', 'clearing_code', 'cat_member', 'bro_rate']
                        dbc.FormGroup(
                            [
                                dbc.Label("Code Name", className="mr-2"),
                                dbc.Input(
                                    type="text",
                                    id="codename",
                                    placeholder="For dropdown",
                                ),
                                dbc.Label("Firm", className="mr-2"),
                                dbc.Input(
                                    type="text", id="firm", placeholder="Firm name"
                                ),
                                dbc.Label("Contact", className="mr-2"),
                                dbc.Input(
                                    type="text",
                                    id="contact",
                                    placeholder="Main contact",
                                ),
                                dbc.Label("E-Mail", className="mr-2"),
                                dbc.Input(
                                    type="text",
                                    id="contact_email",
                                    placeholder="Main Email",
                                ),
                                dbc.Label("Clearing E-Mail", className="mr-2"),
                                dbc.Input(
                                    type="text",
                                    id="clearing_email",
                                    placeholder="Clearing Email",
                                ),
                                dbc.Label("Clearing Code", className="mr-2"),
                                dbc.Input(
                                    type="text",
                                    id="clearing_code",
                                    placeholder="Code for crossing",
                                ),
                                dbc.Label("Cat Member", className="mr-2"),
                                dbc.Input(
                                    type="text",
                                    id="cat_member",
                                    placeholder="Code for crossing",
                                ),
                                dbc.Label("Agreed Rate", className="mr-2"),
                                dbc.Input(
                                    type="numeric", id="bro_rate", placeholder=0.25
                                ),
                            ],
                            className="mr-3",
                        )
                    ],
                    # inline=True,
                )
            ],
            width=6,
        ),
    ]
)

layout = html.Div(
    [
        topMenu("Brokers"),
        # output table
        dbc.Row(
            [
                dbc.Col(
                    [
                        dtable.DataTable(
                            id="broker_table",
                            data=broker_data()[0],
                            columns=broker_data()[1],
                            editable=True,
                            row_deletable=True,
                        )
                    ],
                    width=12,
                )
            ]
        ),
        html.Div(id="table_output", children=["Test"]),
        dbc.Button("Refresh", id="broker_refresh", n_clicks_timestamp="0", active=True),
        dbc.Button("Update", id="broker_update", n_clicks_timestamp="0", active=True),
    ]
)


def initialise_callbacks(app):
    @app.callback(Output("broker_table", "data"), [Input("broker_refresh", "n_clicks")])
    def show_removed_rows(click):
        return broker_data()[0]

    @app.callback(
        Output("broker_refresh", "n_clicks_timestamp"),
        [Input("broker_table", "data_previous"), Input("broker_table", "data")],
    )
    def show_removed_rows(previous, current):
        print(previous)
        if previous is None:
            return no_update
        else:
            for row in previous:
                if row not in current:
                    call_function("delete_broker", row["codename"])
                    print("Just removed {row}".format(row=row["codename"]))
                    return ["Just removed {row}".format(row=row["codename"])]

    @app.callback(
        Output("table_output", "children"),
        [Input("broker_update", "n_clicks")],
        [State("broker_table", "data_previous"), State("broker_table", "data")],
    )
    def show_removed_rows(click, previous, current):
        if previous and current:
            previous = dict(previous[0])
            current = dict(current[0])

            set1 = set(previous.items())
            set2 = set(current.items())

            diffs = set1 ^ set2
