from dash.dependencies import Input, Output, State
from dash import dcc
from dash import dcc, html
from dash import dash_table as dtable
import pandas as pd
import dash_bootstrap_components as dbc


from parts import (
    topMenu
    )

import sftp_utils

import traceback


# layout for dataload page
layout = html.Div(
    [
        topMenu("Cash Manager"),
        html.Div(dbc.Button("refresh", id="refresh-button", n_clicks=0)),
        html.Div(id="rjo-filename", children="RJO filename: "),
        html.Div(id="output-rec-button1"),
    ]
)



def initialise_callbacks(app):
    # sol3 and rjo pos rec on button click
    @app.callback(
        Output("output-rec-button1", "children"),
        Output("rjo-filename", "children"),
        [Input("refresh-button", "n_clicks")],
    )
    def sol3_rjo_rec_button(n):
        # on click do this
        filenames = html.Div()
        table = html.Div()
        if n >= 0:
            # get latest sol3 and rjo pos exports
            rjo_cash = sftp_utils.fetch_latest_rjo_export("UPETRADING_csvnmny_nmny_%Y%m%d.csv")

            latest_rjo_df = rjo_cash[0].T.reset_index()
            latest_rjo_filename = rjo_cash[1]

            cash_table = dtable.DataTable(
                data=latest_rjo_df.to_dict("records"),
                columns=[
                    {"name": str(col_name), "id": str(col_name)} for col_name in latest_rjo_df.columns
                ], style_data_conditional=[
                    {'if': {
                        'column_id': 'index',
                    },
                    'backgroundColor': 'lightgrey',
                }
                ],
                style_header={
                    'if': {
                        'column_id': 'index',
                    },
                    'backgroundColor': 'grey',

                }
            )
            filename_string = (
                "RJO filename: "
                + latest_rjo_filename
            )
            return cash_table, filename_string

        return table, filenames
