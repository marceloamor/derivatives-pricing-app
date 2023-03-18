from dash.dependencies import Input, Output, State
from dash import dcc
from dash import dcc, html
from dash import dash_table as dtable
import pandas as pd
import dash_bootstrap_components as dbc


from parts import topMenu

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
    # cash manager page
    @app.callback(
        Output("output-rec-button1", "children"),
        Output("rjo-filename", "children"),
        [Input("refresh-button", "n_clicks")],
    )
    def cashManager(n):
        # on click do this
        filenames = html.Div()
        table = html.Div()
        if n >= 0:
            # get latest sol3 and rjo pos exports
            (latest_rjo_df, latest_rjo_filename) = sftp_utils.fetch_latest_rjo_export(
                "UPETRADING_csvnmny_nmny_%Y%m%d.csv"
            )

            latest_rjo_df = latest_rjo_df.reset_index()
            latest_rjo_df = latest_rjo_df[latest_rjo_df["Record Code"] == "M"]
            latest_rjo_df = latest_rjo_df.T.reset_index()

            columns_to_keep = [
                "Account Number",
                "Account Type Currency Symbol",
                "Last Activity Date",
                "Account Balance",
                "Future Margin Req Initial",
                "Future Margin Req Maint",
                "Withdrawable Funds",
                "Liquidating Value",
                "Total Equity",
                "Previous Total Equity",
                "Previous Liquidating Value",
                "Record Code",
                "Total Account Requirement",
            ]
            latest_rjo_df = latest_rjo_df[latest_rjo_df["index"].isin(columns_to_keep)]

            # set index to orginal index
            latest_rjo_df.set_index("index", inplace=True)
            # print(latest_rjo_df)

            # # add pnl row
            # latest_rjo_df.loc["PNL"] = (
            #     latest_rjo_df.loc[["Liquidating Value"]]
            #     - latest_rjo_df.loc[["Previous Liquidating Value"]]
            # )

            cash_table = dtable.DataTable(
                data=latest_rjo_df.to_dict("records"),
                columns=[
                    {"name": str(col_name), "id": str(col_name)}
                    for col_name in latest_rjo_df.columns
                ],
                style_data_conditional=[
                    {
                        "if": {
                            "column_id": "index",
                        },
                        "backgroundColor": "lightgrey",
                    }
                ],
                style_header={
                    "display": "none",
                },
            )
            filename_string = "RJO filename: " + latest_rjo_filename
            return cash_table, filename_string

        return table, filenames
