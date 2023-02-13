import os, time
from dash.dependencies import Input, Output, State
from dash import dcc
from dash import dcc, html
from dash import dash_table as dtable
import pandas as pd
import dash_bootstrap_components as dbc
import datetime as dt

from parts import (
    topMenu,
)

import sftp_utils


# options for file type dropdown
fileOptions = [
    {"label": "RJO - Positions", "value": "rjo_pos"},
    {"label": "RJO - Statement", "value": "rjo_statement"},
    {"label": "RJO - Daily Transactions", "value": "rjo_trades"},
    {"label": "Sol3 - Positions", "value": "sol3_pos"},
    {"label": "Sol3 - Daily Transactions", "value": "sol3_trades"},
]

fileDropwdown = dcc.Dropdown(id="file_options", value="rjo_pos", options=fileOptions)
fileLabel = html.Label(
    ["File Type:"], style={"font-weight": "bold", "text-align": "left"}
)

datePicker = dcc.DatePickerSingle(
    id="file_date", date=dt.date.today() - dt.timedelta(days=1)
)
dateLabel = html.Label(
    ["File Date:"], style={"font-weight": "bold", "text-align": "left"}
)

selectors = dbc.Row(
    [
        dbc.Col(
            [fileLabel, fileDropwdown],
            width=4,
        ),
        dbc.Col(
            [dateLabel, datePicker],
            width=3,
        ),
    ]
)

# layout for dataload page
layout = html.Div(
    [
        topMenu("Data Download"),
        selectors,
        html.Button("download", id="download-button"),
        html.Div(id="output-message"),
        dcc.Download(id="output-download-button"),
        dcc.Loading(
            id="loading-1", type="default", children=html.Div(id="loading-output-1")
        ),
        html.Div(id="hidden-output", style={"display": "none"}),
    ]
)


def initialise_callbacks(app):

    # download button prototype
    @app.callback(
        Output("output-download-button", "data"),
        Output("output-message", "children"),
        # Output("hidden-output", "children"),
        [Input("download-button", "n_clicks")],
        State("file_options", "value"),
        State("file_date", "date"),
        prevent_initial_call=True,
    )
    def download_files(n, fileOptions, fileDate):
        # on click do this
        # filenames = html.Div()
        # get latest sol3 and rjo pos exports
        rjo_date = dt.datetime.strptime(fileDate, "%Y-%m-%d").strftime("%Y%m%d")
        sol3_date_format = dt.datetime.strptime(fileDate, "%Y-%m-%d").strftime("%Y%m%d")
        # RJO daily positions csv
        if fileOptions == "rjo_pos":
            try:
                rjo_file = sftp_utils.fetch_latest_rjo_export(
                    f"UPETRADING_csvnpos_npos_{rjo_date}.csv"
                )
                to_download = dcc.send_data_frame(rjo_file[0].to_csv, f"{rjo_file[1]}")
                return to_download, f"Downloaded {rjo_file[1]}"
            except:
                return "error", "No file found"
        # RJO daily PDF statement
        elif fileOptions == "rjo_statement":
            try:
                sftp_utils.download_rjo_statement(
                    f"UPETRADING_statement_dstm_{rjo_date}.pdf"
                )
                return (
                    dcc.send_file(
                        f"./src/assets/UPETRADING_statement_dstm_{rjo_date}.pdf"
                    ),
                    f"Downloaded UPETRADING_statement_dstm_{rjo_date}.pdf",
                )
            except:
                return "error", "No file found", " "
            finally:  # remove file temporarily placed in assets folder
                if os.path.isfile(
                    f"./src/assets/UPETRADING_statement_dstm_{rjo_date}.pdf"
                ):
                    os.unlink(f"./src/assets/UPETRADING_statement_dstm_{rjo_date}.pdf")

        # RJO daily trades CSV
        elif fileOptions == "rjo_trades":
            try:
                rjo_file = sftp_utils.fetch_latest_rjo_export(
                    f"UPETRADING_csvth1_dth1_{rjo_date}.csv"
                )
                to_download = dcc.send_data_frame(rjo_file[0].to_csv, f"{rjo_file[1]}")
                return to_download, f"Downloaded {rjo_file[1]}"
            except:
                return "error", "No file found"
        # Sol3 daily positions CSV, most recent from chosen date
        elif fileOptions == "sol3_pos":
            try:
                sol3_file = sftp_utils.fetch_latest_sol3_export(
                    "positions", f"export_positions_cme_{sol3_date_format}-%H%M.csv"
                )
                to_download = dcc.send_data_frame(
                    sol3_file[0].to_csv, f"{sol3_file[1]}"
                )
                return to_download, f"Downloaded {sol3_file[1]}"
            except:
                return "error", "No file found"
        # Sol3 daily trades CSV, most recent from chosen date
        elif fileOptions == "sol3_trades":
            try:
                sol3_file = sftp_utils.fetch_latest_sol3_export(
                    "trades", f"{sol3_date_format}_%H%M%S.csv"
                )
                to_download = dcc.send_data_frame(
                    sol3_file[0].to_csv, f"{sol3_file[1]}"
                )
                return to_download, f"Downloaded {sol3_file[1]}"
            except:
                return "error", "No file found"

    # download button prototype
    @app.callback(
        Output("loading-output-1", "value"),
        [Input("download-button", "n_clicks")],
        prevent_initial_call=True,
    )
    def download_files(n):
        time.sleep(3)
        return n
