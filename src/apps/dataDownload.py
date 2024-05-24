import datetime as dt
import logging
import os
import pickle
import time

import dash_bootstrap_components as dbc
import pandas as pd
import sftp_utils
from dash import dcc, html
from dash.dependencies import Input, Output, State
from data_connections import conn
from parts import (
    loadStaticDataExpiry,
    topMenu,
)

logger = logging.getLogger("frontend")

# options for file type dropdown
fileOptions = [
    {"label": "RJO - Positions", "value": "rjo_pos"},
    {"label": "RJO - Statement", "value": "rjo_statement"},
    {"label": "RJO - Daily Transactions", "value": "rjo_trades"},
    {"label": "Sol3 - Positions", "value": "sol3_pos"},
    {"label": "Sol3 - Daily Transactions", "value": "sol3_trades"},
    {"label": "LME - Expiring Positions", "value": "lme_monthly_pos"},
]

fileDropdown = dcc.Dropdown(
    id="file_options", value="rjo_pos", options=fileOptions, clearable=False
)
fileLabel = html.Label(
    ["File Type:"], style={"font-weight": "bold", "text-align": "left"}
)

datePicker = dcc.DatePickerSingle(
    id="file_date",
    date=dt.date.today() - dt.timedelta(days=1),
    display_format="DD/MM/YYYY",
)
dateLabel = html.Label(
    ["File Date:"], style={"font-weight": "bold", "text-align": "left"}
)

selectors = dbc.Row(
    [
        dbc.Col(
            [fileLabel, fileDropdown],
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
        html.Div(
            [
                selectors,
                html.Button("download", id="download-button"),
                html.Div(id="output-message"),
                dcc.Download(id="output-download-button"),
                dcc.Loading(
                    id="loading-1",
                    type="default",
                    children=html.Div(id="loading-output-1"),
                ),
                html.Div(id="hidden-output", style={"display": "none"}),
            ],
            className="mx-3",
        ),
    ]
)


# produce monthly positions report for LME expiry
def getMonthlyPositions():
    # get front month from static data
    staticData = loadStaticDataExpiry()

    staticData["expiry"] = pd.to_datetime(staticData["expiry"], format="%d/%m/%Y")
    staticData = staticData.drop_duplicates(subset="expiry")
    staticData = staticData.sort_values(by="expiry")
    staticData.reset_index(drop=True, inplace=True)

    frontMonth = staticData["product"][0][4:]

    # get positions from redis and filter for front month
    pos_df: pd.DataFrame = pickle.loads(conn.get("positions"))

    pos_df["instrument"] = pos_df["instrument"].str.upper()
    pos_df = pos_df[pos_df["instrument"].str.slice(start=4, stop=6) == frontMonth]
    pos_df[["product", "strike", "cop"]] = pos_df["instrument"].str.split(
        " ", expand=True
    )
    index_columns = ["product", "cop", "strike"]  # , "instrument", "quanitity"
    pos_df.set_index(index_columns, inplace=True)
    # # keep only the index columns
    pos_df.drop(
        pos_df.columns.difference(
            ["product", "cop", "strike", "instrument", "quanitity"]
        ),
        axis=1,
        inplace=True,
    )
    pos_df.sort_index(ascending=False, inplace=True)
    pos_df = pos_df[pos_df["quanitity"] != 0]

    fileName = "{}_lme_option_positions.csv".format(frontMonth)

    return (pos_df, fileName)


def initialise_callbacks(app):
    # download button prototype
    @app.callback(
        Output("output-download-button", "data"),
        Output("output-message", "children"),
        [Input("download-button", "n_clicks")],
        State("file_options", "value"),
        State("file_date", "date"),
        State("output-download-button", "data"),
        prevent_initial_call=True,
    )
    def download_files(n, fileOptions, fileDate, downloadState):
        rjo_date = dt.datetime.strptime(fileDate, "%Y-%m-%d").strftime("%Y%m%d")
        sol3_date_format = dt.datetime.strptime(fileDate, "%Y-%m-%d").strftime("%Y%m%d")
        # RJO daily positions csv
        if fileOptions == "rjo_pos":
            try:
                (rjo_df, rjo_filename) = sftp_utils.fetch_latest_rjo_export(
                    f"UPETRADING_csvnpos_npos_{rjo_date}.csv"
                )
                to_download = dcc.send_data_frame(rjo_df.to_csv, rjo_filename)
                return to_download, f"Downloaded {rjo_filename}"
            except Exception:
                logger.exception("error retrieving file")
                return downloadState, "No file found"
        # RJO daily PDF statement
        elif fileOptions == "rjo_statement":
            filepath = None
            try:
                filepath = sftp_utils.download_rjo_statement(rjo_date)
                return dcc.send_file(filepath), f"Downloaded {filepath}"
            except Exception:
                logger.exception("error retrieving file")
                return downloadState, "No file found"
            finally:  # remove file temporarily placed in assets folder
                if filepath is not None:
                    if os.path.isfile(filepath):
                        os.unlink(filepath)

        # RJO daily trades CSV
        elif fileOptions == "rjo_trades":
            try:
                (rjo_df, rjo_filename) = sftp_utils.fetch_latest_rjo_export(
                    f"UPETRADING_csvth1_dth1_{rjo_date}.csv"
                )
                to_download = dcc.send_data_frame(rjo_df.to_csv, rjo_filename)
                return to_download, f"Downloaded {rjo_filename}"
            except Exception:
                logger.exception("error retrieving file")
                return downloadState, "No file found"
        # Sol3 daily positions CSV, most recent from chosen date
        elif fileOptions == "sol3_pos":
            try:
                (sol3_df, sol3_filename) = sftp_utils.fetch_latest_sol3_export(
                    "positions", f"export_positions_cme_{sol3_date_format}-%H%M.csv"
                )
                to_download = dcc.send_data_frame(sol3_df.to_csv, sol3_filename)
                return to_download, f"Downloaded {sol3_filename}"
            except Exception:
                logger.exception("error retrieving file")
                return downloadState, "No file found"
        # Sol3 daily trades CSV, most recent from chosen date
        elif fileOptions == "sol3_trades":
            try:
                (sol3_df, sol3_filename) = sftp_utils.fetch_latest_sol3_export(
                    "trades", f"{sol3_date_format}_%H%M%S.csv"
                )
                to_download = dcc.send_data_frame(sol3_df.to_csv, sol3_filename)
                return to_download, f"Downloaded {sol3_filename}"
            except Exception:
                logger.exception("error retrieving file")
                return downloadState, "No file found"
        # LME monthly pos report for expiry
        elif fileOptions == "lme_monthly_pos":
            try:
                pos, fileName = getMonthlyPositions()
                to_download = dcc.send_data_frame(pos.to_csv, fileName)
                return to_download, f"Downloaded {fileName}"
            except Exception:
                logger.exception("error retrieving file")
                return downloadState, "No file found"

    # download button prototype
    @app.callback(
        Output("loading-output-1", "value"),
        [Input("download-button", "n_clicks")],
        prevent_initial_call=True,
    )
    def download_files(n):
        time.sleep(5)
        return n
