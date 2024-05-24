import datetime as dt
import io
import logging
import os
from typing import Dict

import dash_bootstrap_components as dbc
import numpy as np
import pandas as pd
import pnl_utils
import sftp_utils
import sqlalchemy
from dash import callback_context, dcc, html
from dash import dash_table as dtable
from dash.dash_table.Format import Format
from dash.dependencies import Input, Output
from data_connections import conn, shared_engine, shared_session
from parts import (
    dev_key_redis_append,
    get_first_wednesday,
    topMenu,
)
from sqlalchemy.dialects.postgresql import insert
from upedata import dynamic_data as upe_dynamic
from upedata import static_data as upe_static

logger = logging.getLogger("frontend")

USE_DEV_KEYS = os.getenv("USE_DEV_KEYS", "false").lower() in [
    "true",
    "t",
    "1",
    "y",
    "yes",
]


metals_dict = {
    "AU": "Aluminium",
    "CP": "Copper",
    "BN": "Nickel",
    "LD": "Lead",
    "L8": "Zinc",
}

INTERNAL_PNL_REDIS_BASE_LOCATION_old = "frontend:georgia-pnl"
INTERNAL_PNL_REDIS_BASE_LOCATION = "frontend:internal-georgia-pnl"

RJO_CASH_FILE_REDIS_BASE_LOCATION = "frontend:rjo-cash-file"

CLOSING_PRICE_REC_REDIS_BASE_LOCATION = "frontend:closing-price-rec"


def multiply_rjo_positions(rjo_row: pd.Series) -> int:
    pos = rjo_row["quantity"]
    if rjo_row["buy_sell_code"] == 2:
        pos = pos * -1
    return pos


portfolio_dropdown = dbc.Row(
    [
        # dbc.Col(
        #     dcc.Dropdown(
        #         id="pnl-exchange-dropdown",
        #         options=[],
        #     ),
        #     width={"size": 3},
        # ),
        dbc.Col(
            [
                html.Label(
                    ["Portfolio:"], style={"font-weight": "bold", "text-align": "left"}
                ),
                dcc.Dropdown(
                    id="pnl-portfolio-dropdown",
                    options=[],
                ),
            ],
            width={"size": 3},
        ),
        dbc.Col(
            [html.Br(), html.Button("Refresh", id="pnl-refresh", n_clicks=0)],
            width={"size": 2},
        ),
        dbc.Col(
            [
                html.Br(),
                html.Div(id="new-pnl-filestring", children="Internal PnL Loading... "),
            ],
        ),
    ]
)

grey_divider = html.Hr(
    style={
        "width": "100%",
        "borderTop": "2px solid gray",
        "borderBottom": "2px solid gray",
        "opacity": "unset",
    }
)


# layout for dataload page
layout = html.Div(
    [
        topMenu("Cash Manager"),
        # hidden refresh
        html.Div(
            [
                html.Div(id="hidden-refresh", style={"display": "none"}),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.Button(
                                    "Refresh", id="refresh-button-2", n_clicks=0
                                ),
                            ]
                        ),
                    ],
                ),
                html.Div(id="output-cash-button-2"),
                dcc.Loading(
                    id="loading-3",
                    children=[
                        html.Div(
                            [
                                html.Div(
                                    id="rjo-filename-2",
                                    children="Cash Manager Loading...",
                                )
                            ]
                        )
                    ],
                    type="circle",
                ),
                # html.Br(),
                grey_divider,
                portfolio_dropdown,
                dcc.Loading(
                    id="loading-7",
                    children=[
                        html.Div(
                            [
                                html.Div(
                                    id="pnl-table-output",
                                    children="PnL Loading...",
                                )
                            ]
                        )
                    ],
                    type="circle",
                ),
                html.Br(),
                dcc.Loading(
                    id="loading-8",
                    children=[
                        html.Div(
                            [
                                html.Div(
                                    id="rjo-fees-table-output",
                                    children="fees loading...",
                                ),
                                html.Br(),
                                html.Div(
                                    id="rjo-misc-fees-table-output",
                                    children=" ",
                                ),
                            ]
                        )
                    ],
                    type="circle",
                ),
                grey_divider,
                # push the closing price rec down a bit
                html.Div([html.Br()]),
                html.Div(
                    id="closePrice-rec-filestring-2",
                    children="Closing Price Rec Loading... ",
                ),
                dcc.Loading(
                    id="loading-5",
                    children=[html.Div([html.Div(id="closePrice-rec-table-2")])],
                    type="circle",
                ),
            ],
            className="mx-3 mt-2",
        ),
    ]
)


def initialise_callbacks(app):
    # cash manager page, dont run on page load
    @app.callback(
        Output("output-cash-button-2", "children"),
        Output("rjo-filename-2", "children"),
        [Input("hidden-refresh", "n_clicks"), Input("refresh-button-2", "n_clicks")],
    )
    def cashManager(hidden_refresh, button_refresh):
        # implement refresh logic here
        trig_id = callback_context.triggered[0]["prop_id"].split(".")[0]

        # first check if pnl data is in redis
        ttl = conn.ttl(RJO_CASH_FILE_REDIS_BASE_LOCATION + dev_key_redis_append)

        if trig_id == "refresh-button-2":
            ttl = 0

        if ttl > 0:
            cash_data = conn.get(
                RJO_CASH_FILE_REDIS_BASE_LOCATION + dev_key_redis_append
            )
            cash_data = pd.read_pickle(io.BytesIO(cash_data))

            file_string = conn.get(
                RJO_CASH_FILE_REDIS_BASE_LOCATION + "_filename" + dev_key_redis_append
            ).decode()

            cash_table = dtable.DataTable(
                data=cash_data.to_dict("records"),
                columns=[
                    {
                        "name": str(col_name),
                        "id": str(col_name),
                        "type": "numeric",
                        "format": Format(group=","),
                    }
                    for col_name in cash_data.columns
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

            return cash_table, file_string

        # get latest rjo pos exports
        (latest_rjo_df, latest_rjo_filename) = sftp_utils.fetch_latest_rjo_export(
            "UPETRADING_csvnmny_nmny_%Y%m%d.csv"
        )

        latest_rjo_df = latest_rjo_df.reset_index()
        latest_rjo_df = latest_rjo_df[latest_rjo_df["Record Code"] == "M"]

        # round all integers to 0dp
        latest_rjo_df = latest_rjo_df.round(0)

        # Last Activity Date to datetime from format yyyymmdd to yyyy-mm-dd without pandas
        latest_rjo_df["Last Activity Date"] = latest_rjo_df["Last Activity Date"].apply(
            lambda x: dt.datetime.strptime(str(x), "%Y%m%d").date()
        )

        # transpose
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

        # # add pnl row
        latest_rjo_df.loc["PNL"] = (
            latest_rjo_df.loc["Liquidating Value"]
            - latest_rjo_df.loc["Previous Liquidating Value"]
        )

        # final df, send to redis and then to frontend
        latest_rjo_df.reset_index(inplace=True)

        # get date from filename
        file_date = (
            dt.datetime.strptime(
                latest_rjo_filename, "UPETRADING_csvnmny_nmny_%Y%m%d.csv"
            )
            .date()
            .strftime("%Y-%m-%d")
        )
        filename_string = "RJO Cash file from: " + file_date

        with io.BytesIO() as bio:
            latest_rjo_df.to_pickle(bio, compression=None)
            conn.set(
                RJO_CASH_FILE_REDIS_BASE_LOCATION + dev_key_redis_append,
                bio.getvalue(),
                ex=60 * 60 * 10,
            )

        conn.set(
            RJO_CASH_FILE_REDIS_BASE_LOCATION + "_filename" + dev_key_redis_append,
            filename_string,  # .encode(),
            ex=60 * 60 * 10,
        )

        cash_table = dtable.DataTable(
            data=latest_rjo_df.to_dict("records"),
            columns=[
                {
                    "name": str(col_name),
                    "id": str(col_name),
                    "type": "numeric",
                    "format": Format(group=","),
                }
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

        return cash_table, filename_string

    # closing price rec
    @app.callback(
        Output("closePrice-rec-filestring-2", "children"),
        Output("closePrice-rec-table-2", "children"),
        [Input("hidden-refresh", "n_clicks"), Input("refresh-button-2", "n_clicks")],
        # prevent_initial_call=True,
    )
    def CLO_rec(hidden_refresh, button_refresh):
        # implement refresh logic here
        trig_id = callback_context.triggered[0]["prop_id"].split(".")[0]

        # first check if pnl data is in redis
        ttl = conn.ttl(CLOSING_PRICE_REC_REDIS_BASE_LOCATION + dev_key_redis_append)

        if trig_id == "refresh-button-2":
            ttl = 0

        # this means data exists in redis
        if ttl > 0:
            clo_data = conn.get(
                CLOSING_PRICE_REC_REDIS_BASE_LOCATION + dev_key_redis_append
            )

            try:
                clo_data = clo_data.decode()
            except Exception:
                clo_data = pd.read_pickle(io.BytesIO(clo_data))
                clo_data = dtable.DataTable(
                    data=clo_data.to_dict("records"),
                    columns=[
                        {"name": str(col_name), "id": str(col_name)}
                        for col_name in clo_data.columns
                    ],
                    style_data_conditional=[
                        {
                            "if": {
                                "column_id": "Differentially Priced Instruments",
                            },
                            # yellow background
                            "backgroundColor": "yellow",
                        }
                    ],
                )
            except:
                clo_data = (
                    "Error loading closing price data, try refreshing the feature"
                )

            file_string = conn.get(
                CLOSING_PRICE_REC_REDIS_BASE_LOCATION
                + "_filename"
                + dev_key_redis_append
            ).decode()

            return clo_data, file_string

        # start of normal function

        # get latest rjo exports, CLO and pos
        (clo_df, clo_filename) = sftp_utils.fetch_latest_rjo_export(
            "%Y%m%d_CLO_r.csv", "/LMEPrices"
        )

        (rjo_df, rjo_df_filename) = sftp_utils.fetch_latest_rjo_export(
            "UPETRADING_csvnpos_npos_%Y%m%d.csv"
        )

        # filter for exchange and record code
        rjo_df = rjo_df[rjo_df["Bloomberg Exch Code"] == "LME"]
        rjo_df = rjo_df[rjo_df["Record Code"] == "P"]

        rjo_df = rjo_df.drop_duplicates(subset="Security Desc Line 1", keep="first")

        # get the price from clo_df based on the matching criteria
        def get_lme_price_from_rjo(row):
            # format: {rjo_code: lme_code}
            metals_dict_CLO = {
                "AU": "AH",
                "BN": "NI",
                "LD": "PB",
                "L8": "ZS",
                "CP": "CA",
            }

            if row["Security Subtype Code"] not in ["C", "P"]:
                # futures
                clo_filtered = clo_df[
                    (clo_df["UNDERLYING"] == metals_dict_CLO[row["Contract Code"]])
                    & (clo_df["CONTRACT_TYPE"].isin(["LMEFuture", "LMEForward"]))
                    & (clo_df["FORWARD_DATE"] == row["Option Expire Date"])
                ]
            else:
                # options
                clo_filtered = clo_df[
                    (clo_df["UNDERLYING"] == metals_dict_CLO[row["Contract Code"]])
                    & (clo_df["CONTRACT_TYPE"] == "LMEOption")
                    & (clo_df["FORWARD_MONTH"] == row["Contract Month"])
                    & (clo_df["STRIKE"] == row["Option Strike Price"])
                    & (clo_df["SUB_CONTRACT_TYPE"] == row["Security Subtype Code"])
                ]

            if not clo_filtered.empty:
                return clo_filtered.iloc[0]["PRICE"]
            else:
                return None

        rjo_df["LME Close Price"] = rjo_df.apply(get_lme_price_from_rjo, axis=1)

        # print rjo df but only the lme_clo, close price and security desc line 1 columns
        rjo_df = rjo_df[
            [
                "Security Desc Line 1",
                "LME Close Price",
                "Close Price",
            ]
        ]
        rjo_df = rjo_df[rjo_df["LME Close Price"] - rjo_df["Close Price"] != 0]
        # rename columns
        rjo_df = rjo_df.rename(
            columns={
                "Security Desc Line 1": "Differentially Priced Instruments",
                "LME Close Price": "LME Close Price",
                "Close Price": "RJO Close Price",
            }
        )

        if rjo_df.empty:
            clo_message1 = "No closing price differences found between {}".format(
                clo_filename
            )
            clo_message2 = "and {}".format(rjo_df_filename)

        clo_table = dtable.DataTable(
            data=rjo_df.to_dict("records"),
            columns=[
                {"name": str(col_name), "id": str(col_name)}
                for col_name in rjo_df.columns
            ],
            style_data_conditional=[
                {
                    "if": {
                        "column_id": "Differentially Priced Instruments",
                    },
                    # yellow background
                    "backgroundColor": "yellow",
                }
            ],
        )

        filename_string = "Found these differences between {} and {}".format(
            clo_filename, rjo_df_filename
        )

        if rjo_df.empty:
            # send these two to redis
            conn.set(
                CLOSING_PRICE_REC_REDIS_BASE_LOCATION + dev_key_redis_append,
                clo_message1,
                ex=60 * 60 * 10,
            )
            conn.set(
                CLOSING_PRICE_REC_REDIS_BASE_LOCATION
                + "_filename"
                + dev_key_redis_append,
                clo_message2,
                ex=60 * 60 * 10,
            )
            return clo_message1, clo_message2
        else:
            with io.BytesIO() as bio:
                rjo_df.to_pickle(bio, compression=None)
                conn.set(
                    CLOSING_PRICE_REC_REDIS_BASE_LOCATION + dev_key_redis_append,
                    bio.getvalue(),
                    ex=60 * 60 * 10,
                )
            conn.set(
                CLOSING_PRICE_REC_REDIS_BASE_LOCATION
                + "_filename"
                + dev_key_redis_append,
                filename_string,
                ex=60 * 60 * 10,
            )
            return clo_table, filename_string

    # populate dropdowns for pnl page
    @app.callback(
        Output("pnl-portfolio-dropdown", "options"),
        Output("pnl-portfolio-dropdown", "value"),
        [Input("pnl-refresh", "n_clicks")],
        # [Input("pnl-portfolio-dropdown", "value")],
    )
    def load_pnl_dropdowns(n):
        with shared_session() as session:
            portfolio_options = session.query(upe_static.Portfolio).all()
            portfolio_options = [
                {"label": x.display_name, "value": x.portfolio_id}
                for x in portfolio_options
                if x.display_name != "Error"
                and x.display_name != "Backbook"
                and x.display_name != "CME General"
            ]

        return portfolio_options, portfolio_options[0]["value"]

    # pnl and fees calc, check redis cache or pull from sftp
    @app.callback(
        Output("rjo-fees-table-output", "children"),
        Output("new-pnl-filestring", "children"),
        Output("pnl-table-output", "children"),
        Output("rjo-misc-fees-table-output", "children"),
        # Output("pnl-portfolio-dropdown", "value"),
        [Input("pnl-refresh", "n_clicks"), Input("pnl-portfolio-dropdown", "value")],
    )
    def load_rjo_files_for_pnl(n, portfolio_id):
        # implement refresh logic here
        trig_id = callback_context.triggered[0]["prop_id"].split(".")[0]

        # collect database mappings necessary with redis cache or without
        with shared_engine.connect() as cnxn:
            stmt1 = sqlalchemy.text(
                "SELECT platform_account_id FROM platform_account_portfolio_associations WHERE portfolio_id = :portfolio_id AND platform = :platform"
            )
            stmt2 = sqlalchemy.text(
                "SELECT platform_symbol, product_symbol FROM third_party_product_symbols WHERE platform_name = :platform_name"
            )
            stmt3 = sqlalchemy.text("SELECT symbol, long_name FROM products")
            rjo_portfolio_id = cnxn.execute(
                stmt1, {"platform": "RJO", "portfolio_id": portfolio_id}
            ).scalar_one_or_none()
            result = cnxn.execute(stmt2, {"platform_name": "RJO"})
            products = cnxn.execute(stmt3).fetchall()

        rjo_symbol_map = {res.platform_symbol: res.product_symbol for res in result}
        product_map = {res.symbol: res.long_name for res in products}

        # check for redis ttl here, if yes then pull and skip to bottom, if not, then run the rest
        # pull the last posted
        ttl = conn.ttl(
            "frontend:rjo_fees_file_for_pnl" + "_filename" + dev_key_redis_append
        )

        if trig_id == "pnl-refresh":
            ttl = 0

        if ttl > 0:
            # pull from redis
            fees_file = conn.get(
                "frontend:rjo_fees_file_for_pnl" + dev_key_redis_append
            )
            fees_file = pd.read_pickle(io.BytesIO(fees_file))

            fees_filename = conn.get(
                "frontend:rjo_fees_file_for_pnl" + "_filename" + dev_key_redis_append
            ).decode()

            # now pos files
            rjo_pos_1 = conn.get(
                "frontend:rjo_pos_file_for_pnl_1" + dev_key_redis_append
            )
            rjo_pos_1 = pd.read_pickle(io.BytesIO(rjo_pos_1))
            pos_filename_1 = conn.get(
                "frontend:rjo_pos_file_for_pnl_1" + "_filename" + dev_key_redis_append
            ).decode()

            rjo_pos_2 = conn.get(
                "frontend:rjo_pos_file_for_pnl_2" + dev_key_redis_append
            )
            rjo_pos_2 = pd.read_pickle(io.BytesIO(rjo_pos_2))
            pos_filename_2 = conn.get(
                "frontend:rjo_pos_file_for_pnl_2" + "_filename" + dev_key_redis_append
            ).decode()

            # rebates file
            misc_fees = conn.get(
                "frontend:rjo_misc_fees_file_for_pnl" + dev_key_redis_append
            )
            misc_fees = pd.read_pickle(io.BytesIO(misc_fees))

        else:
            # first load in all 3 sftp files
            file_formats = [
                "UPETRADING_csvnpos_npos_%Y%m%d.csv",
                "UPETRADING_csvth1_dth1_%Y%m%d.csv",
            ]
            (
                pos_file_1,
                pos_filename_1,
                fees_file,
                fees_filename,
                pos_file_2,
                pos_filename_2,
            ) = sftp_utils.fetch_pnl_files_from_sftp(file_formats)

            # do the processing on fees file
            # pull out any rebates, adjustments and format
            misc_fees = fees_file[fees_file["Record Code"].isin(["A", "C"])]
            misc_fees = misc_fees[
                [
                    "Account Number",
                    "Security Desc Line 1",
                    "Total Net Charge Amount",
                ]
            ]

            # filter just for Transaction
            fees_file = fees_file[fees_file["Record Code"].eq("T")]
            rjo_fees_columns = [
                "Quantity",
                "Account Number",
                "Contract Code",
                "Commission Amount",
                "Clearing Fee",
                "Exchange Fee",
                "Nfa Fee",
                "Global Desk Charge Fee",
                "Rjo Tran Fee",
                "Ib Tran Fee",
                "Charge Amount Give Up",
                "Charge Amount Brokerage",
                "Charge Amount Other",
                "Total Net Charge Amount",
            ]

            # keep only the columns we need
            fees_file = fees_file[rjo_fees_columns]  # ready to send to redis

            # remove columns with all zeros - rerun again after filtering on portfolio_id
            fees_file = fees_file.loc[:, (fees_file != 0).any(axis=0)]

            fees_file["georgia_product_symbol"] = fees_file["Contract Code"].apply(
                lambda x: rjo_symbol_map.get(x, x)
            )

            # do the processing on pos files

            rjo_pos_1, rjo_pos_2 = process_rjo_pos_files_for_pnl(
                portfolio_id,
                rjo_symbol_map,
                pos_file_1,
                pos_filename_1,
                pos_file_2,
                pos_filename_2,
            )

            # cache all 3 dfs on redis for pulling and filtering on portfolio_id
            # POS 1
            with io.BytesIO() as bio:
                rjo_pos_1.to_pickle(bio, compression=None)
                conn.set(
                    "frontend:rjo_pos_file_for_pnl_1" + dev_key_redis_append,
                    bio.getvalue(),
                    ex=60 * 60 * 10,
                )
            conn.set(
                "frontend:rjo_pos_file_for_pnl_1" + "_filename" + dev_key_redis_append,
                pos_filename_1,  # .encode(),
                ex=60 * 60 * 10,
            )
            # POS 2
            with io.BytesIO() as bio:
                rjo_pos_2.to_pickle(bio, compression=None)
                conn.set(
                    "frontend:rjo_pos_file_for_pnl_2" + dev_key_redis_append,
                    bio.getvalue(),
                    ex=60 * 60 * 10,
                )
            conn.set(
                "frontend:rjo_pos_file_for_pnl_2" + "_filename" + dev_key_redis_append,
                pos_filename_2,  # .encode(),
                ex=60 * 60 * 10,
            )
            # FEES FILE
            with io.BytesIO() as bio:
                fees_file.to_pickle(bio, compression=None)
                conn.set(
                    "frontend:rjo_fees_file_for_pnl" + dev_key_redis_append,
                    bio.getvalue(),
                    ex=60 * 60 * 10,
                )
            conn.set(
                "frontend:rjo_fees_file_for_pnl" + "_filename" + dev_key_redis_append,
                fees_filename,  # .encode(),
                ex=60 * 60 * 10,
            )
            # REBATES FILE
            with io.BytesIO() as bio:
                misc_fees.to_pickle(bio, compression=None)
                conn.set(
                    "frontend:rjo_misc_fees_file_for_pnl" + dev_key_redis_append,
                    bio.getvalue(),
                    ex=60 * 60 * 10,
                )

        # finish processing positions files after pulling from redis or sftp
        # filter both for account number
        rjo_pos_1 = rjo_pos_1[rjo_pos_1["account_number"].eq(rjo_portfolio_id)]
        rjo_pos_2 = rjo_pos_2[rjo_pos_2["account_number"].eq(rjo_portfolio_id)]

        # drop columns from rj0_pos_2 by keeping some
        rjo_pos_2_pnl = rjo_pos_2[
            [
                "account_number",
                "georgia_product_symbol",
                "market_value_t2",
            ]
        ]
        # lets sort out rjo_pos_2 first
        rjo_pos_2_pnl = rjo_pos_2_pnl.groupby(
            ["georgia_product_symbol"], as_index=False
        ).agg(
            {
                "market_value_t2": "sum",
            }
        )

        # sort out rjo_pos_1
        rjo_pos_1_pnl = rjo_pos_1[
            [
                "account_number",
                "georgia_product_symbol",
                "market_value_t1",
            ]
        ]
        rjo_pos_1_pnl = rjo_pos_1_pnl.groupby(
            ["georgia_product_symbol"], as_index=False
        ).agg(
            {
                "market_value_t1": "sum",
                # "account_number": "first",
            }
        )

        # add fees to rjo_pos_1 soon but first lets get a good table going joining these two
        # merge the dataframes on `georgia_product_symbol`
        merged_df = pd.merge(
            rjo_pos_1_pnl, rjo_pos_2_pnl, on="georgia_product_symbol", how="outer"
        )

        # Set `georgia_product_symbol` as index
        merged_df.set_index("georgia_product_symbol", inplace=True)

        # # add total row
        merged_df["Gross PnL"] = (
            merged_df["market_value_t1"] - merged_df["market_value_t2"]
        )

        # get date from filenames
        rjo_file_1_date = (
            dt.datetime.strptime(pos_filename_1, "UPETRADING_csvnpos_npos_%Y%m%d.csv")
            .date()
            .strftime("%Y-%m-%d")
        )
        rjo_file_2_date = (
            dt.datetime.strptime(pos_filename_2, "UPETRADING_csvnpos_npos_%Y%m%d.csv")
            .date()
            .strftime("%Y-%m-%d")
        )

        # rename columns
        merged_df.rename(
            columns={
                "market_value_t1": f"MV @ {rjo_file_1_date}",
                "market_value_t2": f"MV @ {rjo_file_2_date}",
            },
            inplace=True,
        )

        # transpose the dataframe
        transposed_df = merged_df.transpose()

        # add a total column
        transposed_df["Total"] = transposed_df.sum(axis=1, numeric_only=True)

        # add a index column and make it appear first
        transposed_df.reset_index(inplace=True)
        transposed_df.rename(columns={"index": " "}, inplace=True)

        ########FEEEEESSSS
        # split by portfolio
        fees_file = fees_file[
            fees_file["Account Number"].eq(rjo_portfolio_id)
        ]  # portfolio_id
        if not fees_file.empty:
            # come back to this when revisiting pnl
            # est_fees = add_estimated_fees_to_portfolio(fees_file)
            # remove columns with all zeros
            fees_file = fees_file.loc[:, (fees_file != 0).any(axis=0)]

            # group by product
            fees_file = fees_file.groupby("georgia_product_symbol", as_index=False).sum(
                numeric_only=True
            )

            fees_file.set_index("georgia_product_symbol", inplace=True)

            fees_date = (
                dt.datetime.strptime(fees_filename, "UPETRADING_csvth1_dth1_%Y%m%d.csv")
                .date()
                .strftime("%Y-%m-%d")
            )
            fees_file.rename(
                columns={"Quantity": f"Quantity Traded {fees_date}"}, inplace=True
            )
            fees_file = fees_file.transpose()

            # add total column, excluding the 'Quantity' column
            fees_file["Total"] = fees_file.sum(axis=1, numeric_only=True)

            # # reset index
            fees_file.reset_index(inplace=True)

            # rename index
            fees_file.rename(columns={"index": " "}, inplace=True)

            # rename columns in both tables with product_map
            fees_file.rename(columns=product_map, inplace=True)
            fees_table = dtable.DataTable(
                data=fees_file.round(0).to_dict("records"),
                columns=[
                    {
                        "name": str(col_name),
                        "id": str(col_name),
                        "type": "numeric",
                        "format": Format(group=","),
                    }
                    for col_name in fees_file.columns
                ],
                style_data_conditional=[
                    {
                        "if": {
                            "column_id": " ",
                        },
                        "backgroundColor": "lightgrey",
                    }
                ],
                # style_header={
                #     "display": "none",
                # },
            )
        else:
            fees_table = "No fees found for this portfolio today"

        transposed_df.rename(columns=product_map, inplace=True)

        # finish processing of pos tables

        pnl_table_frontend = dtable.DataTable(
            data=transposed_df.round(0).to_dict("records"),
            columns=[
                {
                    "name": str(col_name),
                    "id": str(col_name),
                    "type": "numeric",
                    "format": Format(group=","),
                }
                for col_name in transposed_df.columns
            ],
            style_data_conditional=[
                {
                    "if": {
                        "column_id": " ",
                    },
                    "backgroundColor": "lightgrey",
                }
            ],
            style_header={
                "display": "table-cell",
            },
        )

        # format rebates file for frontend
        misc_fees = misc_fees[misc_fees["Account Number"].eq(rjo_portfolio_id)]
        if not misc_fees.empty:
            misc_fees_frontend = dtable.DataTable(
                data=misc_fees.to_dict("records"),
                columns=[
                    {
                        "name": str(col_name),
                        "id": str(col_name),
                        "type": "numeric",
                        "format": Format(group=","),
                    }
                    for col_name in misc_fees.columns
                ],
                style_data_conditional=[
                    {
                        "if": {
                            "column_id": " ",
                        },
                        "backgroundColor": "lightgrey",
                    }
                ],
                style_header={
                    "display": "table-cell",
                },
            )
            misc_fees_div = [
                "Rebates/Adjustments found for this portfolio:",
                misc_fees_frontend,
            ]
        else:
            misc_fees_div = " "

        return (
            fees_table,
            " ",
            pnl_table_frontend,
            misc_fees_div,
        )


def get_product_pnl(t1, t2, yesterday, product):
    """Calculates PnL on RJO trades and positions and compares to RJO reported PnL from cash file.
    - Pulls reported PnL from cash file
    - Pulls RJO positions from last two days of position files
    - Separates T-1 trades and calculates PnL from trade price to close price
    - Matches T-1 trades to T-2 trades and calculates PnL from T-2 close price to T-1 close price
    - Calculates estimated fees on the day
    - Calculates
    """
    t1_product = t1[t1["Contract Code"] == product]
    t2_product = t2[t2["Contract Code"] == product]

    yday_trades = t1_product[t1_product["Trade Date"] == int(yesterday)]
    t1_product = t1_product[t1_product["Trade Date"] != yesterday]

    # calculate PnL on yesterdays trades in t1
    yday_trades["PriceDiff"] = (
        yday_trades["Close Price"] - yday_trades["Formatted Trade Price"]
    )
    yday_trades["Vol"] = np.where(
        yday_trades["Buy Sell Code"] == 1,
        yday_trades["Quantity"],
        -yday_trades["Quantity"],
    )
    yday_trades["PnL"] = (
        yday_trades["PriceDiff"]
        * yday_trades["Vol"]
        * yday_trades["Multiplication Factor"]
    )
    # calculate estimated fees as: sum(abs(vol)) * 3.4
    est_fees = yday_trades["Quantity"].sum() * 3.4
    tradesPNL = (yday_trades["PnL"].sum()).round(2)

    # match t1 to t2 on security desc line 1, quantity, trade date
    # Convert the 'Quantity' column to string data type in both DataFrames with decimal precision
    t1_product["Quantity"] = t1_product["Quantity"].astype(str)
    t2_product["Quantity"] = t2_product["Quantity"].astype(str)

    # Merge the DataFrames on common columns and identify unmatched rows
    combined = pd.merge(
        t1_product,
        t2_product,
        on=[
            "Buy Sell Code",
            "Quantity",
            "Security Desc Line 1",
            "Trade Date",
            "Formatted Trade Price",
            "Multiplication Factor",
        ],
        how="outer",
        suffixes=("_t1", "_t2"),
        indicator=True,
    )

    # separate matched (pos present in t1+t2) and unmatched (pos present in either but not the other)
    # t1 unmatched should be trades from the day
    # t2 unmatched should be positions expiring on t1
    matched = combined[combined["_merge"] == "both"]
    t1_unmatched = combined[combined["_merge"] == "left_only"].drop(columns="_merge")
    t2_unmatched = combined[combined["_merge"] == "right_only"].drop(columns="_merge")

    matched = matched[
        [
            "Buy Sell Code",
            "Quantity",
            "Security Desc Line 1",
            "Trade Date",
            "Formatted Trade Price",
            "Multiplication Factor",
            "Market Value_t1",
            "Market Value_t2",
            # "Close Price_t1",
            # "Close Price_t2",
        ]
    ]

    # calculate PnL on these positions
    matched["PnL"] = matched["Market Value_t1"] - matched["Market Value_t2"]

    # ensure relevant columns are numeric for calculations
    matched["Quantity"] = pd.to_numeric(matched["Quantity"], errors="coerce")
    matched["Buy Sell Code"] = pd.to_numeric(matched["Buy Sell Code"], errors="coerce")

    # calculate pos pnl
    matchedPNL = (matched["PnL"].sum()).round(2)

    # handle expiry options
    expiry_opts = t2_unmatched[
        t2_unmatched["Security Subtype Code_t2"].isin(["C", "P"])
    ]
    expiry_opts = expiry_opts[expiry_opts["Option Expire Date_t2"] == int(yesterday)]
    expiry_value = expiry_opts["Market Value_t2"].sum()
    tradesPNL -= expiry_value

    # calculate total pnl
    totalPNL = (tradesPNL + matchedPNL).round(2)
    netPNL = (totalPNL - est_fees).round(2)

    # build and send data to postgres
    # data to send: date, product, t1-trades, pos_pnl, gross_pnl
    date = dt.datetime.strptime(yesterday, "%Y%m%d").date()

    results = [tradesPNL, matchedPNL, totalPNL, est_fees, netPNL]

    metals_dict_db = {
        "AU": "xlme-lad-usd",
        "CP": "xlme-lcu-usd",
        "BN": "xlme-lnd-usd",
        "LD": "xlme-pbd-usd",
        "L8": "xlme-lzh-usd",
    }

    results_dict = {
        "pnl_date": date,
        "product_symbol": metals_dict_db[product],
        "source": "RJO",
        "t1_trades": tradesPNL,
        "pos_pnl": matchedPNL,
        "gross_pnl": totalPNL,
    }

    # send to db
    stmt = (
        insert(upe_dynamic.ExternalPnL)
        .values(**results_dict)
        .on_conflict_do_update(
            index_elements=["pnl_date", "product_symbol", "source"],
            set_=results_dict,
        )
    )

    with shared_session() as session:
        session.execute(stmt)
        session.commit()

    return results


def get_prices_from_clo(t2_pos, clo_df, day):
    # make a matching function
    def get_price_from_clo(row):
        price = -1
        instrument = row["instrument_symbol"]
        isOption = True if row["instrument_symbol"][-1] in ["C", "P"] else False

        metals_dict_CLO = {
            "LZH": "ZS",
            "LND": "NI",
            "LAD": "AH",
            "LCU": "CA",
            "PBD": "PB",
        }

        months = {
            "F": "01",
            "G": "02",
            "H": "03",
            "J": "04",
            "K": "05",
            "M": "06",
            "N": "07",
            "Q": "08",
            "U": "09",
            "V": "10",
            "X": "11",
            "Z": "12",
        }

        if not isOption:
            product, prompt = instrument.split(" ")
            prompt = prompt.replace("-", "")

            clo_filtered = clo_df[
                (clo_df["UNDERLYING"] == metals_dict_CLO[product])
                & (clo_df["CONTRACT"] == metals_dict_CLO[product] + "D")
                & (clo_df["CONTRACT_TYPE"] == "LMEForward")
                & (clo_df["FORWARD_DATE"] == int(prompt))
            ]
            # price = clo_filtered.iloc[0]["PRICE"]

        elif isOption:
            instrument, strike, cop = instrument.split(" ")
            product, month, year = instrument[:3], instrument[-2], instrument[-1]
            expiry = "202" + year + months[month]

            clo_filtered = clo_df[
                (clo_df["UNDERLYING"] == metals_dict_CLO[product])
                & (clo_df["CONTRACT_TYPE"] == "LMEOption")
                & (clo_df["FORWARD_MONTH"] == int(expiry))
                & (clo_df["STRIKE"] == int(strike))
                & (clo_df["SUB_CONTRACT_TYPE"] == cop)
            ]

        if clo_filtered.empty:
            logger.error("No price found for: ", row["instrument_symbol"])
            price = 0
        else:
            price = clo_filtered.iloc[0]["PRICE"]

        return price

    col_name = day + "_price"
    t2_pos[col_name] = t2_pos.apply(get_price_from_clo, axis=1)

    return t2_pos


def get_prices_from_clo2(pos, clo_df):
    # make a matching function
    def get_price_from_clo(row):
        price = 0
        instrument = row["instrument_symbol"]
        isOption = True if row["instrument_symbol"][-1] in ["C", "P"] else False

        metals_dict_CLO = {
            "LZH": "ZS",
            "LND": "NI",
            "LAD": "AH",
            "LCU": "CA",
            "PBD": "PB",
        }

        months = {
            "F": "01",
            "G": "02",
            "H": "03",
            "J": "04",
            "K": "05",
            "M": "06",
            "N": "07",
            "Q": "08",
            "U": "09",
            "V": "10",
            "X": "11",
            "Z": "12",
        }

        if not isOption:
            product, prompt = instrument.split(" ")
            prompt = prompt.replace("-", "")

            clo_filtered = clo_df[
                (clo_df["UNDERLYING"] == metals_dict_CLO[product])
                & (clo_df["CONTRACT"] == metals_dict_CLO[product] + "D")
                & (clo_df["CONTRACT_TYPE"] == "LMEForward")
                & (clo_df["FORWARD_DATE"] == int(prompt))
            ]
            # price = clo_filtered.iloc[0]["PRICE"]

        elif isOption:
            instrument, strike, cop = instrument.split(" ")
            product, month, year = instrument[:3], instrument[-2], instrument[-1]
            expiry = "202" + year + months[month]

            clo_filtered = clo_df[
                (clo_df["UNDERLYING"] == metals_dict_CLO[product])
                & (clo_df["CONTRACT_TYPE"] == "LMEOption")
                & (clo_df["FORWARD_MONTH"] == int(expiry))
                & (clo_df["STRIKE"] == int(strike))
                & (clo_df["SUB_CONTRACT_TYPE"] == cop)
            ]

        if clo_filtered.empty:
            logger.error("No price found for: ", row["instrument_symbol"])
            price = 0
        else:
            price = clo_filtered.iloc[0]["PRICE"]

        return price

    pos["closeprice"] = pos.apply(get_price_from_clo, axis=1)

    return pos


def send_pnl_to_dbs(final_df):
    """Sends pnl data to postgres db and redis timed key"""
    # build and send data to postgres
    # data to send: date, product, t1-trades, pos_pnl, gross_pnl
    # columns in final df are: pnl_date, portfolio_id, metal, source, t1_trades, pos_pnl, gross_pnl, est_fees, net_pnl, product_symbol
    # columns in  postgres are: pnl_date, portfolio_id, product_symbol, source, t1_trades, pos_pnl, gross_pnl,
    with shared_session() as session:
        for _, row in final_df.iterrows():
            results_dict = {
                "pnl_date": row["pnl_date"],
                "portfolio_id": row["portfolio_id"],
                "product_symbol": row["product_symbol"],
                "source": row["source"],
                "t1_trades": row["t1_trades"],
                "pos_pnl": row["pos_pnl"],
                "gross_pnl": row["gross_pnl"],
            }
            record = upe_dynamic.ExternalPnL(**results_dict)
            session.merge(record)
        session.commit()

    return 1


def format_pnl_for_frontend(pnl_data):
    df = pnl_data[["metal", "t1_trades", "pos_pnl", "gross_pnl", "est_fees", "net_pnl"]]
    # rename columns
    df = df.rename(
        columns={
            "t1_trades": "T-1 Trades PnL",
            "pos_pnl": "Pos PnL",
            "gross_pnl": "Gross PnL",
            "est_fees": "Estimated Fees",
            "net_pnl": "Net PnL",
        }
    )
    # Set the index to "metal" column
    df.set_index("metal", inplace=True)

    # Transpose the DataFrame
    pivot_df = df.T.reset_index()

    # add a total column
    pivot_df["Total"] = pivot_df.sum(axis=1, numeric_only=True)
    pivot_df = pivot_df.round(0)

    pivot_df.rename(columns={"index": "Source"}, inplace=True)
    # rename row names in index

    row_names = [
        "T-1 Trades PnL",
        "Pos PnL",
        "Gross PnL",
        "Estimated Fees",
        "Net PnL",
    ]
    pivot_df.index = row_names

    # now send to dash
    table = dtable.DataTable(
        data=pivot_df.to_dict("records"),
        columns=[
            {"name": str(col_name), "id": str(col_name)}
            for col_name in pivot_df.columns
        ],
        style_data_conditional=[
            {
                "if": {
                    "column_id": "Source",
                },
                "backgroundColor": "lightgrey",
            }
        ],
    )
    return table


# currently unused, but may be useful in future
def get_pos_from_trades(pos1, trades1):
    # get positions as they were at t2 close
    aggregated_trades = (
        trades1.groupby("instrument_symbol")["quantity"]
        .sum(numeric_only=True)
        .reset_index()
    )
    net_new_trades = dict(
        zip(
            aggregated_trades["instrument_symbol"],
            aggregated_trades["quantity"],
        )
    )
    pos2 = pos1.copy()
    # update net_quantity for each product traded in last two days
    for index, row in pos2.iterrows():
        symbol = row["instrument_symbol"]
        if symbol in net_new_trades:
            logger.debug("updating net quantity for ", symbol)
            pos2.at[index, "net_quantity"] -= net_new_trades[symbol]
    # t2_positions = t2_positions[t2_positions["net_quantity"] != 0]
    return pos2


def expiry_from_symbol(symbol):
    """Returns expiry date from symbol"""
    info = symbol.split(" ")
    if info[0][0] != "x":
        if len(info) == 2:
            expiry = info[1]
            # convert to date object from yyy-mm-dd
            try:
                expiry = dt.datetime.strptime(expiry, "%Y-%m-%d").date()
            except ValueError:
                # if invalid date, set to expired date to be filtered out
                expiry = dt.date(2020, 1, 1)
                logger.exception(f"invalid date format for {symbol}")
        else:
            try:
                code = info[0]
                year = "202" + code[-1]
                month = code[-2]

                monthCode = {
                    "f": 1,
                    "g": 2,
                    "h": 3,
                    "j": 4,
                    "k": 5,
                    "m": 6,
                    "n": 7,
                    "q": 8,
                    "u": 9,
                    "v": 10,
                    "x": 11,
                    "z": 12,
                }
                expiry = get_first_wednesday(int(year), monthCode[month.lower()])
            except KeyError:
                # if invalid date, set to expired date to be filtered out
                expiry = dt.date(2020, 1, 1)
                logger.exception(f"invalid date code for {symbol}")
            except ValueError:
                # if invalid date, set to expired date to be filtered out
                expiry = dt.date(2020, 1, 1)
                logger.exception(f"invalid date code for {symbol}")
        return expiry

    try:
        expiry = dt.datetime.strptime(info[2], r"%y-%m-%d").date()
    except ValueError:
        expiry = dt.date(2020, 1, 1)
        logger.exception(f"invalid date code for {symbol}")
    return expiry


def build_georgia_symbol_from_rjo_overnight(rjo_overnight_row):
    # whoad whoa whoa ok nevermind i think might not even need this function!!
    # if were only storing final pnl as per product and not per instrument
    # then we can just use the product symbol from the rjo overnight file throughout the entire pnl process
    # and only have to convert back to a georgia product symbol at the very end

    # the issue is this means that we will struggle to mix and match input sources as originally planned
    # rjo positions and rjo prices will work well, but trying to combine sources will be a nightmare
    pass


def process_rjo_pos_files_for_pnl(
    portfolio_account_id: str,
    rjo_symbol_mappings: Dict[str, str],
    rjo_pos_1: pd.DataFrame,
    rjo_pos_1_name: str,
    rjo_pos_2: pd.DataFrame,
    rjo_pos_2_name: str,
) -> pd.DataFrame:
    """Pull and process RJO positions files to calculate per product PnL
    RJO positions and RJO prices to be used for calculation.
    """

    # get date from file names
    rjo_file_1_date = dt.datetime.strptime(
        rjo_pos_1_name, "UPETRADING_csvnpos_npos_%Y%m%d.csv"
    ).date()
    rjo_file_2_date = dt.datetime.strptime(
        rjo_pos_2_name, "UPETRADING_csvnpos_npos_%Y%m%d.csv"
    ).date()

    rjo_pos_1.columns = (
        rjo_pos_1.columns.str.strip(" ").str.lower().str.replace(" ", "_")
    )
    rjo_pos_2.columns = (
        rjo_pos_2.columns.str.strip(" ").str.lower().str.replace(" ", "_")
    )

    # # filter for account -- will leave in for now, but later will mnake sense to do the whole sheet together
    # rjo_pos_1 = rjo_pos_1[rjo_pos_1["account_number"].eq(portfolio_account_id)]
    # rjo_pos_2 = rjo_pos_2[rjo_pos_2["account_number"].eq(portfolio_account_id)]

    # create a set of valid symbols from both the unique values in both files contract_code columns
    valid_rjo_symbols = set(
        [*rjo_pos_1["contract_code"].unique(), *rjo_pos_2["contract_code"].unique()]
    )

    # add georgia product symbol columnn to both dataframes
    rjo_pos_1["georgia_product_symbol"] = rjo_pos_1["contract_code"].apply(
        lambda x: rjo_symbol_mappings.get(x, x)
    )
    rjo_pos_2["georgia_product_symbol"] = rjo_pos_2["contract_code"].apply(
        lambda x: rjo_symbol_mappings.get(x, x)
    )

    rjo_pos_1 = rjo_pos_1.loc[
        rjo_pos_1["contract_code"].isin(valid_rjo_symbols)
        & rjo_pos_1["record_code"].eq("P"),
        [
            "security_desc_line_1",
            "contract_code",
            "close_price",
            "formatted_trade_price",
            "trade_date",
            "quantity",
            "buy_sell_code",
            "multiplication_factor",
            "account_number",
            "georgia_product_symbol",
        ],
    ]
    rjo_pos_2 = rjo_pos_2.loc[
        rjo_pos_2["contract_code"].isin(valid_rjo_symbols)
        & rjo_pos_2["record_code"].eq("P"),
        [
            "security_desc_line_1",
            "contract_code",
            "close_price",
            "trade_date",
            "quantity",
            "buy_sell_code",
            "multiplication_factor",
            "account_number",
            "georgia_product_symbol",
        ],
    ]

    # add settle date
    rjo_pos_1["settle_date"] = rjo_file_1_date
    rjo_pos_2["settle_date"] = rjo_file_2_date

    # convert trade_date to datetime
    rjo_pos_1["trade_date"] = rjo_pos_1["trade_date"].apply(
        lambda x: dt.datetime.strptime(str(x), "%Y%m%d").date()
    )
    rjo_pos_2["trade_date"] = rjo_pos_2["trade_date"].apply(
        lambda x: dt.datetime.strptime(str(x), "%Y%m%d").date()
    )

    # fix quantity columns
    rjo_pos_1["quantity"] = rjo_pos_1.apply(multiply_rjo_positions, axis=1)
    rjo_pos_2["quantity"] = rjo_pos_2.apply(multiply_rjo_positions, axis=1)

    # drop buy_sell_code now
    rjo_pos_1.drop(columns=["buy_sell_code"], inplace=True)
    rjo_pos_2.drop(columns=["buy_sell_code"], inplace=True)

    def add_full_market_value_column_for_t2(rjo_row: pd.Series) -> int:
        mv_full = (
            rjo_row["quantity"]
            * rjo_row["close_price"]
            * rjo_row["multiplication_factor"]
        )
        return mv_full

    def add_conditional_market_value_column_for_t1(rjo_row: pd.Series) -> int:
        price_diff = rjo_row["close_price"] - rjo_row["formatted_trade_price"]
        if rjo_row["trade_date"] == rjo_file_1_date:
            mv_conditional = (
                rjo_row["quantity"] * price_diff * rjo_row["multiplication_factor"]
            )
        else:
            mv_conditional = (
                rjo_row["quantity"]
                * rjo_row["close_price"]
                * rjo_row["multiplication_factor"]
            )
        return mv_conditional

    # what do i need from here on out?
    # t2 file needs: netQty, MVFull
    # t1 file needs: netQty, MVFull, MVSmall, MVConditional
    rjo_pos_1["market_value_t1"] = rjo_pos_1.apply(
        add_conditional_market_value_column_for_t1, axis=1
    )

    rjo_pos_2["market_value_t2"] = rjo_pos_2.apply(
        add_full_market_value_column_for_t2, axis=1
    )

    # rest of the processing occurs in the main callback
    return rjo_pos_1, rjo_pos_2


# was working, now refactoring into the function above
def build_settlement_files_from_rjo_positions(
    portfolio_account_id: str,
    rjo_symbol_mappings: Dict[str, str],
) -> pd.DataFrame:
    """Pull and process RJO positions files to calculate per product PnL
    RJO positions and RJO prices to be used for calculation.
    """
    # 1 is most recent, 2 is second most recent
    rjo_pos_1, rjo_pos_1_name, rjo_pos_2, rjo_pos_2_name = (
        sftp_utils.fetch_two_latest_rjo_exports(r"UPETRADING_csvnpos_npos_%Y%m%d.csv")
    )

    # get date from file names
    rjo_file_1_date = dt.datetime.strptime(
        rjo_pos_1_name, "UPETRADING_csvnpos_npos_%Y%m%d.csv"
    ).date()
    rjo_file_2_date = dt.datetime.strptime(
        rjo_pos_2_name, "UPETRADING_csvnpos_npos_%Y%m%d.csv"
    ).date()

    rjo_pos_1.columns = (
        rjo_pos_1.columns.str.strip(" ").str.lower().str.replace(" ", "_")
    )
    rjo_pos_2.columns = (
        rjo_pos_2.columns.str.strip(" ").str.lower().str.replace(" ", "_")
    )

    # # filter for account -- will leave in for now, but later will mnake sense to do the whole sheet together
    # rjo_pos_1 = rjo_pos_1[rjo_pos_1["account_number"].eq(portfolio_account_id)]
    # rjo_pos_2 = rjo_pos_2[rjo_pos_2["account_number"].eq(portfolio_account_id)]

    # create a set of valid symbols from both the unique values in both files contract_code columns
    valid_rjo_symbols = set(
        [*rjo_pos_1["contract_code"].unique(), *rjo_pos_2["contract_code"].unique()]
    )

    # add georgia product symbol columnn to both dataframes
    rjo_pos_1["georgia_product_symbol"] = rjo_pos_1["contract_code"].apply(
        lambda x: rjo_symbol_mappings.get(x, x)
    )
    rjo_pos_2["georgia_product_symbol"] = rjo_pos_2["contract_code"].apply(
        lambda x: rjo_symbol_mappings.get(x, x)
    )

    rjo_pos_1 = rjo_pos_1.loc[
        rjo_pos_1["contract_code"].isin(valid_rjo_symbols)
        & rjo_pos_1["record_code"].eq("P"),
        [
            "security_desc_line_1",
            "contract_code",
            "close_price",
            "formatted_trade_price",
            "trade_date",
            "quantity",
            "buy_sell_code",
            "multiplication_factor",
            "account_number",
            "georgia_product_symbol",
        ],
    ]
    rjo_pos_2 = rjo_pos_2.loc[
        rjo_pos_2["contract_code"].isin(valid_rjo_symbols)
        & rjo_pos_2["record_code"].eq("P"),
        [
            "security_desc_line_1",
            "contract_code",
            "close_price",
            "trade_date",
            "quantity",
            "buy_sell_code",
            "multiplication_factor",
            "account_number",
            "georgia_product_symbol",
        ],
    ]

    # add settle date
    rjo_pos_1["settle_date"] = rjo_file_1_date
    rjo_pos_2["settle_date"] = rjo_file_2_date

    # convert trade_date to datetime
    rjo_pos_1["trade_date"] = rjo_pos_1["trade_date"].apply(
        lambda x: dt.datetime.strptime(str(x), "%Y%m%d").date()
    )
    rjo_pos_2["trade_date"] = rjo_pos_2["trade_date"].apply(
        lambda x: dt.datetime.strptime(str(x), "%Y%m%d").date()
    )

    # fix quantity columns
    rjo_pos_1["quantity"] = rjo_pos_1.apply(multiply_rjo_positions, axis=1)
    rjo_pos_2["quantity"] = rjo_pos_2.apply(multiply_rjo_positions, axis=1)

    # drop buy_sell_code now
    rjo_pos_1.drop(columns=["buy_sell_code"], inplace=True)
    rjo_pos_2.drop(columns=["buy_sell_code"], inplace=True)

    def add_full_market_value_column_for_t2(rjo_row: pd.Series) -> int:
        mv_full = (
            rjo_row["quantity"]
            * rjo_row["close_price"]
            * rjo_row["multiplication_factor"]
        )
        return mv_full

    def add_conditional_market_value_column_for_t1(rjo_row: pd.Series) -> int:
        price_diff = rjo_row["close_price"] - rjo_row["formatted_trade_price"]
        if rjo_row["trade_date"] == rjo_file_1_date:
            mv_conditional = (
                rjo_row["quantity"] * price_diff * rjo_row["multiplication_factor"]
            )
        else:
            mv_conditional = (
                rjo_row["quantity"]
                * rjo_row["close_price"]
                * rjo_row["multiplication_factor"]
            )
        return mv_conditional

    # what do i need from here on out?
    # t2 file needs: netQty, MVFull
    # t1 file needs: netQty, MVFull, MVSmall, MVConditional
    rjo_pos_1["market_value_t1"] = rjo_pos_1.apply(
        add_conditional_market_value_column_for_t1, axis=1
    )

    rjo_pos_2["market_value_t2"] = rjo_pos_2.apply(
        add_full_market_value_column_for_t2, axis=1
    )

    # filter both for account number
    rjo_pos_1 = rjo_pos_1[rjo_pos_1["account_number"].eq(portfolio_account_id)]
    rjo_pos_2 = rjo_pos_2[rjo_pos_2["account_number"].eq(portfolio_account_id)]

    # drop columns from rj0_pos_2 by keeping some
    rjo_pos_2_pnl = rjo_pos_2[
        [
            "account_number",
            "georgia_product_symbol",
            "market_value_t2",
        ]
    ]
    # lets sort out rjo_pos_2 first
    rjo_pos_2_pnl = rjo_pos_2_pnl.groupby(
        ["georgia_product_symbol"], as_index=False
    ).agg(
        {
            "market_value_t2": "sum",
        }
    )

    # sort out rjo_pos_1
    rjo_pos_1_pnl = rjo_pos_1[
        [
            "account_number",
            "georgia_product_symbol",
            "market_value_t1",
        ]
    ]
    rjo_pos_1_pnl = rjo_pos_1_pnl.groupby(
        ["georgia_product_symbol"], as_index=False
    ).agg(
        {
            "market_value_t1": "sum",
            # "account_number": "first",
        }
    )

    # add fees to rjo_pos_1 soon but first lets get a good table going joining these two
    # merge the dataframes on `georgia_product_symbol`
    merged_df = pd.merge(
        rjo_pos_1_pnl, rjo_pos_2_pnl, on="georgia_product_symbol", how="outer"
    )

    # Set `georgia_product_symbol` as index
    merged_df.set_index("georgia_product_symbol", inplace=True)

    # # add total row
    merged_df["Gross PnL"] = merged_df["market_value_t1"] - merged_df["market_value_t2"]

    # rename columns
    merged_df.rename(
        columns={
            "market_value_t1": f"MV @ {rjo_file_1_date}",
            "market_value_t2": f"MV @ {rjo_file_2_date}",
        },
        inplace=True,
    )

    # transpose the dataframe
    transposed_df = merged_df.transpose()

    # add a total column
    transposed_df["Total"] = transposed_df.sum(axis=1, numeric_only=True)

    # add a index column and make it appear first
    transposed_df.reset_index(inplace=True)
    transposed_df.rename(columns={"index": " "}, inplace=True)

    return transposed_df.round(0)


# currently unused, but may be useful in future
def build_dfs_for_general_pnl_utils_functions_from_rjo_positions(
    portfolio_account_id: str,
    rjo_symbol_mappings: Dict[str, str],
) -> pd.DataFrame:
    """The logic in this function was excised from build_settlement_files_from_rjo_positions()
    after deriving generalised RJO PnL algorithm that makes this and pnl_utils overkill for RJO to RJO PnL

    Bring this logic back to use for pnl_utils in the future
    """
    # rjo column for account numbher = "Account Number"

    # 1 is most recent, 2 is second most recent
    rjo_pos_1, rjo_pos_1_name, rjo_pos_2, rjo_pos_2_name = (
        sftp_utils.fetch_two_latest_rjo_exports(r"UPETRADING_csvnpos_npos_%Y%m%d.csv")
    )

    # get date from file names
    rjo_file_1_date = dt.datetime.strptime(
        rjo_pos_1_name, "UPETRADING_csvnpos_npos_%Y%m%d.csv"
    ).date()
    rjo_file_2_date = dt.datetime.strptime(
        rjo_pos_2_name, "UPETRADING_csvnpos_npos_%Y%m%d.csv"
    ).date()

    # rjo_file_1_date = dt.datetime(rjo_pos_1_name.split("_")[-1][:-4], r"%Y%m%d").date()
    # rjo_file_2_date = dt.datetime(rjo_pos_2_name.split("_")[-1][:-4], r"%Y%m%d").date()

    rjo_pos_1.columns = (
        rjo_pos_1.columns.str.strip(" ").str.lower().str.replace(" ", "_")
    )
    rjo_pos_2.columns = (
        rjo_pos_2.columns.str.strip(" ").str.lower().str.replace(" ", "_")
    )

    # filter for account
    rjo_pos_1 = rjo_pos_1[rjo_pos_1["account_number"].eq(portfolio_account_id)]
    rjo_pos_2 = rjo_pos_2[rjo_pos_2["account_number"].eq(portfolio_account_id)]

    # create a set of valid symbols from both the unique values in both files contract_code columns
    valid_rjo_symbols = set(
        [*rjo_pos_1["contract_code"].unique(), *rjo_pos_2["contract_code"].unique()]
    )

    # valid_rjo_symbols = list(rjo_symbol_mappings.keys())

    rjo_pos_1 = rjo_pos_1.loc[
        rjo_pos_1["contract_code"].isin(valid_rjo_symbols)
        & rjo_pos_1["record_code"].eq("P"),
        [
            "security_desc_line_1",
            "contract_code",
            "contract_month",
            "contract_day",
            "option_expire_date",
            "close_price",
            "trade_date",
            "quantity",
            "buy_sell_code",
            "multiplication_factor",
            "account_number",
        ],
    ]
    rjo_pos_2 = rjo_pos_2.loc[
        rjo_pos_2["contract_code"].isin(valid_rjo_symbols)
        & rjo_pos_2["record_code"].eq("P"),
        [
            "security_desc_line_1",
            "contract_code",
            "contract_month",
            "contract_day",
            "option_expire_date",
            "close_price",
            "trade_date",
            "quantity",
            "buy_sell_code",
            "multiplication_factor",
            "account_number",
        ],
    ]

    # add settle date
    rjo_pos_1["settle_date"] = rjo_file_1_date
    rjo_pos_2["settle_date"] = rjo_file_2_date

    # convert trade_date to datetime
    rjo_pos_1["trade_date"] = rjo_pos_1["trade_date"].apply(
        lambda x: dt.datetime.strptime(str(x), "%Y%m%d").date()
    )
    rjo_pos_2["trade_date"] = rjo_pos_2["trade_date"].apply(
        lambda x: dt.datetime.strptime(str(x), "%Y%m%d").date()
    )

    # fix quantity columns
    rjo_pos_1["quantity"] = rjo_pos_1.apply(multiply_rjo_positions, axis=1)
    rjo_pos_2["quantity"] = rjo_pos_2.apply(multiply_rjo_positions, axis=1)

    # drop buy_sell_code now
    rjo_pos_1.drop(columns=["buy_sell_code"], inplace=True)
    rjo_pos_2.drop(columns=["buy_sell_code"], inplace=True)

    ######From here on out we are building the 3 required DataFrames for the generalised pnl_utils function

    # build one of the 3 required pd.DataFrames necessary for the generalised pnl_utils function # DONE!!!
    dated_instrument_settlement_prices = (
        pnl_utils.build_dated_instrument_settlement_prices_from_rjo_positions(
            rjo_pos_1, rjo_pos_2, rjo_file_1_date, rjo_file_2_date
        )
    )
    # ic(dated_instrument_settlement_prices) # DONE!!!-----------------------------------
    #########################

    tm1_trades = rjo_pos_1[rjo_pos_1["trade_date"].eq(rjo_file_1_date)].copy()

    # rename columns to internal standard required for pnl_utils
    tm1_trades.rename(
        columns={
            "trade_date": "trade_datetime_utc",
            "security_desc_line_1": "instrument_symbol",
            "account_number": "portfolio_id",
            "multiplication_factor": "multiplier",
            "quantity": "quantity",
            "close_price": "price",
        },
        inplace=True,
    )
    # ic(tm1_trades)  # checked and DONE!!!------------------------------------------------------

    # ic(t1_traded_posies) # DONE!!!------------------------------------------------------

    # prepare files to be passed to build_tm1_to_2_dated_pos_file_from_rjo_positions pnl_utils function
    rjo_pos_1.rename(
        columns={
            "security_desc_line_1": "instrument_symbol",
            "account_number": "portfolio_id",
            "multiplication_factor": "multiplier",
            "quantity": "quantity",
            "settle_date": "position_date",
        },
        inplace=True,
    )
    rjo_pos_2.rename(
        columns={
            "security_desc_line_1": "instrument_symbol",
            "account_number": "portfolio_id",
            "multiplication_factor": "multiplier",
            "quantity": "quantity",
            "settle_date": "position_date",
        },
        inplace=True,
    )
    # remove trades from tm1 from rjo_pos1
    rjo_pos_1 = rjo_pos_1[~rjo_pos_1["trade_date"].eq(rjo_file_1_date)]

    # pass the two pos files to the pnl_utils function
    tm1_to_2_dated_pos = pnl_utils.build_tm1_to_2_dated_pos_file_from_rjo_positions(
        rjo_pos_1, rjo_pos_2
    )
    # ic(tm1_to_2_dated_pos)  # checked and DONE!!!---------------------------------------

    # all 3 files are now built, time to start passing it into the pnl_utils function and testing
    per_product_pnl = pnl_utils.get_per_instrument_portfolio_pnl(
        tm1_to_2_dated_pos, tm1_trades, dated_instrument_settlement_prices
    )

    return per_product_pnl


def add_estimated_fees_to_portfolio(rjo_dth_1: pd.DataFrame) -> pd.DataFrame:
    """Add estimated fees to the positions DataFrame
    Each portfolio with its own fee structure
    LME Portfolios: qty * 3.4
    XEXT Portfolios: qty * 1.38
    ICE Portfolios: qty * (2.37 + giveUpFees*0.1 + crossFees*0.4)

    """
    est_fees = rjo_dth_1.copy()

    # Add a new column for estimated fees
    est_fees["estimated_fees"] = 0

    # Update estimated fees based on portfolio ID
    for idx, row in est_fees.iterrows():
        portfolio_id = row["Account Number"]
        quantity = row["Quantity"]
        if portfolio_id in ["UPLME", "UPE03"]:
            est_fees.at[idx, "estimated_fees"] = quantity * 3.4
        elif portfolio_id == "UPENX":
            est_fees.at[idx, "estimated_fees"] = quantity * 1.38
        elif portfolio_id == "UPICE":
            est_fees.at[idx, "estimated_fees"] = quantity * 2.87
        else:
            est_fees.at[idx, "estimated_fees"] = 0

    return est_fees
