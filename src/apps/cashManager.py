from dash.dependencies import Input, Output, State
from dash import dcc
from dash import dcc, html
from dash import dash_table as dtable
import pandas as pd
import dash_bootstrap_components as dbc

from parts import topMenu
import sftp_utils

import traceback
import datetime as dt
import numpy as np


metals_dict = {
    "AU": "Aluminium",
    "CP": "Copper",
    "BN": "Nickel",
    "LD": "Lead",
    "L8": "Zinc",
}

# layout for dataload page
layout = html.Div(
    [
        topMenu("Cash Manager"),
        html.Div(
            dbc.Button(
                "refresh", id="refresh-button", n_clicks=0, style={"display": "none"}
            )
        ),
        html.Div(id="rjo-filename", children="RJO filename: "),
        dcc.Loading(
            id="loading-2",
            children=[html.Div([html.Div(id="output-cash-button")])],
            type="circle",
        ),
        html.Br(),
        html.Div(id="pnl-filestring", children="Filename: "),
        dcc.Loading(
            id="loading-2",
            children=[html.Div([html.Div(id="pnl-rec-table")])],
            type="circle",
        ),
        dcc.Store(id="lme-pnl"),
    ]
)


def initialise_callbacks(app):
    # cash manager page, dont run on page load
    @app.callback(
        Output("output-cash-button", "children"),
        Output("rjo-filename", "children"),
        Output("lme-pnl", "data"),
        [Input("refresh-button", "n_clicks")],
    )
    def cashManager(n):
        # on click do this
        # filenames = html.Div()
        # table = html.Div()
        # if n >= 0:
        # get latest sol3 and rjo pos exports
        (latest_rjo_df, latest_rjo_filename) = sftp_utils.fetch_latest_rjo_export(
            "UPETRADING_csvnmny_nmny_%Y%m%d.csv"
        )

        latest_rjo_df = latest_rjo_df.reset_index()
        latest_rjo_df = latest_rjo_df[latest_rjo_df["Record Code"] == "M"]

        # extract lme pnl before further trasfpormation
        lme_df = latest_rjo_df[latest_rjo_df["Account Number"] == "UPLME"]
        lme_df = lme_df[lme_df["Account Type Currency Symbol"] == "USD"]
        lme_df["PNL"] = (
            lme_df["Liquidating Value"] - lme_df["Previous Liquidating Value"]
        )
        lme_pnl = lme_df["PNL"].iloc[0]
        # print("lme pnl: ", lme_pnl)

        # round all integers to 0dp
        latest_rjo_df = latest_rjo_df.round(0)

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

        latest_rjo_df.reset_index(inplace=True)

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
        return cash_table, filename_string, lme_pnl

        # return table, filenames, lme_pnl

    # cash manager page
    @app.callback(
        Output("pnl-rec-table", "children"),
        Output("pnl-filestring", "children"),
        [Input("lme-pnl", "data")],
    )
    def pnlManager(n):
        # fetch latest rjo pos files
        (t1, t1_filename, t2, t2_filename) = sftp_utils.fetch_two_latest_rjo_exports(
            "UPETRADING_csvnpos_npos_%Y%m%d.csv"
        )

        # filter for exchange and record code
        t1 = t1[t1["Bloomberg Exch Code"] == "LME"]
        t1 = t1[t1["Record Code"] == "P"]

        t2 = t2[t2["Bloomberg Exch Code"] == "LME"]
        t2 = t2[t2["Record Code"] == "P"]

        t1["Trade Date"] = t1["Trade Date"].astype(int)
        t2["Trade Date"] = t2["Trade Date"].astype(int)

        # get most recent file date from t1
        yesterday = str(
            dt.datetime.strptime(
                t1_filename, "UPETRADING_csvnpos_npos_%Y%m%d.csv"
            ).strftime("%Y%m%d")
        )
        # dynamically calculate pnl for each metal
        unique_products_t1 = t1["Contract Code"].unique()
        unique_products_t2 = t2["Contract Code"].unique()
        product_list = list(set(unique_products_t1) | set(unique_products_t2))

        pnl_per_product = {}
        for product in product_list:
            # handle unexepected products in rec
            try:
                productName = metals_dict[product]
            except:
                print("Unknown product in pnl calculation: ", product)
                productName = product

            pnl_per_product[productName] = get_product_pnl(t1, t2, yesterday, product)

        # sort products for consistensy
        sorted_product_names = sorted(pnl_per_product.keys())

        row_names = [
            "T-1 Trades PnL",
            "Pos PnL",
            "Gross PnL",
            "Estimated Fees",
            "Net PnL",
        ]
        data = pd.DataFrame.from_dict(
            pnl_per_product, orient="index", columns=sorted_product_names
        )
        data = data.T
        data.index = row_names
        data.index.name = "Source"

        data["Total"] = data.sum(axis=1)
        data = data.round(2)

        data.reset_index(inplace=True)

        table = dtable.DataTable(
            data=data.to_dict("records"),
            columns=[
                # {"name": "Source", "id": "index"},
                {"name": col, "id": col}
                for col in data.columns
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
        # build filename string for frontend
        # change this to two different components to display on new lines
        filename_string = (
            "t1_filename: " + t1_filename + " ------- t2_filename: " + t2_filename
        )

        return table, filename_string


def recRJOstaticPNL():
    """Calculates PnL on RJO trades and positions and compares to RJO reported PnL from cash file.
    - Pulls reported PnL from cash file
    - Pulls RJO positions from last two days of position file
    - For each metal in positions:
        - Separates T-1 trades and calculates PnL from trade price to close price
        - Matches T-1 trades to T-2 trades and calculates PnL from T-2 close price to T-1 close price
        - Calculates estimated fees on the day
    """

    # # pull most recent rjo position files
    # (
    #     t1,
    #     t1_filename,
    # ) = sftp_utils.fetch_latest_rjo_export("UPETRADING_csvnpos_npos_%Y%m%d.csv")

    # # fetch second latest rjo file
    # (t2, t2_filename) = sftp_utils.fetch_2nd_latest_rjo_export(
    #     "UPETRADING_csvnpos_npos_%Y%m%d.csv"
    # )

    t1["Trade Date"] = t1["Trade Date"].astype(int)
    t2["Trade Date"] = t2["Trade Date"].astype(int)

    # metals_dict = {
    #     "AU": "Aluminium",
    #     "CP": "Copper",
    #     "BN": "Nickel",
    #     "LD": "Lead",
    #     "L8": "Zinc",
    # }

    metals_pnl = {}

    # for metal in metals_dict.keys():
    # filter for metal

    for metal in metals_dict.keys():
        metals_pnl[metal] = perMetalPnL(metal, t1, t2, yesterday)
    print(metals_pnl)
    # # build pnl table for frontend
    # table = pd.DataFrame(
    #     [
    #         ["T-1 Trades PnL", tradesPNL],
    #         ["Pos PnL", matchedPNL],
    #         ["Gross PnL", totalPNL],
    #         ["Estimated Fees", est_fees],
    #         ["Net PnL", totalPNL - est_fees],
    #         ["Reported PnL", reported_pnl],
    #         ["PnL Diff", (totalPNL - est_fees - reported_pnl).round(2)],
    #     ],
    #     columns=["source", "pnl"],
    # )

    # build filename string for frontend
    filename_string = (
        "t1_filename: "
        + t1_filename
        + "t2_filename: "
        + t2_filename
        + "cash_filename: "
        + cash_filename
    )
    return (table, filename_string)


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
    # yday_trades["Vol"] = yday_trades.apply(
    #     lambda row: row["Quantity"]
    #     if int(row["Buy Sell Code"]) == 1
    #     else -int(row["Quantity"]),
    #     axis=1,
    # )
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
        ]
    ]

    # calculate PnL on these positions
    matched["PnL"] = matched["Market Value_t1"] - matched["Market Value_t2"]

    # ensure relevant columns are numeric for calculations
    matched["Quantity"] = pd.to_numeric(matched["Quantity"], errors="coerce")
    matched["Buy Sell Code"] = pd.to_numeric(matched["Buy Sell Code"], errors="coerce")

    # # calculate positions and gross pnl
    # matched["Vol"] = matched.apply(
    #     lambda row: int(row["Quantity"])
    #     if int(row["Buy Sell Code"]) == 1
    #     else int(row["Quantity"]) * -1,
    #     axis=1,
    # )
    # matched["PnL"] = (
    #     matched["PriceDiff"] * matched["Vol"] * matched["Multiplication Factor"]
    # )
    matchedPNL = (matched["PnL"].sum()).round(2)
    totalPNL = (tradesPNL + matchedPNL).round(2)
    netPNL = (totalPNL - est_fees).round(2)

    # t2_unmatched["Vol"] = t2_unmatched.apply(
    #     lambda row: float(row["Quantity"])
    #     if int(row["Buy Sell Code"]) == 1
    #     else -float(row["Quantity"]),
    #     axis=1,
    # )
    # # print(t2_unmatched["Vol"].sum())  # should be 0
    # t1_unmatched["Vol"] = t1_unmatched.apply(
    #     lambda row: float(row["Quantity"])
    #     if int(row["Buy Sell Code"]) == 1
    #     else -float(row["Quantity"]),
    #     axis=1,
    # )
    # print(t1_unmatched["Vol"].sum())  # should be 0

    results = [tradesPNL, matchedPNL, totalPNL, est_fees, netPNL]

    return results
