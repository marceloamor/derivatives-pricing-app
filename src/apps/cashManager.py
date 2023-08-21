from dash.dependencies import Input, Output, State
from dash import dcc
from dash import dcc, html
from dash import dash_table as dtable
import dash_bootstrap_components as dbc

from parts import topMenu
import sftp_utils
from data_connections import Session
import upestatic

from sqlalchemy.dialects.postgresql import insert
import traceback
import pandas as pd
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
        html.Div(id="rjo-filename", children="Cash Manager Loading..."),
        dcc.Loading(
            id="loading-3",
            children=[html.Div([html.Div(id="output-cash-button")])],
            type="circle",
        ),
        html.Br(),
        html.Div(id="pnl-filestring1", children="PnL Rec Loading..."),
        html.Div(id="pnl-filestring2", children=" "),
        dcc.Loading(
            id="loading-4",
            children=[html.Div(id="pnl-rec-table")],
            type="circle",
        ),
        html.Br(),
        html.Div(
            id="closePrice-rec-filestring", children="Closing Price Rec Loading... "
        ),
        dcc.Loading(
            id="loading-5",
            children=[html.Div([html.Div(id="closePrice-rec-table")])],
            type="circle",
        ),
    ]
)


def initialise_callbacks(app):
    # cash manager page, dont run on page load
    @app.callback(
        Output("output-cash-button", "children"),
        Output("rjo-filename", "children"),
        [Input("refresh-button", "n_clicks")],
    )
    def cashManager(n):
        # get latest rjo pos exports
        (latest_rjo_df, latest_rjo_filename) = sftp_utils.fetch_latest_rjo_export(
            "UPETRADING_csvnmny_nmny_%Y%m%d.csv"
        )

        latest_rjo_df = latest_rjo_df.reset_index()
        latest_rjo_df = latest_rjo_df[latest_rjo_df["Record Code"] == "M"]

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
        return cash_table, filename_string

    # pnl table page
    @app.callback(
        Output("pnl-rec-table", "children"),
        Output("pnl-filestring1", "children"),
        Output("pnl-filestring2", "children"),
        [Input("refresh-button", "n_clicks")],
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

        # filter out rows with empty card numbers TESTING
        # t1 = t1[t1["Card Number"].str.len() > 0]
        # t2 = t2[t2["Card Number"].str.len() > 0]

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
        t1_filestring = "T-1 filename: " + t1_filename
        t2_filestring = "T-2 filename: " + t2_filename

        return table, t1_filestring, t2_filestring

    # closing price rec
    @app.callback(
        Output("closePrice-rec-filestring", "children"),
        Output("closePrice-rec-table", "children"),
        [Input("refresh-button", "n_clicks")],
    )
    def cashManager(n):
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
                "Security Desc Line 1": "Instrument",
                "LME Close Price": "LME Close Price",
                "Close Price": "RJO Close Price",
            }
        )
        if rjo_df.empty:
            return "No closing price differences found between {}".format(
                clo_filename
            ), "and {}".format(rjo_df_filename)
        else:
            clo_table = dtable.DataTable(
                data=rjo_df.to_dict("records"),
                columns=[
                    {"name": str(col_name), "id": str(col_name)}
                    for col_name in rjo_df.columns
                ],
                style_data_conditional=[
                    {
                        "if": {
                            "column_id": "index",
                        },
                        "backgroundColor": "lightgrey",
                    }
                ],
            )

        filename_string = "Found these differences between {} and {}".format(
            clo_filename, rjo_df_filename
        )
        return clo_table, filename_string


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
        insert(upestatic.ExternalPnL)
        .values(**results_dict)
        .on_conflict_do_update(
            index_elements=["pnl_date", "product_symbol", "source"],
            set_=results_dict,
        )
    )

    with Session() as session:
        session.execute(stmt)
        session.commit()

    return results
