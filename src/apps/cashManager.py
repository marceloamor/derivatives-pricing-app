from dash.dependencies import Input, Output, State
from dash import dcc
from dash import dcc, html
from dash import dash_table as dtable
import dash_bootstrap_components as dbc

from parts import topMenu
import sftp_utils
from data_connections import Session, conn, engine
import upestatic

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text
import traceback
import pandas as pd
import datetime as dt
import numpy as np
import pickle
import os

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

options_list = [
    {"label": "All LME Portfolios", "value": "all"},
    {"label": "1- LME General", "value": "1"},
    {"label": "2- LME Carry", "value": "2"},
]

portfolio_dropdown = dbc.Row(
    [
        dbc.Col(
            dcc.Dropdown(
                id="portfolio-dropdown",
                options=options_list,
                value=options_list[0]["value"],
            ),
            width={"size": 3},
        ),
        dbc.Col(
            html.Button("Refresh", id="submit-button", n_clicks=0),
            width={"size": 2},
        ),
    ]
)

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
        portfolio_dropdown,
        # dcc.Dropdown(
        #     id="portfolio-dropdown",
        #     value=options_list[0]["value"],
        #     options=options_list,
        # ),
        dcc.Loading(
            id="loading-6",
            children=[html.Div([html.Div(id="internal-pnl-table")])],
            type="circle",
        ),
        html.Div(id="internal-pnl-filestring", children="Internal PnL Loading... "),
        html.Br(),
        html.Div(
            id="closePrice-rec-filestring", children="Closing Price Rec Loading... "
        ),
        dcc.Loading(
            id="loading-5",
            children=[html.Div([html.Div(id="closePrice-rec-table")])],
            type="circle",
        ),
        dbc.Row(
            [
                html.Div(id="pnl-filestring1", children="PnL Rec Loading..."),
            ]
        ),
        html.Div(id="pnl-filestring2", children=" "),
        dcc.Loading(
            id="loading-4",
            children=[html.Div(id="pnl-rec-table")],
            type="circle",
        ),
        # html.Br(),
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
        prevent_initial_call=True,
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
        prevent_initial_call=True,
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

    # internal pnl tool - georgia positions, lme prices
    @app.callback(
        Output("internal-pnl-filestring", "children"),
        Output("internal-pnl-table", "children"),
        [Input("refresh-button", "n_clicks")],
        [Input("portfolio-dropdown", "value")],
    )
    def internalPnL(n, portfolio_id):
        # first check if pnl data is in redis
        ttl = conn.ttl("internal_pnl")

        if ttl < 1:
            print("data in redis!")
            pnl_data = pickle.loads(conn.get("internal_pnl"))
            if portfolio_id != "all":
                pnl_data = pnl_data[pnl_data["portfolio_id"] == int(portfolio_id)]
            else:
                # aggregate pnl data
                pnl_data = (
                    pnl_data.groupby(["pnl_date", "metal", "product_symbol", "source"])
                    .sum()
                    .reset_index()
                )
                # turn df into dash table
                # Pivot the DataFrame to have metals as columns and set "metal" as the index

            # df = pnl_data.drop(
            #     columns=["pnl_date", "product_symbol", "portfolio_id", "source"]
            # )
            # keep instead of drop
            df = pnl_data[
                ["metal", "t1_trades", "pos_pnl", "gross_pnl", "est_fees", "net_pnl"]
            ]
            # Set the index to "metal" column
            df.set_index("metal", inplace=True)

            # Transpose the DataFrame
            pivot_df = df.T.reset_index()

            # add a total column
            pivot_df["Total"] = pivot_df.sum(axis=1, numeric_only=True)
            pivot_df = pivot_df.round(2)

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

            return "Internal PnL", table

        else:  # if not, calculate pnl
            print("hello, i guess lets do some calculations!")

        # get latest CLO files and georgia pos/trades data
        (
            clo_t1,
            clo_t1_name,
            clo_t2,
            clo_t2_name,
        ) = sftp_utils.fetch_two_latest_rjo_exports("%Y%m%d_CLO_r.csv", "/LMEPrices")

        # get date object from file names
        t1_date = dt.datetime.strptime(clo_t1_name, "%Y%m%d_CLO_r.csv").date()
        t2_date = dt.datetime.strptime(clo_t2_name, "%Y%m%d_CLO_r.csv").date()
        print(t1_date, t2_date)
        # this is a random

        # get georgia pos
        with engine.connect() as cnxn:
            positions = pd.read_sql_table("positions", cnxn)
            stmt = f"SELECT * FROM trades WHERE deleted = false and trade_datetime_utc > '{t1_date}'"
            trades = pd.read_sql(stmt, cnxn)

        # format data
        trades["date"] = trades["trade_datetime_utc"].dt.date
        positions["instrument_symbol"] = positions["instrument_symbol"].str.upper()

        # this is where the ideal split woul be
        pnl_per_portfolio = {}

        def calc_pnl_per_portfolio(portfolio_id):
            positions_portfolio = positions[positions["portfolio_id"] == portfolio_id]
            trades_portfolio = trades[trades["portfolio_id"] == portfolio_id]

            def calc_pnl_per_metal(metal):
                # filter pos and trades for metal
                positions_metal = positions_portfolio[
                    positions_portfolio["instrument_symbol"].str.contains(metal)
                ]
                trades_metal = trades_portfolio[
                    trades_portfolio["instrument_symbol"].str.contains(metal)
                ]

                # calc t1_trades pnl and est_fees
                t1_trades = trades_metal[trades_metal["date"] == t1_date]
                t0_trades = trades_metal[trades_metal["date"] != t1_date]

                if not t1_trades.empty:
                    print("t1 trades:")
                    t1_trades = get_prices_from_clo(t1_trades, clo_t1, "t1")
                    t1_trades["price_diff"] = t1_trades["t1_price"] - t1_trades["price"]
                    t1_trades["mult"] = t1_trades["instrument_symbol"].apply(
                        lambda x: 25 if x[:3] != "LND" else 6
                    )
                    t1_trades["pnl"] = (
                        t1_trades["price_diff"]
                        * t1_trades["quantity"]
                        * t1_trades["mult"]
                    )
                    est_fees = t1_trades["quantity"].abs().sum() * 3.4
                    t1_trades_pnl = t1_trades["pnl"].sum()
                else:
                    t1_trades_pnl = 0
                    est_fees = 0

                # get positions as they were at t2 close
                aggregated_trades = (
                    trades.groupby("instrument_symbol")["quantity"].sum().reset_index()
                )
                net_new_trades = dict(
                    zip(
                        aggregated_trades["instrument_symbol"],
                        aggregated_trades["quantity"],
                    )
                )
                t2_positions = positions_metal.copy()
                # update net_quantity for each product traded in last two days
                for index, row in t2_positions.iterrows():
                    symbol = row["instrument_symbol"]
                    if symbol in net_new_trades:
                        t2_positions.at[index, "net_quantity"] -= net_new_trades[symbol]
                t2_positions = t2_positions[t2_positions["net_quantity"] != 0]
                print("t2pos:", t2_positions)
                if not t2_positions.empty:
                    # get t1 and t2 settle prices from lme files
                    print("t2 positions:")
                    t2_positions = get_prices_from_clo(t2_positions, clo_t2, "t2")
                    print("t1 positions:")
                    t2_positions = get_prices_from_clo(t2_positions, clo_t1, "t1")

                    # calculate pnl on positions
                    t2_positions["price_diff"] = (
                        t2_positions["t1_price"] - t2_positions["t2_price"]
                    )
                    # set multiplier manually!!
                    t2_positions["mult"] = t2_positions["instrument_symbol"].apply(
                        lambda x: 25 if x[:3] != "LND" else 6
                    )
                    t2_positions["pnl"] = (
                        t2_positions["price_diff"]
                        * t2_positions["net_quantity"]
                        * t2_positions["mult"]
                    )
                    pos_pnl = t2_positions["pnl"].sum()
                else:
                    pos_pnl = 0
                gross_pnl = t1_trades_pnl + pos_pnl
                net_pnl = gross_pnl - est_fees

                results = [t1_trades_pnl, pos_pnl, gross_pnl, est_fees, net_pnl]
                print(metal, results)
                return results

            metals = {
                "LZH": "Zinc",
                "LND": "Nickel",
                "LAD": "Aluminium",
                "LCU": "Copper",
                "PBD": "Lead",
            }

            pnl_per_product = {}
            for symbol, metal in metals.items():
                # handle unexepected products in rec

                pnl_per_product[metal] = calc_pnl_per_metal(symbol)
                # print(metal, pnl_per_product[metal])

            return pnl_per_product

        # portfolios = [1, 2]

        portfolio_1 = calc_pnl_per_portfolio(1)
        portfolio_2 = calc_pnl_per_portfolio(2)

        print(portfolio_1)
        print(portfolio_2)

        # Convert the dictionaries into dataframes with "source" column added
        df1 = pd.DataFrame.from_dict(
            portfolio_1,
            orient="index",
            columns=["t1_trades", "pos_pnl", "gross_pnl", "est_fees", "net_pnl"],
        )
        df1["metal"] = df1.index
        df1["source"] = "Georgia"
        df1["portfolio_id"] = 1

        df2 = pd.DataFrame.from_dict(
            portfolio_2,
            orient="index",
            columns=["t1_trades", "pos_pnl", "gross_pnl", "est_fees", "net_pnl"],
        )
        df2["metal"] = df2.index
        df2["source"] = "Georgia"
        df2["portfolio_id"] = 2

        # Concatenate the dataframes
        final_df = pd.concat([df1, df2], ignore_index=True)
        final_df["pnl_date"] = t1_date

        metals_symbol_dict = {
            "Aluminium": "xlme-lad-usd",
            "Copper": "xlme-lcu-usd",
            "Nickel": "xlme-lnd-usd",
            "Lead": "xlme-pbd-usd",
            "Zinc": "xlme-lzh-usd",
        }

        # Reorder the columns
        final_df = final_df[
            [
                "pnl_date",
                "portfolio_id",
                "metal",
                "source",
                "t1_trades",
                "pos_pnl",
                "gross_pnl",
                "est_fees",
                "net_pnl",
            ]
        ]
        final_df["product_symbol"] = final_df["metal"].map(metals_symbol_dict)

        print(final_df)

        # ex keyword argument in conn.set , check docs
        # first run of day store in redis, 10hr timeout
        # store pnl data in redis for 12hrs to avoid re-running pnl calculations
        conn.set("internal_pnl", pickle.dumps(final_df), ex=60 * 60 * 12)

        # send to postgres as well

        return "hello", "world"


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
            print("No price found for: ", row["instrument_symbol"])
            price = 0
        else:
            price = clo_filtered.iloc[0]["PRICE"]

        return price

    col_name = day + "_price"
    t2_pos[col_name] = t2_pos.apply(get_price_from_clo, axis=1)

    return t2_pos


def send_pnl_to_dbs(port_1, port_2):
    """Sends pnl data to postgres db and redis timed key"""
    # build and send data to postgres
    # data to send: date, product, t1-trades, pos_pnl, gross_pnl
    date = dt.datetime.strptime(yesterday, "%Y%m%d").date()

    results = [tradesPNL, matchedPNL, totalPNL, est_fees, netPNL]

    metals_dict_db = {
        "Aluminium": "xlme-lad-usd",
        "Copper": "xlme-lcu-usd",
        "Nickel": "xlme-lnd-usd",
        "Lead": "xlme-pbd-usd",
        "Zinc": "xlme-lzh-usd",
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
