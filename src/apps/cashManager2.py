import datetime as dt
import os, io
import pickle
from typing import Dict, Set, List

import dash_bootstrap_components as dbc
import numpy as np
import pandas as pd
from icecream import ic

import sftp_utils
from dash import callback_context, dcc, html
from dash import dash_table as dtable
from dash.dependencies import Input, Output
from data_connections import conn, shared_engine, shared_session
from parts import dev_key_redis_append, get_first_wednesday, topMenu
from sqlalchemy.dialects.postgresql import insert
import sqlalchemy
from upedata import dynamic_data as upe_dynamic
from upedata import static_data as upe_static


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
        html.Div(id="hidden-refresh", style={"display": "none"}),
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.Button("Refresh", id="refresh-button-2", n_clicks=0),
                    ]
                ),
            ],
        ),
        html.Div(id="output-cash-button-2"),
        dcc.Loading(
            id="loading-3",
            children=[
                html.Div(
                    [html.Div(id="rjo-filename-2", children="Cash Manager Loading...")]
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
        grey_divider,
        # push the closing price rec down a bit
        html.Div([html.Br(), html.Br(), html.Br()]),
        html.Div(
            id="closePrice-rec-filestring-2", children="Closing Price Rec Loading... "
        ),
        dcc.Loading(
            id="loading-5",
            children=[html.Div([html.Div(id="closePrice-rec-table-2")])],
            type="circle",
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
                    {"name": str(col_name), "id": str(col_name)}
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

        ## NEED TO FIX THIS SITUATION !!!! the return of the string/pd.df situatiin is not working as inhtended !!!!
        if ttl > 0:
            clo_data = conn.get(
                CLOSING_PRICE_REC_REDIS_BASE_LOCATION + dev_key_redis_append
            )

            try:
                clo_data = clo_data.decode()
            except Exception as e:
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
                if x.display_name != "Error" and x.display_name != "Backbook"
            ]

        return portfolio_options, portfolio_options[0]["value"]

    # NEW GENERAL PNL - get this working for LME
    @app.callback(
        Output("new-pnl-filestring", "children"),
        Output("pnl-table-output", "children"),
        [Input("pnl-refresh", "n_clicks")],
        [Input("pnl-portfolio-dropdown", "value")],
    )
    def new_general_pnl(n, portfolio_id):
        # print(ctx.triggered[0]["prop_id"].split(".")[0])
        trig_id = callback_context.triggered[0]["prop_id"].split(".")[0]

        # first check if pnl data is in redis
        ttl = conn.ttl("new_general_pnl" + dev_key_redis_append)
        if trig_id == "pnl-refresh":
            ttl = 0

        if ttl > 0:
            pnl_data = conn.get(INTERNAL_PNL_REDIS_BASE_LOCATION + dev_key_redis_append)
            pnl_data = pd.read_pickle(io.BytesIO(pnl_data))
            file_string = conn.get(
                INTERNAL_PNL_REDIS_BASE_LOCATION + "_filestring" + dev_key_redis_append
            )
            file_string = pd.read_pickle(io.BytesIO(file_string))

            if portfolio_id != "all":
                pnl_data = pnl_data[pnl_data["portfolio_id"] == int(portfolio_id)]
            else:
                # aggregate pnl data
                pnl_data = (
                    pnl_data.groupby(["pnl_date", "metal", "product_symbol", "source"])
                    .sum(numeric_only=True)
                    .reset_index()
                )

            # format df for frontend
            table = format_pnl_for_frontend(pnl_data)

            return file_string, table

        # fiddle with the existing functions
        # fiddle with it here

        ########### my work from here

        # get mappings from database
        with shared_engine.connect() as cnxn:
            stmt1 = sqlalchemy.text(
                "SELECT platform_account_id FROM platform_account_portfolio_associations WHERE portfolio_id = :portfolio_id AND platform = :platform"
            )
            stmt2 = sqlalchemy.text(
                "SELECT platform_symbol, product_symbol FROM third_party_product_symbols WHERE platform_name = :platform_name"
            )
            rjo_portfolio_id = cnxn.execute(
                stmt1, {"platform": "RJO", "portfolio_id": portfolio_id}
            ).scalar_one_or_none()
            result = cnxn.execute(stmt2, {"platform_name": "RJO"})
        ic(rjo_portfolio_id)

        rjo_symbol_map = {res.platform_symbol: res.product_symbol for res in result}
        ic(rjo_symbol_map)

        work_in_progress = build_settlement_file_from_rjo_positions(
            rjo_portfolio_id, rjo_symbol_map
        )
        ic(work_in_progress)

        return "pnl table under progress", pd.DataFrame().to_dict("records")


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
            print("No price found for: ", row["instrument_symbol"])
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
            print("No price found for: ", row["instrument_symbol"])
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
            print("updating net quantity for ", symbol)
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
                print(f"invalid date format for {symbol}")
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
                print(f"invalid date code for {symbol}")
            except ValueError:
                # if invalid date, set to expired date to be filtered out
                expiry = dt.date(2020, 1, 1)
                print(f"invalid date code for {symbol}")
        return expiry

    try:
        expiry = dt.datetime.strptime(info[2], r"%y-%m-%d").date()
    except ValueError:
        expiry = dt.date(2020, 1, 1)
        print(f"invalid date code for {symbol}")
    return expiry


def build_georgia_symbol_from_rjo_overnight(rjo_overnight_row):
    pass


def build_settlement_file_from_rjo_positions(
    portfolio_account_id: str,
    rjo_symbol_mappings: Dict[str, str],
) -> pd.DataFrame:
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
    ic(rjo_file_1_date, rjo_file_2_date)

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

    ic(valid_rjo_symbols)

    # valid_rjo_symbols = list(rjo_symbol_mappings.keys())

    rjo_pos_1 = (
        rjo_pos_1.loc[
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
            ],
        ]  # .groupby("security_desc_line_1")
        # .first()
    )
    rjo_pos_2 = (
        rjo_pos_2.loc[
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
            ],
        ]  # .groupby("security_desc_line_1")
        # .first()
    )

    rjo_pos_1["settle_date"] = rjo_file_1_date
    rjo_pos_2["settle_date"] = rjo_file_2_date

    rjo_pos_1["trade_date"] = rjo_pos_1["trade_date"].apply(
        lambda x: dt.datetime.strptime(str(x), "%Y%m%d").date()
    )
    rjo_pos_2["trade_date"] = rjo_pos_2["trade_date"].apply(
        lambda x: dt.datetime.strptime(str(x), "%Y%m%d").date()
    )

    # before I start stacking the dfs, this is a good place to build the final tm1-pos file from rjo_pos_1
    # in format:
    # trade_datetime_utc, instrument_symbol, portfolio_id, multiplier, quantity, price
    # first just filter rjo_pos_1 for trades on the day

    # THIS IS WHERE I GOT TO before switching branches for now

    #############################
    # start of the stacking process!!!
    stacked_settlement_data = pd.concat(
        [rjo_pos_1, rjo_pos_2], axis=0, ignore_index=True
    )

    # split the df into two, those with trade_date = rjo_file_1_date and those with trade_date = anything else
    t1_traded_posies = stacked_settlement_data[
        stacked_settlement_data["trade_date"].eq(rjo_file_1_date)
    ]
    t2_traded_posies = stacked_settlement_data[
        stacked_settlement_data["trade_date"].ne(rjo_file_1_date)
    ]

    return stacked_settlement_data

    ic(stacked_settlement_data)

    # do the rest from here myself

    stacked_settlement_data = stacked_settlement_data[
        (stacked_settlement_data["contract_day"].isna())
        & (~stacked_settlement_data["option_expire_date"].eq(0))  # ambiguous
    ]

    stacked_settlement_data.loc[
        (~stacked_settlement_data["option_expire_date"].eq(0)), "expiry_date"
    ] = stacked_settlement_data.loc[
        (~stacked_settlement_data["option_expire_date"].eq(0)), "option_expiry_date"
    ].apply(
        str
    )

    stacked_settlement_data.loc[
        stacked_settlement_data["option_expire_date"].eq(0), "expiry_date"
    ] = stacked_settlement_data.loc[
        stacked_settlement_data["option_expire_date"].eq(0), "option_expiry_date"
    ].apply(
        str
    )

    stacked_settlement_data["georgia_symbol"] = rjo_pos_1.apply(
        build_georgia_symbol_from_rjo_overnight, axis=0
    )
    ic(stacked_settlement_data)

    return 1


# CLO1
# CLO2

# Pos1
# Pos2

# Dropdown on frontned
# Symbol mappings from database
