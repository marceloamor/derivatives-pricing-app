import io, base64
from dash.dependencies import Input, Output, State
from dash import dcc
from dash import dcc, html
from dash import dash_table as dtable
import pandas as pd
import dash_bootstrap_components as dbc

from parts import settleVolsProcess
from data_connections import PostGresEngine, conn

from parts import (
    topMenu,
    recRJO,
    rec_britannia_mir13,
    rec_sol3_cme_pos_bgm_mir_14,
    rec_sol3_rjo_cme_pos,
    rjo_to_sol3_hash,
    sendEURVolsToPostgres,
)

import sftp_utils
import upestatic
import traceback
import datetime as dt


# options for file type dropdown
fileOptions = [
    {"label": "LME Vols", "value": "lme_vols"},
    {"label": "EUR Vols", "value": "eur_vols"},
    {"label": "Rec LME Positions", "value": "rec_lme_pos"},
    {"label": "Rec CME Positions", "value": "rec_cme_pos"},
    {"label": "Rec Euronext Positions", "value": "rec_euro_pos"},
    {"label": "Rec LME/RJO PnL", "value": "rec_settle_prices"},
]

# layout for dataload page
layout = html.Div(
    [
        topMenu("Data Load"),
        dcc.Dropdown(
            id="file_type", value=fileOptions[0]["value"], options=fileOptions
        ),
        dbc.Button("rec-button", id="rec-button", n_clicks=0),
        dcc.Upload(
            id="upload-data",
            children=html.Div(["Drag and Drop or ", html.A("Select Vols")]),
            style={
                "width": "100%",
                "height": "60px",
                "lineHeight": "60px",
                "borderWidth": "1px",
                "borderStyle": "dashed",
                "borderRadius": "5px",
                "textAlign": "center",
                "margin": "10px",
            },
            # Allow multiple files to be uploaded
            multiple=True,
        ),
        html.Div(id="output-data-upload"),
        html.Div(id="sol3-rjo-filenames"),
        dcc.Loading(
            id="loading-2",
            children=[html.Div([html.Div(id="output-rec-button")])],
            type="circle",
        ),
        # html.Div(id="output-rec-button"),
    ]
)


def recRJOstaticPNL():
    """Calculates PnL on RJO trades and positions and compares to RJO reported PnL from cash file.
    - Pulls reported PnL from cash file
    - Pulls RJO positions from last two days of position files
    - Separates T-1 trades and calculates PnL from trade price to close price
    - Matches T-1 trades to T-2 trades and calculates PnL from T-2 close price to T-1 close price
    - Calculates estimated fees on the day
    - Calculates
    """

    # pull most recent cash file for pnl comparison
    (cashdf, cash_filename) = sftp_utils.fetch_latest_rjo_export(
        "UPETRADING_csvnmny_nmny_%Y%m%d.csv"
    )
    cashdf = cashdf[cashdf["Record Code"] == "M"]
    cashdf = cashdf[cashdf["Account Number"] == "UPLME"]
    cashdf = cashdf[cashdf["Account Type Currency Symbol"] == "USD"]
    cashdf["PNL"] = cashdf["Liquidating Value"] - cashdf["Previous Liquidating Value"]
    reported_pnl = (cashdf["PNL"].iloc[0]).round(2)

    # pull most recent rjo position files
    (
        t1,
        t1_filename,
    ) = sftp_utils.fetch_latest_rjo_export("UPETRADING_csvnpos_npos_%Y%m%d.csv")

    # fetch second latest rjo file
    (t2, t2_filename) = sftp_utils.fetch_2nd_latest_rjo_export(
        "UPETRADING_csvnpos_npos_%Y%m%d.csv"
    )

    # filter for exchange and record code
    t1 = t1[t1["Bloomberg Exch Code"] == "LME"]
    t1 = t1[t1["Record Code"] == "P"]

    t2 = t2[t2["Bloomberg Exch Code"] == "LME"]
    t2 = t2[t2["Record Code"] == "P"]

    # remove yesterday's trades from t1
    yesterday = str(
        dt.datetime.strptime(
            t1_filename, "UPETRADING_csvnpos_npos_%Y%m%d.csv"
        ).strftime("%Y%m%d")
    )
    t1["Trade Date"] = t1["Trade Date"].astype(int)
    t2["Trade Date"] = t2["Trade Date"].astype(int)

    yday_trades = t1[t1["Trade Date"] == int(yesterday)]
    t1 = t1[t1["Trade Date"] != yesterday]

    # calculate PnL on yesterdays trades in t1
    yday_trades["PriceDiff"] = (
        yday_trades["Close Price"] - yday_trades["Formatted Trade Price"]
    )
    yday_trades["Vol"] = yday_trades.apply(
        lambda row: row["Quantity"]
        if int(row["Buy Sell Code"]) == 1
        else -int(row["Quantity"]),
        axis=1,
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
    t1["Quantity"] = t1["Quantity"].astype(str)
    t2["Quantity"] = t2["Quantity"].astype(str)

    # Merge the DataFrames on common columns and identify unmatched rows
    combined = pd.merge(
        t1,
        t2,
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
            "Close Price_t1",
            "Close Price_t2",
        ]
    ]

    # calculate PnL on these positions
    matched["PriceDiff"] = matched["Close Price_t1"] - matched["Close Price_t2"]

    # ensure relevant columns are numeric for calculations
    matched["Quantity"] = pd.to_numeric(matched["Quantity"], errors="coerce")
    matched["Buy Sell Code"] = pd.to_numeric(matched["Buy Sell Code"], errors="coerce")

    # calculate positions and gross pnl
    matched["Vol"] = matched.apply(
        lambda row: int(row["Quantity"])
        if int(row["Buy Sell Code"]) == 1
        else int(row["Quantity"]) * -1,
        axis=1,
    )
    matched["PnL"] = (
        matched["PriceDiff"] * matched["Vol"] * matched["Multiplication Factor"]
    )
    matchedPNL = (matched["PnL"].sum()).round(2)
    totalPNL = (tradesPNL + matchedPNL).round(2)

    t2_unmatched["Vol"] = t2_unmatched.apply(
        lambda row: float(row["Quantity"])
        if int(row["Buy Sell Code"]) == 1
        else -float(row["Quantity"]),
        axis=1,
    )
    # print(t2_unmatched["Vol"].sum()) # should be 0

    # build pnl table for frontend
    table = pd.DataFrame(
        [
            ["T-1 Trades PnL", tradesPNL],
            ["Pos PnL", matchedPNL],
            ["Gross PnL", totalPNL],
            ["Estimated Fees", est_fees],
            ["Net PnL", totalPNL - est_fees],
            ["Reported PnL", reported_pnl],
            ["PnL Diff", (totalPNL - est_fees - reported_pnl).round(2)],
        ],
        columns=["source", "pnl"],
    )

    # build filename string for frontend
    filename_string = (
        "t1_filename: "
        + t1_filename
        + " ------- t2_filename: "
        + t2_filename
        + " ------- cash_filename: "
        + cash_filename
    )
    return (table, filename_string)


# function to parse data file
def parse_data(contents, filename, input_type=None):
    content_type, content_string = contents.split(",")

    decoded = base64.b64decode(content_string)
    try:
        if "csv" in filename:
            # Assume that the user uploaded a CSV or TXT file
            if input_type in ["rec", "rec_trades", "rec_cme_pos"]:
                df = pd.read_csv(
                    io.StringIO(decoded.decode("utf-8")),
                    skiprows=1,
                )
            else:
                # LME Vols
                df = pd.read_csv(io.StringIO(decoded.decode("utf-8")))

        elif "xls" in filename:
            # Assume that the user uploaded an excel file
            df = pd.read_excel(io.BytesIO(decoded))
        elif "txt" or "tsv" in filename:
            # Assume that the user upl, delimiter = r'\s+'oaded an excel file
            df = pd.read_csv(io.StringIO(decoded.decode("utf-8")), delimiter=r"\s+")
    except Exception as e:
        print(e)
        return html.Div(["There was an error processing this file."])

    return df


def initialise_callbacks(app):
    @app.callback(
        Output("output-data-upload", "children"),
        [Input("upload-data", "contents"), Input("upload-data", "filename")],
        State("file_type", "value"),
    )
    def update_table(contents, filename, file_type):
        # base table holder
        table = html.Div()

        # if contents then translate .csv into table contents.
        if contents:
            if file_type == "lme_vols":
                # un pack and parse data
                contents = contents[0]
                filename = filename[0]
                df = parse_data(contents, filename, "lme_vols")
                # ensure "NA" is read as string and not set to NaN
                df["Product"] = df["Product"].fillna("NA")

                date = df["Date"].iloc[0]

                # load LME vols
                try:
                    table = pd.read_sql(
                        'SELECT DISTINCT "Date" FROM "settlementVolasLME"',
                        con=PostGresEngine(),
                    )

                    if date in table["Date"].values:
                        PostGresEngine().execute(
                            'DELETE FROM "settlementVolasLME" WHERE "Date" = %s',
                            (date,),
                        )

                    # add current vols to end of settlement volas in SQL DB
                    df.to_sql(
                        "settlementVolasLME",
                        con=PostGresEngine(),
                        if_exists="append",
                        index=False,
                    )

                    # reprocess vols in prep
                    settleVolsProcess()
                    return "Sucessfully loads Settlement Vols"

                except Exception as e:
                    traceback.print_exc()
                    return "Failed to load Settlement Vols"

            elif file_type == "eur_vols":
                monthCode = {
                    "u3": "23-08-15",
                    "z3": "23-11-15",
                    "h4": "24-02-15",
                    "k4": "24-04-15",
                    "u4": "24-08-15",
                    "z4": "24-11-15",
                    "h5": "25-02-17",
                    "k5": "25-04-15",
                    "u5": "25-08-15",
                    "z5": "25-11-17",
                }

                def build_symbol(row):
                    prefix = "xext-ebm-eur o "
                    instrument = prefix + monthCode[row["code"]] + " a"
                    return instrument

                # un pack and parse data
                contents = contents[0]
                filename = filename[0]
                df = parse_data(contents, filename, "lme_vols")

                df.columns = df.columns.str.lower()

                # interpolate strikes within range
                df = df.set_index("strike")
                new_index = pd.Index(
                    range((int(df.index[0])), (int(df.index[-1] + 1)), 1)
                )
                df = df.reindex(new_index)

                df = df.interpolate(method="polynomial", order=2)
                df = df.reset_index().rename(columns={"index": "strike"})

                # melt dataframe on expiry and build product name
                df = pd.melt(
                    df, id_vars=["date", "strike"], var_name="code", value_name="vol"
                )
                df["option"] = df.apply(build_symbol, axis=1)
                year = "202" + df["code"].iloc[0][1]

                df.rename(
                    columns={
                        "date": "settlement_date",
                        "option": "option_symbol",
                        "vol": "volatility",
                    },
                    inplace=True,
                )

                df = df[["settlement_date", "option_symbol", "strike", "volatility"]]

                date = df["settlement_date"].iloc[0] + "-" + year
                date = dt.datetime.strptime(date, "%d-%b-%Y")
                df["settlement_date"] = date

                try:
                    sendEURVolsToPostgres(df, date)
                    return "Sucessfully loaded Euronext Settlement Vols"
                except Exception as e:
                    print(e)
                    return "There was an error processing this file."

        return table

    # LME, CME, or Euronext rec on button click
    @app.callback(
        Output("output-rec-button", "children"),
        Output("sol3-rjo-filenames", "children"),
        [Input("rec-button", "n_clicks")],
        State("file_type", "value"),
    )
    def rec_button(n, file_type):
        # on click do this
        filenames = html.Div()
        table = html.Div()

        if n > 0:
            if file_type == "rec_cme_pos":
                # get latest sol3 and rjo pos exports
                (
                    latest_sol3_df,
                    latest_sol3_filename,
                ) = sftp_utils.fetch_latest_sol3_export(
                    "positions", "export_positions_cme_%Y%m%d-%H%M.csv"
                )

                (
                    latest_rjo_df,
                    latest_rjo_filename,
                ) = sftp_utils.fetch_latest_rjo_export(
                    "UPETRADING_csvnpos_npos_%Y%m%d.csv"
                )
                # drop all contracts not in sol3 (LME)
                latest_rjo_df = latest_rjo_df[
                    ~latest_rjo_df["Bloomberg Exch Code"].isin(["LME", "EOP"])
                ]
                latest_rjo_df = latest_rjo_df[
                    latest_rjo_df["Contract Code"].isin(list(rjo_to_sol3_hash.keys()))
                ]

                rec = rec_sol3_rjo_cme_pos(latest_sol3_df, latest_rjo_df)
                rec_table = dtable.DataTable(
                    data=rec.to_dict("records"),
                    columns=[
                        {"name": col_name, "id": col_name} for col_name in rec.columns
                    ],
                )
                filename_string = (
                    "Sol3 filename: "
                    + latest_sol3_filename
                    + " RJO filename: "
                    + latest_rjo_filename
                )
                return rec_table, filename_string

            elif file_type == "rec_lme_pos":
                # column titles for output table.
                columns = [
                    {"id": "instrument", "name": "Instrument"},
                    {"id": "quanitity_UPE", "name": "Georgia"},
                    {"id": "quanitity_RJO", "name": "RJO"},
                    {"id": "diff", "name": "Diff"},
                ]

                # rec current dataframe
                rec, latest_rjo_filename = recRJO("LME")
                rec["instrument"] = rec.index
                table = dtable.DataTable(
                    id="recTable",
                    data=rec.to_dict("records"),
                    columns=columns,
                )
                return table, latest_rjo_filename

            elif file_type == "rec_euro_pos":
                # column titles for output table.
                columns = [
                    {"id": "instrument", "name": "Instrument"},
                    {"id": "quanitity_UPE", "name": "Georgia"},
                    {"id": "quanitity_RJO", "name": "RJO"},
                    {"id": "diff", "name": "Diff"},
                ]

                # rec current dataframe
                rec, latest_rjo_filename = recRJO("EURONEXT")
                rec["instrument"] = rec.index
                table = dtable.DataTable(
                    id="recTable",
                    data=rec.to_dict("records"),
                    columns=columns,
                )
                return table, latest_rjo_filename

            elif file_type == "rec_settle_prices":
                # column titles for output table.
                columns = [
                    {"id": "source", "name": "Source"},
                    {"id": "pnl", "name": "PnL"},
                ]
                (table, filenames) = recRJOstaticPNL()
                table = dtable.DataTable(
                    id="recTable",
                    data=table.to_dict("records"),
                    columns=columns,
                )
                return table, filenames

        return table, filenames
