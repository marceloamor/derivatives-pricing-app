import base64
import datetime as dt
import io
import traceback

import dash_bootstrap_components as dbc
import orjson
import pandas as pd
import sftp_utils
import sqlalchemy.orm
from dash import callback_context, dcc, html
from dash import dash_table as dtable
from dash.dependencies import Input, Output, State
from icecream import ic

from data_connections import PostGresEngine, conn, shared_engine, shared_session
from parts import (
    dev_key_redis_append,
    rec_sol3_rjo_cme_pos,
    recRJO,
    rjo_to_sol3_hash,
    sendEURVolsToPostgres,
    settleVolsProcess,
    topMenu,
)

# options for file type dropdown
fileOptions = [
    {"label": "Non-LME Settlement Vols", "value": "general_vols"},
    {"label": "LME Vols", "value": "lme_vols"},
    # {"label": "EUR Vols", "value": "eur_vols"},
    # {"label": "ICE Vols", "value": "ice_vols"},
    {"label": "Rec LME Positions", "value": "rec_lme_pos"},
    {"label": "Rec CME Positions", "value": "rec_cme_pos"},
    {"label": "Rec Euronext Positions", "value": "rec_euro_pos"},
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
        dcc.ConfirmDialog(
            id="confirm-upload",
            message="Vols appear to be in Absolute format, not Relative! Upload anyway?",
        ),
        html.Div(id="output-data-upload"),
        html.Div(id="output-confirm-upload"),
        html.Div(id="sol3-rjo-filenames"),
        dcc.Loading(
            id="loading-2",
            children=[html.Div([html.Div(id="output-rec-button")])],
            type="circle",
        ),
        # html.Div(id="output-rec-button"),
    ]
)


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
            elif input_type == "general_vols":
                df = pd.read_csv(
                    io.StringIO(decoded.decode("utf-8")),
                    parse_dates=True,
                    date_format="%d/%m/%Y",
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


# validate lme vols, handle absolute/relative format possibility
def validate_lme_vols(df):
    # 4 validation steps
    status = (0, "Vols uploaded successfully")
    # 1- check Date column format: ddMmmYY
    try:
        _ = pd.to_datetime(df["Date"], format="%d%b%y")
    except ValueError:
        status = (1, "Date column not formatted correctly")
    # 2- check all dates are the same
    if df["Date"].nunique() > 1:
        status = (1, "Multiple dates in file")
    # 3- check Series column format: MmmYY
    try:
        _ = pd.to_datetime(df["Series"], format="%b%y")
    except ValueError:
        status = (1, "Series column not formatted correctly")
    # 4- check Relative and not Absolute
    diff_columns = ["-10 DIFF", "-25 DIFF", "50 Delta", "+25 DIFF", "+10 DIFF"]
    if (df[diff_columns].max(axis=1) != df["50 Delta"]).any():
        # do something
        status = (2, "Vols appear to be in Absolute format, not Relative")

    return status


def initialise_callbacks(app):
    @app.callback(
        Output("output-data-upload", "children"),
        Output("confirm-upload", "displayed"),
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

                # validate vols are correct
                status = validate_lme_vols(df)

                # handle relative vs absolute confirm dialog
                if status[0] == 2:
                    return status[1], True

                # convert Date column to date - from ddMmmYY to date
                df["settlement_date"] = pd.to_datetime(df["Date"], format="%d%b%y")
                settlement_date = df["settlement_date"].iloc[0].date()

                # divide all numerical columns by 100
                df_numeric = df.select_dtypes(include=["number"])
                df[df_numeric.columns] = df_numeric / 100

                # ic(df)
                settlement_date = df["settlement_date"].iloc[0].date()

                # divide all numerical columns by 100
                df_numeric = df.select_dtypes(include=["number"])
                df[df_numeric.columns] = df_numeric / 100

                # ic(df)

                # build georgia symbol from Product and Series columns
                lme_to_georgia_map = {
                    "AH": "xlme-lad-usd",
                    "CA": "xlme-lcu-usd",
                    "PB": "xlme-pbd-usd",
                    "NI": "xlme-lnd-usd",
                    "ZS": "xlme-lzh-usd",
                }

                # from db pull all option symbols, expiry dates where product.exchange = lme
                with shared_engine.connect() as db_conn:
                    stmt = sqlalchemy.text(
                        "SELECT symbol FROM options WHERE product_symbol IN (SELECT symbol FROM products WHERE exchange_symbol = 'xlme')"
                    )

                    result = db_conn.execute(stmt).fetchall()
                    # option_symbols_expiry_map = {row[1]: row[0] for row in result.fetchall()}
                    valid_option_symbols = [row[0] for row in result]
                    ic(valid_option_symbols)

                df["product_symbol"] = df["Product"].map(lme_to_georgia_map)
                # remove rows that are not in the lme_to_georgia_map
                df = df[~df["product_symbol"].isna()]

                def build_georgia_symbol_from_lme_vols(row):
                    # convert a date in Mmmyy format to a string in format yyyy-mm
                    date = dt.datetime.strptime(row["Series"], "%b%y").strftime("%y-%m")
                    return f"{row['product_symbol']} o {date}"

                df["instrument_prefix"] = df.apply(
                    build_georgia_symbol_from_lme_vols, axis=1
                )

                # create instrument symbol row by finding the correct option symbol in valid_option_symbols
                def find_closest_match(prefix, valid_symbols):
                    # find the first valid symbol that starts with the prefix
                    for symbol in valid_symbols:
                        if symbol.startswith(prefix):
                            return symbol
                    print(f"Could not find a valid georgia mapping for {prefix}")
                    return None

                df["option_symbol"] = df.apply(
                    lambda row: find_closest_match(
                        row["instrument_prefix"], valid_option_symbols
                    ),
                    axis=1,
                )

                # drop rows where no match was found, as well as the Date, Product, and Series columns
                df = df.dropna(subset=["option_symbol"])
                df = df.drop(
                    columns=[
                        "Date",
                        "Product",
                        "Series",
                        "instrument_prefix",
                        "product_symbol",
                    ]
                )
                df.rename(
                    columns={
                        "-10 DIFF": "m10_diff",
                        "-25 DIFF": "m25_diff",
                        "50 Delta": "atm_vol",
                        "+25 DIFF": "p25_diff",
                        "+10 DIFF": "p10_diff",
                        "Median": "median",
                        "Lowest": "lowest",
                        "Highest": "highest",
                        "S1": "s1",
                        "S2": "s2",
                    },
                    inplace=True,
                )

                ################################################################ KEEP GOING FROM HERE !!!!
                # load LME vols
                if status[0] == 0:
                    # send df to lme_settlement_spline_params table in postgres
                    with shared_engine.connect() as db_conn:
                        # check if date is in the table
                        stmt = sqlalchemy.text(
                            "SELECT settlement_date FROM lme_settlement_spline_params WHERE settlement_date = :settlement_date"
                        )
                        result = db_conn.execute(
                            stmt,
                            {"settlement_date": settlement_date},
                        )
                        # result = db_conn.execute(
                        #     stmt, {"settlement_date": settlement_date}
                        # )
                        if result.fetchone():
                            # clear out old data
                            try:
                                db_conn.execute(
                                    sqlalchemy.text(
                                        "DELETE FROM lme_settlement_spline_params WHERE settlement_date = :settlement_date"
                                    ),
                                    {"settlement_date": settlement_date},
                                )
                                db_conn.commit()
                            except Exception as e:
                                ic(e)
                                return (
                                    f"Failed to replace exisiting vols for {settlement_date}: {status[1]}",
                                    False,
                                )
                        # insert new data
                        df.to_sql(
                            "lme_settlement_spline_params",
                            con=db_conn,
                            if_exists="append",
                            index=False,
                        )
                        db_conn.commit()

                    return f"Loaded LME Vols for {settlement_date}", False
                else:
                    return f"Failed to load Settlement Vols: {status[1]}", False

            elif file_type == "general_vols":

                def build_symbol(row):
                    return row["code"]

                products_updated = []

                # un pack and parse data
                contents = contents[0]
                filename = filename[0]
                df = parse_data(contents, filename, "general_vols")

                df.columns = df.columns.str.lower()

                # interpolate strikes within range
                df = df.set_index("strike")
                try:
                    settlement_date = dt.datetime.strptime(
                        df["date"].iloc[0], "%d/%m/%y"
                    )
                except ValueError:
                    settlement_date = dt.datetime.strptime(
                        df["date"].iloc[0], "%d/%m/%Y"
                    )
                df["date"] = df["date"].iloc[0]

                df = df.reset_index().rename(columns={"index": "strike"})

                # melt dataframe on expiry and build product name
                df = pd.melt(
                    df,
                    id_vars=["date", "strike"],
                    var_name="option_symbol",
                    value_name="volatility",
                )
                df.rename(
                    columns={
                        "date": "settlement_date",
                    },
                    inplace=True,
                )
                df["settlement_date"] = settlement_date
                products_updated = list(
                    df.loc[:, "option_symbol"].str.split(" ").str[0].unique()
                )
                try:
                    sendEURVolsToPostgres(df, settlement_date)
                    conn.publish(
                        "v2:compute" + dev_key_redis_append,
                        orjson.dumps(
                            {"type": "staticdata", "product_symbols": products_updated},
                        ),
                    )
                except Exception as e:
                    print(e)
                    return "There was an error processing this file", False
                return (
                    f"Loaded vols for {products_updated}, option engine refresh in progress",
                    False,
                )

            elif file_type == "ice_vols":
                # this remains terrible but it's a quick fix to get things out,
                # I LOVE BANDAIDS
                monthCode = {
                    "k4": "24-04-12",
                    "m4": "24-06-12",
                    "u4": "24-08-09",
                    "z4": "24-11-08",
                    "h5": "25-02-12",
                    "k5": "25-04-11",
                    "m5": "25-06-12",
                    "u5": "25-08-08",
                    "z5": "25-11-12",
                    "h6": "26-02-11",
                    "k6": "26-04-10",
                    "m6": "26-06-12",
                    "u6": "26-08-14",
                    "z6": "26-11-12",
                }

                def build_symbol(row):
                    prefix = "xice-kc-usd o "
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
                    conn.publish(
                        "v2:compute" + dev_key_redis_append,
                        orjson.dumps(
                            {"type": "staticdata", "product_symbols": ["xice-kc-usd"]}
                        ),
                    )
                    return "Sucessfully loaded Euronext Settlement Vols", False
                except Exception as e:
                    print(e)
                    return "There was an error processing this file.", False
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
                    "h6": "26-02-16",
                    "k6": "26-04-15",
                    "u6": "26-08-17",
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
                    conn.publish(
                        "v2:compute" + dev_key_redis_append,
                        orjson.dumps(
                            {"type": "staticdata", "product_symbols": ["xext-ebm-eur"]}
                        ),
                    )
                    return "Sucessfully loaded Euronext Settlement Vols", False
                except Exception as e:
                    print(e)
                    return "There was an error processing this file.", False

        return table, False

    @app.callback(
        Output("output-confirm-upload", "children"),
        [
            Input("confirm-upload", "submit_n_clicks"),
            Input("confirm-upload", "cancel_n_clicks"),
        ],
    )
    def update_table(y, n):
        ctx = callback_context

        if not ctx.triggered:
            return ""

        if ctx.triggered[0]["prop_id"] == "confirm-upload.submit_n_clicks":
            # user clicks upload anyway
            if y:
                return "Upload continued and processed."

        elif ctx.triggered[0]["prop_id"] == "confirm-upload.cancel_n_clicks":
            # user clicks cancel upload
            if n:
                return "Upload was canceled."

        return ""

    # LME, CME, or Euronext rec on button click
    @app.callback(
        [
            Output("output-rec-button", "children"),
            Output("sol3-rjo-filenames", "children"),
        ],
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
                return [rec_table], [filename_string]

            elif file_type == "rec_lme_pos":
                # column titles for output table.
                columns = [
                    {"id": "instrument_symbol", "name": "Instrument"},
                    {"id": "accountnumber", "name": "Account"},
                    {"id": "net_quantity_UPE", "name": "Georgia"},
                    {"id": "net_quantity_RJO", "name": "RJO"},
                    {"id": "diff", "name": "Diff"},
                ]

                # rec current dataframe
                # with shared_engine.connect() as connection:
                #     print(connection.execute(sqlalchemy.text("SELECT 1")))
                with sqlalchemy.orm.Session(shared_engine) as session:
                    rec, latest_rjo_filename = recRJO("LME", session)
                rec.reset_index(inplace=True)
                table = dtable.DataTable(
                    id="recTable",
                    data=rec.to_dict("records"),
                    columns=columns,
                )
                return [table], [latest_rjo_filename]

            elif file_type == "rec_euro_pos":
                # column titles for output table.
                columns = [
                    {"id": "instrument_symbol", "name": "Instrument"},
                    {"id": "accountnumber", "name": "Account"},
                    {"id": "net_quantity_UPE", "name": "Georgia"},
                    {"id": "net_quantity_RJO", "name": "RJO"},
                    {"id": "diff", "name": "Diff"},
                ]

                # rec current dataframe
                with sqlalchemy.orm.Session(shared_engine) as session:
                    rec, latest_rjo_filename = recRJO("EURONEXT", session)
                rec.reset_index(inplace=True)
                table = dtable.DataTable(
                    id="recTable",
                    data=rec.to_dict("records"),
                    columns=columns,
                )
                return [table], [latest_rjo_filename]

        return table, filenames
